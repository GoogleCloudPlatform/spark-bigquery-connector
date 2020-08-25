/*
 * Copyright 2020 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.spark.bigquery

import com.google.api.services.bigquery.model.TableRow
import com.google.cloud.bigquery.BigQuery
import com.google.cloud.bigquery.InsertAllRequest.RowToInsert
import com.google.gson.Gson
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}

import scala.collection.mutable.ListBuffer

private[bigquery] object BigQueryStreamWriter extends Logging {
  private val INSERT_ID_COLUMN_NAME: String = "insertId"

  /**
   * Convert streaming dataframe to fixed dataframe by
   * Getting partitioned RDD and mapping to Row dataframe
   *
   * @param data       Streaming dataframe
   * @param sqlContext Spark SQLContext
   * @param opts       Spark BigQuery Options
   */
  def writeBatch(data: DataFrame,
                 sqlContext: SQLContext,
                 outputMode: OutputMode,
                 opts: SparkBigQueryOptions,
                 client: BigQuery): Unit = {
    val schema: StructType = data.schema
    val expressionEncoder: ExpressionEncoder[Row] = RowEncoder(schema).resolveAndBind()
    val rowRdd: RDD[Row] = data.queryExecution.toRdd.mapPartitions(iter =>
      iter.map(internalRow =>
        expressionEncoder.fromRow(internalRow)
      )
    )
    // Create fixed dataframe
    val dataFrame: DataFrame = sqlContext.createDataFrame(rowRdd, schema)
    val table = Option(client.getTable(opts.tableId))

    val saveMode = getSaveMode(outputMode)
    val helper = BigQueryWriteHelper(client, sqlContext, saveMode, opts, dataFrame, table.isDefined)
    helper.writeDataFrameToBigQuery
  }

  /**
   * Use insertAll API for streaming to BigQuery
   *
   * @param data
   * @param sqlContext
   * @param outputMode
   * @param opts
   */
  def writeStream(data: DataFrame,
                  sqlContext: SQLContext,
                  outputMode: OutputMode,
                  opts: SparkBigQueryOptions): Unit = {
    val queryExecution = data.queryExecution
    val schema = data.schema

    val rdd: RDD[InternalRow] = queryExecution.toRdd

    logDebug("Schema: " + new Gson().toJson(schema))
    logDebug("Partitions: " + rdd.getNumPartitions)
    logInfo("Count: " + rdd.count())

    val insertIdOrdinal: Int = if (!opts.ignoreInsertIds) {
      getInsertIdOrdinal(schema)
    } else {
      -1
    }

    /**
     * Convert row objects into TableRows per partition
     * Collect each partition as a list and write BigQuery
     */

    rdd.foreachPartition(iter =>
      if (!iter.isEmpty) {
        val client: BigQuery = BigQueryRelationProvider.createBigQuery(opts)
        val tableRowList: ListBuffer[TableRow] = ListBuffer[TableRow]()
        val insertIdListBuffer: ListBuffer[String] = ListBuffer[String]()
        val rowConvertor: RowConverter = RowConverter(schema)

        while (iter.hasNext) {
          val row: InternalRow = iter.next()
          if (!opts.ignoreInsertIds) {
            insertIdListBuffer += row.getString(insertIdOrdinal)
          }
          val tr: TableRow = rowConvertor.convert(row)
          if (!opts.ignoreInsertIds) {
            tr.remove(INSERT_ID_COLUMN_NAME)
          }
          tableRowList += tr
        }

        logInfo("Got rows: " + tableRowList)
        val outRowsBuffer: ListBuffer[RowToInsert] = ListBuffer[RowToInsert]()
        for (row: TableRow <- tableRowList) {
          outRowsBuffer += RowToInsert.of(row.getUnknownKeys)
        }

        val insertService: BigQueryInsertService = BigQueryInsertService()
        insertService.insertAll(ref = opts.tableId,
          rowList = tableRowList.toList,
          insertIds = insertIdListBuffer.toList,
          backoff = FluentBackoff().backoff(),
          ignoreInsertIds = opts.ignoreInsertIds,
          client = client
        )
      }
    )
  }


  /**
   * Convert Output mode to save mode
   * Complete => Truncate
   * Append => Append (Default)
   * Update => Not yet supported
   *
   * @param outputMode
   * @throws NotImplementedError
   * @return SaveMode
   */
  @throws[NotImplementedError]
  private def getSaveMode(outputMode: OutputMode): SaveMode = {
    if (outputMode == OutputMode.Complete()) {
      SaveMode.Overwrite
    } else if (outputMode == OutputMode.Update()) {
      throw new NotImplementedError("Updates are not yet supported")
    } else {
      SaveMode.Append
    }
  }

  private def getInsertIdOrdinal(schema: StructType): Int = {
    var ordinal: Int = 0
    for (name: String <- schema.fieldNames) {
      if (name == INSERT_ID_COLUMN_NAME) {
        return ordinal
      }
      ordinal += 1
    }
    -1
  }
}

