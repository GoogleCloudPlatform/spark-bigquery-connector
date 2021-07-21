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

import com.google.cloud.bigquery.connector.common.BigQueryClient
import com.google.cloud.spark.bigquery.spark3.{DataFrameToRDDConverter, Spark3DataFrameToRDDConverter}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType

private[bigquery] object BigQueryStreamWriter extends Logging {

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
                 opts: SparkBigQueryConfig,
                 bigQueryClient: BigQueryClient): Unit = {
    val schema: StructType = data.schema
    val sparkVersion = sqlContext.sparkSession.version

    val rowRdd: RDD[Row] =
      dataFrameToRDDConverterFactory(sparkVersion).convertToRDD(data)

    // Create fixed dataframe
    val dataFrame: DataFrame = sqlContext.createDataFrame(rowRdd, schema)
    val table = Option(bigQueryClient.getTable(opts.getTableId))
    val saveMode = getSaveMode(outputMode)
    val helper = BigQueryWriteHelper(
      bigQueryClient, sqlContext, saveMode, opts, dataFrame, table.isDefined)
    helper.writeDataFrameToBigQuery
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

  class Spark2DataFrameToRDDConverter extends DataFrameToRDDConverter {

    override def convertToRDD(data: Dataset[Row]): RDD[Row] = {
      val schema: StructType = data.schema
      val expressionEncoder = RowEncoder(schema).resolveAndBind()

      val rowRdd: RDD[Row] =
        data.queryExecution.toRdd.mapPartitions(
          iter =>
            iter.map(internalRow => expressionEncoder.fromRow(internalRow)))

      rowRdd
    }
  }

  def dataFrameToRDDConverterFactory(sparkVersion: String): DataFrameToRDDConverter = {
    val version = sparkVersion.charAt(0) - '0'
    if (version < 3) {
      new Spark2DataFrameToRDDConverter
    } else {
      new Spark3DataFrameToRDDConverter
    }
  }

}

