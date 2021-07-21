/*
 * Copyright 2018 Google Inc. All Rights Reserved.
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

import java.math.BigInteger

import com.google.cloud.bigquery.TableDefinition
import com.google.cloud.bigquery.connector.common.BigQueryClient
import org.apache.spark.internal.Logging
import org.apache.spark.sql.sources.{BaseRelation, InsertableRelation}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

/**
 * Used only to insert data into BigQuery. Getting the table metadata (existence,
 * number of records, size, etc.) is done by API calls without reading existing
 * data
 */
case class BigQueryInsertableRelation(val bigQueryClient: BigQueryClient,
                                      val sqlContext: SQLContext,
                                      val options: SparkBigQueryConfig)
  extends BaseRelation
    with InsertableRelation with Logging {

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    logDebug(s"insert data=${data}, overwrite=$overwrite")
    // the helper also supports the v2 api
    val saveMode = if (overwrite) SaveMode.Overwrite else SaveMode.Append
    val helper = BigQueryWriteHelper(bigQueryClient, sqlContext, saveMode, options, data, exists)
    helper.writeDataFrameToBigQuery
  }

  /**
   * Does this table exist?
   */
  lazy val exists: Boolean = {
    // scalastyle:off
    // See https://googleapis.dev/java/google-cloud-clients/latest/com/google/cloud/bigquery/BigQuery.html#getTable-com.google.cloud.bigquery.TableId-com.google.cloud.bigquery.BigQuery.TableOption...-
    // scalastyle:on
    val table = getTable
    table.isDefined
  }

  /**
   * Is this table empty? A none-existing table is considered to be empty
   */
  def isEmpty: Boolean = numberOfRows.map(n => n.longValue() == 0).getOrElse(true)

  /**
   * Returns the number of rows in the table. If the table does not exist return None
   */
  private def numberOfRows: Option[BigInteger] = getTable.map(t => t.getNumRows())

  lazy val getTable = Option(bigQueryClient.getTable(options.getTableId))

  override def schema: StructType = {
    val tableInfo = getTable.get
    val tableDefinition = tableInfo.getDefinition.asInstanceOf[TableDefinition]
    SchemaConverters.toSpark(tableDefinition.getSchema)
  }

}



