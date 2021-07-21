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

import java.io.IOException

import com.google.cloud.bigquery.connector.common.BigQueryClient
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
 * BigQuery Spark streaming sink
 * @param sqlContext
 * @param parameters
 * @param partitionColumns
 * @param outputMode
 * @param opts
 */
case class BigQueryStreamingSink(
                         sqlContext: SQLContext,
                         parameters: Map[String, String],
                         partitionColumns: Seq[String],
                         outputMode: OutputMode,
                         opts: SparkBigQueryConfig,
                         bigQueryClient: BigQueryClient
                       ) extends Sink with Logging {

  @volatile private var latestBatchId: Long = -1L

  @throws[IOException]
  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    if (batchId <= latestBatchId) {
      logWarning("Skipping as already committed batch " + batchId)
    } else {
      logDebug(s"addBatch($batchId)")
      BigQueryStreamWriter.writeBatch(data, sqlContext, outputMode, opts, bigQueryClient)
    }
    latestBatchId = batchId
  }
}
