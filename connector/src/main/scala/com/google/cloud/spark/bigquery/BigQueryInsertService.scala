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
import java.util.concurrent.{Callable, ExecutorService, Executors, FutureTask, TimeUnit}

import com.google.api.client.util.{BackOff, Sleeper}
import com.google.api.services.bigquery.model.TableRow
import com.google.cloud.bigquery.InsertAllRequest.RowToInsert
import com.google.cloud.bigquery._
import org.apache.spark.internal.Logging

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.Duration
import scala.util.control.Breaks._

/**
 * Class to make insertAll calls to BigQuery for streaming inserts
 * Breaks the list of table rows into chunks of
 * MAX_STREAMING_BATCH_SIZE or MAX_STREAMING_ROWS_TO_BATCH.
 * Throws exception if retries are exhausted, and all records are not yet inserted.
 * BackOff is applied if API calls fail
 * @param executor
 * @param sleeper
 * @param insertRetryPolicy
 */
private[bigquery] class BigQueryInsertService(executor: ExecutorService,
                            sleeper: Sleeper,
                            insertRetryPolicy: InsertRetryPolicy) extends Logging {
  @throws[IOException]
  def insertAll(ref: TableId,
                rowList: List[TableRow],
                insertIds: List[String] = null,
                backoff: BackOff = FluentBackoff().backoff(),
                ignoreInsertIds: Boolean = true,
                client: BigQuery
               ): Long = {
    var retTotalDataSize: Long = 0
    val allErrors: ListBuffer[BigQueryError] =
      new ListBuffer[BigQueryError]

    var rowsToPublish: List[TableRow] = rowList
    var insertIdsToPublish: List[String] = insertIds

    // Don't ignore insert ids going forward
    var done: Boolean = false
    while (!done) {
      val retryRows: ListBuffer[TableRow] = new ListBuffer[TableRow]
      val retryIds: ListBuffer[String] = new ListBuffer[String]

      val rows: ListBuffer[RowToInsert] =
        new ListBuffer[RowToInsert]
      var dataSize: Long = 0
      var i: Int = 0
      val futures: ListBuffer[FutureTask[InsertAllResponse]] =
        new ListBuffer[FutureTask[InsertAllResponse]]

      // val responses: ListBuffer[InsertAllResponse] = new ListBuffer[InsertAllResponse]

      for (row: TableRow <- rowsToPublish) {
        i += 1
        var out: RowToInsert = {
          if (ignoreInsertIds) {
            RowToInsert.of(row.getUnknownKeys)
          } else {
            RowToInsert.of(insertIdsToPublish.apply(i-1), row.getUnknownKeys)
          }
        }: RowToInsert
        rows += out

        dataSize += EncodeUtils.getEncodedElementByteSize(row)

        if (dataSize >= BigQueryInsertService.MAX_STREAMING_BATCH_SIZE ||
          rows.size >= BigQueryInsertService.MAX_STREAMING_ROWS_TO_BATCH ||
          i == rowsToPublish.size) {
          val request = InsertAllRequest.newBuilder(ref)
            .setRows(rows.toList.asJava)
            .setIgnoreUnknownValues(true)
            .setSkipInvalidRows(true)

          val task =
            new FutureTask[InsertAllResponse](new Callable[InsertAllResponse]() {
            def call(): InsertAllResponse = {

              val backoff: BackOff = BigQueryInsertService.RATE_LIMIT_BACKOFF_FACTORY.backoff()
              var returnValue: InsertAllResponse = null
              breakable {
                while (true) {
                  try {
                    returnValue = client.insertAll(request.build())
                    break
                  } catch {
                    case e: IOException =>
                      logWarning(
                        String.format(
                          "BigQuery insertAll error, retrying: %s", e.getMessage
                        ))
                      try sleeper.sleep(backoff.nextBackOffMillis()) catch {
                        case e: InterruptedException =>
                          throw new IOException(
                            "Interrupted while waiting before retrying insertAll");
                      }
                  }
                }
              }
              // responses.+=(returnValue)
              returnValue
            }
          })

          futures += task
          executor.execute(task)
          retTotalDataSize += dataSize
          dataSize = 0
          rows.clear()
        }
      }
      // Wait on responses
      try {
        for (task <- futures) {
        // for (response: InsertAllResponse <- responses) {
          val response: InsertAllResponse = task.get()
          if (response.hasErrors) {
            val errorMap: mutable.Map[java.lang.Long, java.util.List[BigQueryError]] =
              response.getInsertErrors.asScala
            for ((index, jErrorList) <- errorMap) {
              // Add table rows to retry list and reset input collections if enabled
              val errorList: List[BigQueryError] = jErrorList.asScala.toList
              if (insertRetryPolicy.shouldRetry(errorList)) {
                // Retry the error
                allErrors.++=(errorList)
                val indexInt: Int = index.toInt
                retryRows += rowsToPublish.apply(indexInt)
                retryIds += insertIdsToPublish.apply(indexInt)
              }
            }
          }
        }
      } catch {
        case e: InterruptedException =>
          Thread.currentThread().interrupt()
          throw new IOException("Interrupted while inserting " + rowsToPublish)
      }
      if (allErrors.isEmpty) {
        done = true
      } else {
        val nextBackoffMilis = backoff.nextBackOffMillis()
        done = (nextBackoffMilis == BackOff.STOP)

        if (!done) {
          try {
            sleeper.sleep(nextBackoffMilis)
          } catch {
            case e: InterruptedException =>
              Thread.currentThread().interrupt()
              throw new IOException("Interrupted while waiting before retrying insert of "
                + retryRows)
          }
          rowsToPublish = retryRows.toList
          insertIdsToPublish = retryIds.toList
          allErrors.clear()
          logWarning("Retrying " + rowsToPublish.size + " failed inserts to BigQuery")
        }
      }
    }
    if (!allErrors.isEmpty) {
      throw new IOException("Insert failed: " + allErrors)
    }
    retTotalDataSize
  }
}

private[bigquery] object BigQueryInsertService {
  /**
   * Maximum data size in one insertAll batch
   */
  val MAX_STREAMING_BATCH_SIZE: Long = 64L * 1024L
  /**
   * The maximum number of rows to batch in a single streaming insertAll to BigQuery
   */
  val MAX_STREAMING_ROWS_TO_BATCH: Long = 500
  /** The given number of maximum concurrent threads will be used to insert
   * rows from one bundle to BigQuery service with streaming insert API
   */
  val INSERT_BUNDLE_PARALLELISM: Int = 2

  private val executor: ExecutorService =
    Executors.newFixedThreadPool(INSERT_BUNDLE_PARALLELISM)
  private val RATE_LIMIT_BACKOFF_FACTORY =
    FluentBackoff(maxBackoff = Duration.create(2, TimeUnit.MINUTES))

  def apply(executor: ExecutorService = executor,
            sleeper: Sleeper = Sleeper.DEFAULT,
            insertRetryPolicy: InsertRetryPolicy = RetryTransientErrorsPolicy):
  BigQueryInsertService = new BigQueryInsertService(
    executor,
    sleeper,
    insertRetryPolicy
  )
}
