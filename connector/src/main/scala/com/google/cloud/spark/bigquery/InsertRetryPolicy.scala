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

import com.google.cloud.bigquery.BigQueryError

/**
 * Realtime streaming inserts retry policy
 */
trait InsertRetryPolicy {
  // A list of known persistent errors for which retrying never helps.
  val PERSISTENT_ERRORS: Set[String] =
    Set[String]("invalid", "invalidQuery", "notImplemented")
  def shouldRetry(errors: List[BigQueryError]): Boolean = {
    if (errors != null) {
      for (error: BigQueryError <- errors) {
        if (error.getReason != null && PERSISTENT_ERRORS.contains(error.getReason)) {
          return false
        }
      }
    }
    true
  }
}

object AllwaysRetryInsertPolicy extends InsertRetryPolicy {
  override def shouldRetry(errors: List[BigQueryError]): Boolean = true
}

object NeverRetryInsertPolicy extends InsertRetryPolicy {
  override def shouldRetry(errors: List[BigQueryError]): Boolean = false
}

object RetryTransientErrorsPolicy extends InsertRetryPolicy