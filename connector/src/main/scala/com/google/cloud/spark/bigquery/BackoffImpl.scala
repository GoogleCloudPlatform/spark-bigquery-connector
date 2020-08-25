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

import java.util.concurrent.TimeUnit

import com.google.api.client.util.BackOff

import scala.concurrent.duration.Duration

/**
 * Backoff implementation ported from Beam BigQuery connector
 * @param backoffConfig
 */
case class BackoffImpl(backoffConfig: FluentBackoff) extends BackOff {
  // Current state
  private var currentCumulativeBackoff: Duration = null
  private var currentRetry: Int = 0
  private val DEFAULT_RANDOMIZATION_FACTOR = 0.5

  override def reset(): Unit = {
    currentRetry = 0
    currentCumulativeBackoff = Duration.Zero
  }

  override def nextBackOffMillis(): Long = {
    // Maximum number of retries reached.
    if (currentRetry >= backoffConfig.maxRetries) {
      return BackOff.STOP
    }
    // Maximum cumulative backoff reached.
    if (currentCumulativeBackoff.compareTo(backoffConfig.maxCumulativeBackoff) >= 0) {
      return BackOff.STOP
    }

    val currentIntervalMillis = Math.min(
      backoffConfig.initialBackoff.toMillis * Math.pow(backoffConfig.exponent, currentRetry),
      backoffConfig.maxBackoff.toMillis)
    val randomOffset = (Math.random * 2 - 1) *
      DEFAULT_RANDOMIZATION_FACTOR *
      currentIntervalMillis
    var nextBackoffMillis = currentIntervalMillis + randomOffset.round

    // Cap to limit on cumulative backoff
    val remainingCumulative = backoffConfig.maxCumulativeBackoff.minus(currentCumulativeBackoff)
    nextBackoffMillis = Math.min(nextBackoffMillis, remainingCumulative.toMillis)

    // Update state and return backoff.
    currentCumulativeBackoff = currentCumulativeBackoff.plus(
      Duration.create(nextBackoffMillis, TimeUnit.MILLISECONDS))
    currentRetry += 1
    nextBackoffMillis.toLong
  }
}
