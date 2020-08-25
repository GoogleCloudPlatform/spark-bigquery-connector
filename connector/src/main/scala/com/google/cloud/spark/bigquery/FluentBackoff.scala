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
 * Backoff base class
 * Ported from Beam BigQuery connector
 * @param exponent
 * @param initialBackoff
 * @param maxBackoff
 * @param maxCumulativeBackoff
 * @param maxRetries
 */
case class FluentBackoff(exponent: Double,
                         initialBackoff: Duration,
                         maxBackoff: Duration,
                         maxCumulativeBackoff: Duration,
                         maxRetries: Int) {

  def backoff() : BackOff = {
    BackoffImpl(this)
  }
}

object FluentBackoff {
  private val DEFAULT_EXPONENT = 1.5
  private val DEFAULT_MIN_BACKOFF = Duration.create(1, TimeUnit.SECONDS)
  private val DEFAULT_MAX_BACKOFF = Duration.create(1000, TimeUnit.DAYS)
  private val DEFAULT_MAX_RETRIES = Integer.MAX_VALUE
  private val DEFAULT_MAX_CUM_BACKOFF = Duration.create(1000, TimeUnit.DAYS)

  def apply(initialBackoff: Duration = DEFAULT_MIN_BACKOFF,
            maxBackoff: Duration = DEFAULT_MAX_BACKOFF,
            maxCumulativeBackoff: Duration = DEFAULT_MAX_CUM_BACKOFF,
            maxRetries: Int = DEFAULT_MAX_RETRIES): FluentBackoff = new FluentBackoff(
    DEFAULT_EXPONENT,
    initialBackoff,
    maxBackoff,
    maxCumulativeBackoff,
    maxRetries)
}
