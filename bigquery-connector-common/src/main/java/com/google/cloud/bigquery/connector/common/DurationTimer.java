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
package com.google.cloud.bigquery.connector.common;

import java.io.Serializable;
import java.time.Duration;

/**
 * Minimal timer class that measures total durations.  It can work in two
 * modes.  Repeated calls to {@link #finish()} will measure time between calls to Finish.
 *
 * A call to {@link #start() with a subsequent call to {@link #finish()} will accumulate
 * time between the {@link #start } and {@link #finish()} call.
 */
final class DurationTimer implements Serializable {
  private long start = Long.MIN_VALUE;
  private long accumulatedNanos = 0;
  private long samples = 0;

  public void start() {
    start = System.nanoTime();
  }

  public void finish() {
    long now = System.nanoTime();
    if (start != Long.MIN_VALUE) {
      accumulatedNanos += now - start;
      samples++;
    }
    start = now;
  }

  public Duration getAccumulatedTime() {
    return Duration.ofNanos(accumulatedNanos);
  }

  public long getSamples() {
    return samples;
  }
}
