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

import org.junit.Test;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static com.google.common.truth.Truth.assertThat;

public class DurationTimerTest {
  @Test
  public void testStartStopPairedCall() throws Exception {
    DurationTimer timer = new DurationTimer();
    assertThat(timer.getSamples()).isEqualTo(0);

    timer.start();
    TimeUnit.MILLISECONDS.sleep(2);
    timer.finish();
    assertThat(timer.getSamples()).isEqualTo(1);
    Duration accumulated = timer.getAccumulatedTime();
    assertThat(accumulated).isAtLeast(Duration.ofMillis(1));

    timer.start();
    TimeUnit.MILLISECONDS.sleep(2);
    timer.finish();
    assertThat(timer.getSamples()).isEqualTo(2);
    assertThat(timer.getAccumulatedTime()).isAtLeast(Duration.ofMillis(1).plus(accumulated));
  }

  @Test
  public void testFinishedByItselfCall() throws Exception {
    DurationTimer timer = new DurationTimer();
    assertThat(timer.getSamples()).isEqualTo(0);
    timer.finish();
    assertThat(timer.getSamples()).isEqualTo(0);

    TimeUnit.MILLISECONDS.sleep(2);
    timer.finish();
    assertThat(timer.getSamples()).isEqualTo(1);
    Duration accumulated = timer.getAccumulatedTime();
    assertThat(accumulated).isAtLeast(Duration.ofMillis(1));

    TimeUnit.MILLISECONDS.sleep(2);
    timer.finish();
    assertThat(timer.getSamples()).isEqualTo(2);
    assertThat(timer.getAccumulatedTime()).isAtLeast(Duration.ofMillis(1).plus(accumulated));
  }
}
