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
import org.junit.Before;

import java.time.Instant;

import static com.google.common.truth.Truth.assertThat;

public class LoggingBigQueryStorageReadRowsTracerTest {
  LoggingBigQueryStorageReadRowsTracer loggingTracer;
  BigQueryStorageReadRowsTracer tracer;

  @Before
  public void setup() {
    loggingTracer = new LoggingBigQueryStorageReadRowsTracer("streamName", /*powerOfTwoLogging*/ 3);
    tracer = loggingTracer;
  }

  @Test
  public void testStartAndFinish() {
    assertThat(loggingTracer.startTime).isNull();
    tracer.startStream();
    Instant startTime = loggingTracer.startTime;
    assertThat(startTime).isGreaterThan(Instant.now().minusMillis(20));
    assertThat(loggingTracer.endTime).isNull();

    assertThat(loggingTracer.linesLogged).isEqualTo(0);
    tracer.finished();
    assertThat(loggingTracer.linesLogged).isEqualTo(1);
    assertThat(loggingTracer.endTime).isAtLeast(startTime);
    assertThat(loggingTracer.startTime).isSameInstanceAs(startTime);
    assertThat(loggingTracer.endTime).isNotSameInstanceAs(startTime);
  }

  @Test
  public void testWaitingForSpark() {
    assertThat(loggingTracer.sparkTime.getSamples()).isEqualTo(0);

    tracer.nextBatchNeeded();
    assertThat(loggingTracer.sparkTime.getSamples()).isEqualTo(0);
    // Needs to be called a second time to get the first batch.
    tracer.nextBatchNeeded();
    assertThat(loggingTracer.sparkTime.getSamples()).isEqualTo(1);
  }

  @Test
  public void testWaitingForService() {
    assertThat(loggingTracer.serviceTime.getSamples()).isEqualTo(0);
    tracer.readRowsResponseRequested();
    assertThat(loggingTracer.serviceTime.getSamples()).isEqualTo(0);
    tracer.readRowsResponseObtained(10000);
    assertThat(loggingTracer.serviceTime.getSamples()).isEqualTo(1);
    assertThat(loggingTracer.bytes).isEqualTo(10000);

    tracer.readRowsResponseRequested();
    tracer.readRowsResponseObtained(5000);
    assertThat(loggingTracer.bytes).isEqualTo(15000);
    assertThat(loggingTracer.serviceTime.getSamples()).isEqualTo(2);
  }

  @Test
  public void testParseTime() {
    assertThat(loggingTracer.parseTime.getSamples()).isEqualTo(0);
    tracer.rowsParseStarted();
    assertThat(loggingTracer.parseTime.getSamples()).isEqualTo(0);
    tracer.rowsParseFinished(500);
    assertThat(loggingTracer.parseTime.getSamples()).isEqualTo(1);
    assertThat(loggingTracer.rows).isEqualTo(500);

    tracer.rowsParseStarted();
    tracer.rowsParseFinished(1000);
    assertThat(loggingTracer.parseTime.getSamples()).isEqualTo(2);
    assertThat(loggingTracer.rows).isEqualTo(1500);
  }

  @Test
  public void testLogsAppropriatelyFinished() {
    assertThat(loggingTracer.linesLogged).isEqualTo(0);
    for (int x = 0; x < 8; x++) {
      tracer.nextBatchNeeded();
    }
    assertThat(loggingTracer.linesLogged).isEqualTo(1);
    tracer.finished();
    assertThat(loggingTracer.linesLogged).isEqualTo(2);
  }

  @Test
  public void testFinishedNoLogs() {
    tracer.finished();
    assertThat(loggingTracer.linesLogged).isEqualTo(1);
  }

  @Test
  public void testForkWithPrefix() {
    loggingTracer = new LoggingBigQueryStorageReadRowsTracer("streamName", /*powerOfTwoLogging*/ 3);
    LoggingBigQueryStorageReadRowsTracer newTracer =
        (LoggingBigQueryStorageReadRowsTracer) tracer.forkWithPrefix("newPrefix");
    assertThat(newTracer.getStreamName()).isEqualTo("id-newPrefix-streamName");
  }
}
