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
package com.google.cloud.bigquery.connector.common;

import com.google.cloud.bigquery.*;
import com.google.common.collect.ImmutableList;
import org.junit.BeforeClass;
import org.junit.Test;

import java.math.BigInteger;
import java.util.Optional;
import java.util.OptionalLong;

import static com.google.common.truth.Truth8.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BigQueryEstimatedTableStatisticsTest {

  public static final TableId TABLE_ID = TableId.of("dataset", "table");
  public static final StandardTableDefinition TABLE_DEFINITION =
      StandardTableDefinition.newBuilder().setNumBytes(1000L).setNumRows(10L).build();
  public static final TableInfo TABLE = mock(TableInfo.class);

  @BeforeClass
  public static void prepareMocks() {
    when(TABLE.getNumBytes()).thenReturn(TABLE_DEFINITION.getNumBytes());
    when(TABLE.getNumRows()).thenReturn(BigInteger.valueOf(TABLE_DEFINITION.getNumRows()));
    when(TABLE.getDefinition()).thenReturn(TABLE_DEFINITION);
  }

  @Test
  public void testNoFilterNoProjection() {
    BigQueryClient bigQueryClient = mock(BigQueryClient.class);
    prepareCalculateSizeNoFilter(bigQueryClient);
    BigQueryEstimatedTableStatistics s =
        BigQueryEstimatedTableStatistics.newFactory(TABLE, bigQueryClient).create();
    assertThat(s.numRows()).hasValue(10L);
    assertThat(s.sizeInBytes()).hasValue(1000L);
  }

  @Test
  public void testWithFilterNoProjection() {
    BigQueryClient bigQueryClient = mock(BigQueryClient.class);
    prepareCalculateSizeWithFilter(bigQueryClient);
    BigQueryEstimatedTableStatistics s =
        BigQueryEstimatedTableStatistics.newFactory(TABLE, bigQueryClient)
            .setFilter(Optional.of("some_filter = 1"))
            .create();
    // Original table has 10 rows and 1000 bytes, each row is 100 bytes
    // The filter returns just 3 rows.
    // Hence a size of 3*100=300 bytes is expected.
    assertThat(s.numRows()).hasValue(3L);
    assertThat(s.sizeInBytes()).hasValue(300L);
  }

  @Test
  public void testNoFilterWithProjection() {
    BigQueryClient bigQueryClient = mock(BigQueryClient.class);
    prepareCalculateSizeNoFilter(bigQueryClient);
    prepareDryRun(bigQueryClient);
    BigQueryEstimatedTableStatistics s =
        BigQueryEstimatedTableStatistics.newFactory(TABLE, bigQueryClient)
            .setSelectedColumns(Optional.of(ImmutableList.of("col_a", "col_b")))
            .create();
    // Original table has 10 rows and 1000 bytes, each row is 100 bytes.
    // Field projection return just 40 bytes per row, but does not change the number of rows.
    // Hence a size of 10*40=400 bytes is expected.
    assertThat(s.numRows()).hasValue(10L);
    assertThat(s.sizeInBytes()).hasValue(400L);
  }

  @Test
  public void testWithFilterWithProjection() {
    BigQueryClient bigQueryClient = mock(BigQueryClient.class);
    prepareCalculateSizeWithFilter(bigQueryClient);
    prepareDryRun(bigQueryClient);
    BigQueryEstimatedTableStatistics s =
        BigQueryEstimatedTableStatistics.newFactory(TABLE, bigQueryClient)
            .setFilter(Optional.of("some_filter = 1"))
            .setSelectedColumns(Optional.of(ImmutableList.of("col_a", "col_b")))
            .create();
    // Original table has 10 rows and 1000 bytes, each row is 100 bytes.
    // Field projection return just 40 bytes per row, the filter return just 3 rows.
    // Hence a size of 3*40=120 bytes is expected.    assertThat(s.sizeInBytes()).hasValue(120L);
  }

  private void prepareDryRun(BigQueryClient mockBigQueryClient) {
    JobStatistics.QueryStatistics mockQueryStatistics = mock(JobStatistics.QueryStatistics.class);
    when(mockQueryStatistics.getTotalBytesProcessed()).thenReturn(400L);
    Job mockJob = mock(Job.class);
    when(mockJob.getStatistics()).thenReturn(mockQueryStatistics);
    when(mockBigQueryClient.dryRunQuery(any())).thenReturn(mockJob);
    when(mockBigQueryClient.createSql(any(TableId.class), any(ImmutableList.class), any()))
        .thenReturn("");
  }

  private void prepareCalculateSizeNoFilter(BigQueryClient mockBigQueryClient) {
    when(mockBigQueryClient.calculateTableSize(any(TableInfo.class), any(Optional.class)))
        .thenReturn(new TableStatistics(10, OptionalLong.empty()));
  }

  private void prepareCalculateSizeWithFilter(BigQueryClient mockBigQueryClient) {
    when(mockBigQueryClient.calculateTableSize(any(TableInfo.class), any(Optional.class)))
        .thenReturn(new TableStatistics(10, OptionalLong.of(3L)));
  }
}
