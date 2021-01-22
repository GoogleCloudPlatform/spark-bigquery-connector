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

import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobStatistics;
import com.google.cloud.bigquery.TableInfo;
import com.google.common.collect.ImmutableList;

import java.util.Optional;
import java.util.OptionalLong;

/**
 * A DataFrame/table Statistics implementation, taking it's value from the BigQuery metadata. The
 * makes assumptions of the table, in order to support row filtering and column projection. The
 * provided sizes are only an estimation, they aim to provide a ballpark figure of the result's size
 * in bytes, not an accurate number. The number of rows is the actual number.
 */
public class BigQueryEstimatedTableStatistics {

  private static BigQueryEstimatedTableStatistics UNKNOWN_STATISTICS =
      new BigQueryEstimatedTableStatistics(OptionalLong.empty(), OptionalLong.empty());

  private OptionalLong numRows;
  private OptionalLong sizeInBytes;

  private BigQueryEstimatedTableStatistics(OptionalLong numRows, OptionalLong sizeInBytes) {
    this.numRows = numRows;
    this.sizeInBytes = sizeInBytes;
  }

  public static Factory newFactory(TableInfo table, BigQueryClient bigQueryClient) {
    return new Factory(table, bigQueryClient);
  }

  public OptionalLong sizeInBytes() {
    return sizeInBytes;
  }

  public OptionalLong numRows() {
    return numRows;
  }

  public static class Factory {
    final TableInfo table;
    final BigQueryClient bigQueryClient;
    Optional<String> filter = Optional.empty();
    Optional<ImmutableList<String>> selectedColumns = Optional.empty();

    public Factory(TableInfo table, BigQueryClient bigQueryClient) {
      this.table = table;
      this.bigQueryClient = bigQueryClient;
    }

    public Factory setFilter(Optional<String> filter) {
      this.filter = filter;
      return this;
    }

    public Factory setSelectedColumns(Optional<ImmutableList<String>> selectedColumns) {
      this.selectedColumns = selectedColumns;
      return this;
    }

    public BigQueryEstimatedTableStatistics create() {
      // step 1 - get the statistics
      TableStatistics tableStatistics = bigQueryClient.calculateTableSize(table, filter);
      // step 2 - if there is a filter, then issue a query to find the number of rows by the query.
      // Otherwise, take it from the tableInfo.
      long numberOfRows = tableStatistics.getNumberOfRows();
      long numberOfQueriedRows = tableStatistics.getQueriedNumberOfRows();
      if (numberOfQueriedRows == 0) {
        // No rows, means the size is 0 as well
        return new BigQueryEstimatedTableStatistics(OptionalLong.of(0L), OptionalLong.of(0L));
      }
      // Step 3 - if there where selected columns, estimate the size of the table for the entire
      // table. Otherwise, take it from the tableInfo.
      long projectedTableSizeInBytes =
          selectedColumns
              .map(this::getProjectedTableSizeInBytesFromColumns)
              .orElse(table.getNumBytes());
      long sizeInBytes =
          (long) (1.0 * projectedTableSizeInBytes * numberOfQueriedRows / numberOfRows);

      // step 4 - return the statistics
      return new BigQueryEstimatedTableStatistics(
          OptionalLong.of(numberOfQueriedRows), OptionalLong.of(sizeInBytes));
    }

    private long getProjectedTableSizeInBytesFromColumns(ImmutableList<String> columns) {
      String sql = bigQueryClient.createSql(table.getTableId(), columns, new String[] {});
      Job dryRunJob = bigQueryClient.dryRunQuery(sql);
      long projectedTableSizeInBytes =
          ((JobStatistics.QueryStatistics) dryRunJob.getStatistics()).getTotalBytesProcessed();
      return projectedTableSizeInBytes;
    }
  }
}
