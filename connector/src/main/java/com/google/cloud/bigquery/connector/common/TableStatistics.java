package com.google.cloud.bigquery.connector.common;

import java.util.OptionalLong;

/**
 *
 */
public class TableStatistics {

  private final long numberOfRows;
  private final OptionalLong numberOfFilteredRows;

  public TableStatistics(long numberOfRows, OptionalLong numberOfFilteredRows) {
    this.numberOfRows = numberOfRows;
    this.numberOfFilteredRows = numberOfFilteredRows;
  }

  public long getNumberOfRows() {
    return numberOfRows;
  }

  public OptionalLong getNumberOfFilteredRows() {
    return numberOfFilteredRows;
  }

  /**
   * If a filter was used, return numberOfFilteredRows, otherwise return numberOfRows
   */
  public long getQueriedNumberOfRows() {
    return numberOfFilteredRows.orElse(numberOfRows);
  }
}
