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
package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.bigquery.connector.common.BigQueryEstimatedTableStatistics;
import org.apache.spark.sql.sources.v2.reader.Statistics;

import java.util.OptionalLong;

/**
 * A DataFrame/table Statistics implementation, taking it's value from the common library
 * BigQueryEstimatedTableStatistics.
 */
class BigQueryEstimatedTableStatisticsSparkAdapter implements Statistics {

  private final BigQueryEstimatedTableStatistics statistics;

  BigQueryEstimatedTableStatisticsSparkAdapter(BigQueryEstimatedTableStatistics statistics) {
    this.statistics = statistics;
  }

  @Override
  public OptionalLong sizeInBytes() {
    return statistics.sizeInBytes();
  }

  @Override
  public OptionalLong numRows() {
    return statistics.numRows();
  }
}
