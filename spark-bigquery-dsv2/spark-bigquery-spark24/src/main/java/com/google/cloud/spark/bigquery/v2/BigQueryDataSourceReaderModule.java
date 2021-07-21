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
package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.bigquery.connector.common.BigQueryReadClientFactory;
import com.google.cloud.bigquery.connector.common.BigQueryTracerFactory;
import com.google.cloud.spark.bigquery.SparkBigQueryConfig;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import org.apache.spark.sql.SparkSession;

public class BigQueryDataSourceReaderModule implements Module {
  @Override
  public void configure(Binder binder) {
    // empty
  }

  @Singleton
  @Provides
  public BigQueryDataSourceReader provideDataSourceReader(
      BigQueryClient bigQueryClient,
      BigQueryReadClientFactory bigQueryReadClientFactory,
      BigQueryTracerFactory tracerFactory,
      SparkBigQueryConfig config,
      SparkSession sparkSession) {
    TableInfo tableInfo = bigQueryClient.getReadTable(config.toReadTableOptions());
    return new BigQueryDataSourceReader(
        tableInfo,
        bigQueryClient,
        bigQueryReadClientFactory,
        tracerFactory,
        config.toReadSessionCreatorConfig(),
        config.getFilter(),
        config.getSchema(),
        sparkSession.sparkContext().applicationId());
  }
}
