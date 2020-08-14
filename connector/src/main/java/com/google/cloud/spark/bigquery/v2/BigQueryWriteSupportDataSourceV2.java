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

import com.google.cloud.bigquery.connector.common.BigQueryClientModule;
import com.google.cloud.bigquery.connector.common.BigQueryWriteClientModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.WriteSupport;
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class BigQueryWriteSupportDataSourceV2 implements DataSourceV2, WriteSupport {

  final Logger logger = LoggerFactory.getLogger(BigQueryWriteSupportDataSourceV2.class);

  @Override
  public Optional<DataSourceWriter> createWriter(
      String writeUUID, StructType schema, SaveMode mode, DataSourceOptions options) {
    logger.trace("createWriter({}, {}, {}, {})", writeUUID, schema, mode, options);

    SparkSession spark = getDefaultSparkSessionOrCreate();

    Injector injector =
        Guice.createInjector(
            new BigQueryWriteClientModule(writeUUID, mode, schema),
            new BigQueryClientModule(),
            new SparkBigQueryConnectorModule(spark, options, Optional.of(schema)));

    BigQueryDataSourceWriter writer = injector.getInstance(BigQueryDataSourceWriter.class);
    return Optional.of(writer);
  }

  private SparkSession getDefaultSparkSessionOrCreate() {
    scala.Option<SparkSession> defaultSpareSession = SparkSession.getDefaultSession();
    if (defaultSpareSession.isDefined()) {
      return defaultSpareSession.get();
    }
    return SparkSession.builder().appName("spark-bigquery-connector").getOrCreate();
  }
}
