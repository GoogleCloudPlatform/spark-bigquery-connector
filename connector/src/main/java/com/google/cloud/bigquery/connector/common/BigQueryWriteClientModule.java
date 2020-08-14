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

import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.spark.bigquery.SparkBigQueryConfig;
import com.google.cloud.spark.bigquery.v2.BigQueryDataSourceWriter;
import com.google.inject.*;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.StructType;

public class BigQueryWriteClientModule implements com.google.inject.Module {

  final String writeUUID;
  final SaveMode saveMode;
  final StructType sparkSchema;

  public BigQueryWriteClientModule(String writeUUID, SaveMode saveMode, StructType sparkSchema) {
    this.writeUUID = writeUUID;
    this.saveMode = saveMode;
    this.sparkSchema = sparkSchema;
  }

  @Override
  public void configure(Binder binder) {
    // BigQuery related
    binder.bind(BigQueryWriteClientFactory.class).in(Scopes.SINGLETON);
  }

  @Singleton
  @Provides
  public BigQueryDataSourceWriter provideDataSourceWriter(
      BigQueryClient bigQueryClient,
      BigQueryWriteClientFactory bigQueryWriteClientFactory,
      SparkBigQueryConfig config) {
    TableId tableId = config.getTableId();
    RetrySettings bigqueryDataWriteHelperRetrySettings =
        config.getBigqueryDataWriteHelperRetrySettings();
    return new BigQueryDataSourceWriter(
        bigQueryClient,
        bigQueryWriteClientFactory,
        tableId,
        writeUUID,
        saveMode,
        sparkSchema,
        bigqueryDataWriteHelperRetrySettings);
  }
}
