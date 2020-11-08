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
package com.google.cloud.spark.bigquery;

import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.storage.v1.DataFormat;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.junit.Test;

import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;

import static com.google.common.truth.Truth.assertThat;

public class SparkBigQueryConfigTest {

  public static final int DEFAULT_PARALLELISM = 10;
  public static final String SPARK_VERSION = "2.4.0";
  ImmutableMap<String, String> defaultOptions = ImmutableMap.of("table", "dataset.table");
  // "project", "test_project"); // to remove the need for default project

  @Test
  public void testDefaults() {
    Configuration hadoopConfiguration = new Configuration();
    DataSourceOptions options = new DataSourceOptions(defaultOptions);
    SparkBigQueryConfig config =
        SparkBigQueryConfig.from(
            options.asMap(),
            ImmutableMap.of(),
            hadoopConfiguration,
            DEFAULT_PARALLELISM,
            new SQLConf(),
            SPARK_VERSION,
            Optional.empty());
    assertThat(config.getTableId()).isEqualTo(TableId.of("dataset", "table"));
    assertThat(config.getFilter()).isEqualTo(Optional.empty());
    assertThat(config.getSchema()).isEqualTo(Optional.empty());
    assertThat(config.getMaxParallelism()).isEqualTo(DEFAULT_PARALLELISM);
    assertThat(config.getTemporaryGcsBucket()).isEqualTo(Optional.empty());
    assertThat(config.getIntermediateFormat())
        .isEqualTo(SparkBigQueryConfig.DEFAULT_INTERMEDIATE_FORMAT);
    assertThat(config.getReadDataFormat()).isEqualTo(SparkBigQueryConfig.DEFAULT_READ_DATA_FORMAT);
    assertThat(config.getMaterializationProject()).isEqualTo(Optional.empty());
    assertThat(config.getMaterializationDataset()).isEqualTo(Optional.empty());
    assertThat(config.getPartitionField()).isEqualTo(Optional.empty());
    assertThat(config.getPartitionExpirationMs()).isEqualTo(OptionalLong.empty());
    assertThat(config.getPartitionRequireFilter()).isEqualTo(Optional.empty());
    assertThat(config.getPartitionType()).isEqualTo(Optional.empty());
    assertThat(config.getClusteredFields()).isEqualTo(Optional.empty());
    assertThat(config.getCreateDisposition()).isEqualTo(Optional.empty());
    assertThat(config.getLoadSchemaUpdateOptions()).isEqualTo(ImmutableList.of());
    assertThat(config.getViewExpirationTimeInHours()).isEqualTo(24);
    assertThat(config.getMaxReadRowsRetries()).isEqualTo(3);
  }

  @Test
  public void testConfigFromOptions() {
    Configuration hadoopConfiguration = new Configuration();
    DataSourceOptions options =
        new DataSourceOptions(
            ImmutableMap.<String, String>builder()
                .put("table", "test_t")
                .put("dataset", "test_d")
                .put("project", "test_p")
                .put("filter", "test > 0")
                .put("parentProject", "test_pp")
                .put("maxParallelism", "99")
                .put("viewsEnabled", "true")
                .put("viewMaterializationProject", "vmp")
                .put("viewMaterializationDataset", "vmd")
                .put("readDataFormat", "ARROW")
                .put("optimizedEmptyProjection", "false")
                .put("createDisposition", "CREATE_NEVER")
                .put("temporaryGcsBucket", "some_bucket")
                .put("intermediateFormat", "ORC")
                .put("partitionRequireFilter", "true")
                .put("partitionField", "some_field")
                .put("partitionExpirationMs", "999")
                .put("clusteredFields", "field1,field2")
                .put("allowFieldAddition", "true")
                .put("allowFieldRelaxation", "true")
                .build());
    SparkBigQueryConfig config =
        SparkBigQueryConfig.from(
            options.asMap(),
            ImmutableMap.of(),
            hadoopConfiguration,
            DEFAULT_PARALLELISM,
            new SQLConf(),
            SPARK_VERSION,
            Optional.empty());
    assertThat(config.getTableId()).isEqualTo(TableId.of("test_p", "test_d", "test_t"));
    assertThat(config.getFilter()).isEqualTo(Optional.of("test > 0"));
    assertThat(config.getSchema()).isEqualTo(Optional.empty());
    assertThat(config.getMaxParallelism()).isEqualTo(99);
    assertThat(config.getTemporaryGcsBucket()).isEqualTo(Optional.of("some_bucket"));
    assertThat(config.getIntermediateFormat())
        .isEqualTo(SparkBigQueryConfig.IntermediateFormat.ORC);
    assertThat(config.getReadDataFormat()).isEqualTo(DataFormat.ARROW);
    assertThat(config.getMaterializationProject()).isEqualTo(Optional.of("vmp"));
    assertThat(config.getMaterializationDataset()).isEqualTo(Optional.of("vmd"));
    assertThat(config.getPartitionField()).isEqualTo(Optional.of("some_field"));
    assertThat(config.getPartitionExpirationMs()).isEqualTo(OptionalLong.of(999));
    assertThat(config.getPartitionRequireFilter()).isEqualTo(Optional.of(true));
    assertThat(config.getClusteredFields().get()).isEqualTo(new String[] {"field1", "field2"});
    assertThat(config.getCreateDisposition())
        .isEqualTo(Optional.of(JobInfo.CreateDisposition.CREATE_NEVER));
    assertThat(config.getLoadSchemaUpdateOptions())
        .isEqualTo(
            ImmutableList.of(
                JobInfo.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
                JobInfo.SchemaUpdateOption.ALLOW_FIELD_RELAXATION));
    assertThat(config.getViewExpirationTimeInHours()).isEqualTo(24);
    assertThat(config.getMaxReadRowsRetries()).isEqualTo(3);
  }

  @Test
  public void testConfigFromGlobalOptions() {
    Configuration hadoopConfiguration = new Configuration();
    DataSourceOptions options =
        new DataSourceOptions(
            ImmutableMap.<String, String>builder().put("table", "dataset.table").build());
    ImmutableMap<String, String> globalOptions = ImmutableMap.<String, String>builder()
            .put("viewsEnabled", "true")
            .put("spark.datasource.bigquery.temporaryGcsBucket", "bucket")
            .build();
    SparkBigQueryConfig config =
            SparkBigQueryConfig.from(
                    options.asMap(),
                    globalOptions,
                    hadoopConfiguration,
                    DEFAULT_PARALLELISM,
                    new SQLConf(),
                    SPARK_VERSION,
                    Optional.empty());

    assertThat(config.isViewsEnabled()).isTrue();
    assertThat(config.getTemporaryGcsBucket()).isEqualTo(Optional.of("bucket"));
  }
}
