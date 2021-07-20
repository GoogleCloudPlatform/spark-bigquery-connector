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
import com.google.cloud.bigquery.TimePartitioning;
import com.google.cloud.bigquery.storage.v1.DataFormat;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
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
  public void testSerializability() throws IOException {
    Configuration hadoopConfiguration = new Configuration();
    DataSourceOptions options = new DataSourceOptions(defaultOptions);
    // test to make sure all members can be serialized.
    new ObjectOutputStream(new ByteArrayOutputStream())
        .writeObject(
            SparkBigQueryConfig.from(
                options.asMap(),
                ImmutableMap.of(),
                hadoopConfiguration,
                DEFAULT_PARALLELISM,
                new SQLConf(),
                SPARK_VERSION,
                Optional.empty()));
  }

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
    assertThat(config.getMaxParallelism()).isEqualTo(OptionalInt.empty());
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
    assertThat(config.getMaterializationExpirationTimeInMinutes()).isEqualTo(24 * 60);
    assertThat(config.getMaxReadRowsRetries()).isEqualTo(3);
    assertThat(config.isUseAvroLogicalTypes()).isFalse();
    assertThat(config.getBigQueryClientConnectTimeout()).isEqualTo(60 * 1000);
    assertThat(config.getBigQueryClientReadTimeout()).isEqualTo(60 * 1000);
    assertThat(config.getBigQueryClientRetrySettings().getMaxAttempts()).isEqualTo(10);
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
                .put("materializationExpirationTimeInMinutes", "100")
                .put("readDataFormat", "ARROW")
                .put("optimizedEmptyProjection", "false")
                .put("createDisposition", "CREATE_NEVER")
                .put("temporaryGcsBucket", "some_bucket")
                .put("intermediateFormat", "ORC")
                .put("useAvroLogicalTypes", "true")
                .put("partitionRequireFilter", "true")
                .put("partitionType", "HOUR")
                .put("partitionField", "some_field")
                .put("partitionExpirationMs", "999")
                .put("clusteredFields", "field1,field2")
                .put("allowFieldAddition", "true")
                .put("allowFieldRelaxation", "true")
                .put("httpConnectTimeout", "10000")
                .put("httpReadTimeout", "20000")
                .put("httpMaxRetry", "5")
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
    assertThat(config.getMaxParallelism()).isEqualTo(OptionalInt.of(99));
    assertThat(config.getTemporaryGcsBucket()).isEqualTo(Optional.of("some_bucket"));
    assertThat(config.getIntermediateFormat())
        .isEqualTo(SparkBigQueryConfig.IntermediateFormat.ORC);
    assertThat(config.getReadDataFormat()).isEqualTo(DataFormat.ARROW);
    assertThat(config.getMaterializationProject()).isEqualTo(Optional.of("vmp"));
    assertThat(config.getMaterializationDataset()).isEqualTo(Optional.of("vmd"));
    assertThat(config.getPartitionType()).isEqualTo(Optional.of(TimePartitioning.Type.HOUR));
    assertThat(config.getPartitionField()).isEqualTo(Optional.of("some_field"));
    assertThat(config.getPartitionExpirationMs()).isEqualTo(OptionalLong.of(999));
    assertThat(config.getPartitionRequireFilter()).isEqualTo(Optional.of(true));
    assertThat(config.getClusteredFields().get()).isEqualTo(ImmutableList.of("field1", "field2"));
    assertThat(config.getCreateDisposition())
        .isEqualTo(Optional.of(JobInfo.CreateDisposition.CREATE_NEVER));
    assertThat(config.getLoadSchemaUpdateOptions())
        .isEqualTo(
            ImmutableList.of(
                JobInfo.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
                JobInfo.SchemaUpdateOption.ALLOW_FIELD_RELAXATION));
    assertThat(config.getMaterializationExpirationTimeInMinutes()).isEqualTo(100);
    assertThat(config.getMaxReadRowsRetries()).isEqualTo(3);
    assertThat(config.isUseAvroLogicalTypes()).isTrue();
    assertThat(config.getBigQueryClientConnectTimeout()).isEqualTo(10000);
    assertThat(config.getBigQueryClientReadTimeout()).isEqualTo(20000);
    assertThat(config.getBigQueryClientRetrySettings().getMaxAttempts()).isEqualTo(5);
  }

  @Test
  public void testConfigFromGlobalOptions() {
    Configuration hadoopConfiguration = new Configuration();
    DataSourceOptions options =
        new DataSourceOptions(
            ImmutableMap.<String, String>builder().put("table", "dataset.table").build());
    ImmutableMap<String, String> globalOptions =
        ImmutableMap.<String, String>builder()
            .put("viewsEnabled", "true")
            .put("spark.datasource.bigquery.temporaryGcsBucket", "bucket")
            .put("bqStorageReadEndpoint", "ep")
            .put("bqEncodedCreateReadSessionRequest", "ec")
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
    assertThat(config.toReadSessionCreatorConfig().endpoint().get()).isEqualTo("ep");
    assertThat(config.toReadSessionCreatorConfig().getRequestEncodedBase().get()).isEqualTo("ec");
  }

  @Test
  public void testGetTableIdWithoutThePartition_PartitionExists() {
    Configuration hadoopConfiguration = new Configuration();
    DataSourceOptions options =
        new DataSourceOptions(
            ImmutableMap.of("table", "dataset.table", "datePartition", "20201010"));
    SparkBigQueryConfig config =
        SparkBigQueryConfig.from(
            options.asMap(),
            ImmutableMap.of(),
            hadoopConfiguration,
            DEFAULT_PARALLELISM,
            new SQLConf(),
            SPARK_VERSION,
            Optional.empty());

    assertThat(config.getTableId().getTable()).isEqualTo("table$20201010");
    assertThat(config.getTableIdWithoutThePartition().getTable()).isEqualTo("table");
    assertThat(config.getTableIdWithoutThePartition().getDataset())
        .isEqualTo(config.getTableId().getDataset());
    assertThat(config.getTableIdWithoutThePartition().getProject())
        .isEqualTo(config.getTableId().getProject());
  }

  @Test
  public void testGetTableIdWithoutThePartition_PartitionMissing() {
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

    assertThat(config.getTableIdWithoutThePartition().getTable())
        .isEqualTo(config.getTableId().getTable());
    assertThat(config.getTableIdWithoutThePartition().getDataset())
        .isEqualTo(config.getTableId().getDataset());
    assertThat(config.getTableIdWithoutThePartition().getProject())
        .isEqualTo(config.getTableId().getProject());
  }

  @Test
  public void testQueryMatching() {
    assertThat(SparkBigQueryConfig.isQuery("table")).isFalse();
    assertThat(SparkBigQueryConfig.isQuery("dataset.table")).isFalse();
    assertThat(SparkBigQueryConfig.isQuery("project.dataset.table")).isFalse();

    assertThat(SparkBigQueryConfig.isQuery("select a,b from table")).isTrue();
    assertThat(SparkBigQueryConfig.isQuery("SELECT\n a,b\nfrom table")).isTrue();
    assertThat(SparkBigQueryConfig.isQuery("SELECT\ta,b from table")).isTrue();
    assertThat(SparkBigQueryConfig.isQuery("WITH bar AS (SELECT * FROM foo)\nSELECT * FROM bar"))
        .isTrue();
  }
}
