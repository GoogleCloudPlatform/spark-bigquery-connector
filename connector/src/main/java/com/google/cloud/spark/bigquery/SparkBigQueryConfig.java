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

import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.connector.common.BigQueryConfig;
import com.google.cloud.bigquery.connector.common.ReadSessionCreatorConfig;
import com.google.cloud.bigquery.storage.v1.DataFormat;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.types.StructType;

import java.util.Collection;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static com.google.cloud.bigquery.connector.common.BigQueryUtil.firstPresent;
import static com.google.cloud.bigquery.connector.common.BigQueryUtil.parseTableId;
import static java.lang.String.format;
import static java.util.stream.Collectors.joining;

public class SparkBigQueryConfig implements BigQueryConfig {

  public static final String VIEWS_ENABLED_OPTION = "viewsEnabled";
  @VisibleForTesting static final DataFormat DEFAULT_READ_DATA_FORMAT = DataFormat.AVRO;

  @VisibleForTesting
  static final FormatOptions DEFAULT_INTERMEDIATE_FORMAT = FormatOptions.parquet();

  private static final String GCS_CONFIG_CREDENTIALS_FILE_PROPERTY =
      "google.cloud.auth.service.account.json.keyfile";
  private static final String GCS_CONFIG_PROJECT_ID_PROPERTY = "fs.gs.project.id";
  private static final String INTERMEDIATE_FORMAT_OPTION = "intermediateFormat";
  private static final String READ_DATA_FORMAT_OPTION = "readDataFormat";
  private static final ImmutableList<String> PERMITTED_READ_DATA_FORMATS =
      ImmutableList.of(DataFormat.ARROW.toString(), DataFormat.AVRO.toString());
  private static final ImmutableList<FormatOptions> PERMITTED_INTERMEDIATE_FORMATS =
      ImmutableList.of(FormatOptions.orc(), FormatOptions.parquet());

  private static final Supplier<Optional<String>> DEFAULT_FALLBACK = () -> Optional.empty();

  TableId tableId;
  Optional<String> parentProjectId;
  Optional<String> credentialsKey;
  Optional<String> credentialsFile;
  Optional<String> accessToken;
  Optional<String> filter = Optional.empty();
  Optional<StructType> schema = Optional.empty();
  OptionalInt maxParallelism = OptionalInt.empty();
  int defaultParallelism = 1;
  Optional<String> temporaryGcsBucket = Optional.empty();
  FormatOptions intermediateFormat = DEFAULT_INTERMEDIATE_FORMAT;
  DataFormat readDataFormat = DEFAULT_READ_DATA_FORMAT;
  boolean combinePushedDownFilters = true;
  boolean viewsEnabled = false;
  Optional<String> materializationProject = Optional.empty();
  Optional<String> materializationDataset = Optional.empty();
  Optional<String> partitionField = Optional.empty();
  OptionalLong partitionExpirationMs = OptionalLong.empty();
  Optional<Boolean> partitionRequireFilter = Optional.empty();
  Optional<String> partitionType = Optional.empty();
  Optional<String[]> clusteredFields = Optional.empty();
  Optional<JobInfo.CreateDisposition> createDisposition = Optional.empty();
  boolean optimizedEmptyProjection = true;
  ImmutableList<JobInfo.SchemaUpdateOption> loadSchemaUpdateOptions = ImmutableList.of();
  int viewExpirationTimeInHours = 24;
  int maxReadRowsRetries = 3;
  // for V2 write with BigQuery Storage Write API
  RetrySettings bigqueryDataWriteHelperRetrySettings =
      RetrySettings.newBuilder().setMaxAttempts(5).build();

  private SparkBigQueryConfig() {
    // empty
  }

  public static SparkBigQueryConfig from(
      DataSourceOptions options,
      ImmutableMap<String, String> globalOptions,
      Configuration hadoopConfiguration,
      int defaultParallelism) {
    SparkBigQueryConfig config = new SparkBigQueryConfig();

    String tableParam = getRequiredOption(options, "table");
    Optional<String> datasetParam = getOption(options, "dataset");
    Optional<String> projectParam =
        firstPresent(
            getOption(options, "project"),
            Optional.ofNullable(hadoopConfiguration.get(GCS_CONFIG_PROJECT_ID_PROPERTY)));
    config.tableId = parseTableId(tableParam, datasetParam, projectParam);
    config.parentProjectId = getAnyOption(globalOptions, options, "parentProject");
    config.credentialsKey = getAnyOption(globalOptions, options, "credentials");
    config.credentialsFile =
        firstPresent(
            getAnyOption(globalOptions, options, "credentialsFile"),
            Optional.ofNullable(hadoopConfiguration.get(GCS_CONFIG_CREDENTIALS_FILE_PROPERTY)));
    config.accessToken = getAnyOption(globalOptions, options, "gcpAccessToken");
    config.filter = getOption(options, "filter");
    config.maxParallelism =
        toOptionalInt(
            getOptionFromMultipleParams(
                    options, ImmutableList.of("maxParallelism", "parallelism"), DEFAULT_FALLBACK)
                .map(Integer::valueOf));
    config.defaultParallelism = defaultParallelism;
    config.temporaryGcsBucket = getAnyOption(globalOptions, options, "temporaryGcsBucket");
    config.intermediateFormat =
        getAnyOption(globalOptions, options, INTERMEDIATE_FORMAT_OPTION)
            .map(String::toUpperCase)
            .map(FormatOptions::of)
            .orElse(DEFAULT_INTERMEDIATE_FORMAT);
    if (!PERMITTED_INTERMEDIATE_FORMATS.contains(config.intermediateFormat)) {
      throw new IllegalArgumentException(
          format(
              "Intermediate format '%s' is not supported. Supported formats are %s",
              config.intermediateFormat.getType(),
              PERMITTED_INTERMEDIATE_FORMATS.stream()
                  .map(FormatOptions::getType)
                  .collect(joining(","))));
    }
    String readDataFormatParam =
        getAnyOption(globalOptions, options, READ_DATA_FORMAT_OPTION)
            .map(String::toUpperCase)
            .orElse(DEFAULT_READ_DATA_FORMAT.toString());
    if (!PERMITTED_READ_DATA_FORMATS.contains(readDataFormatParam)) {
      throw new IllegalArgumentException(
          format(
              "Data read format '%s' is not supported. Supported formats are '%s'",
              readDataFormatParam, String.join(",", PERMITTED_READ_DATA_FORMATS)));
    }
    config.readDataFormat = DataFormat.valueOf(readDataFormatParam);
    config.combinePushedDownFilters =
        getAnyBooleanOption(globalOptions, options, "combinePushedDownFilters", true);
    config.viewsEnabled = getAnyBooleanOption(globalOptions, options, VIEWS_ENABLED_OPTION, false);
    config.materializationProject =
        getAnyOption(
            globalOptions,
            options,
            ImmutableList.of("materializationProject", "viewMaterializationProject"));
    config.materializationDataset =
        getAnyOption(
            globalOptions,
            options,
            ImmutableList.of("materializationDataset", "viewMaterializationDataset"));

    config.partitionField = getOption(options, "partitionField");
    config.partitionExpirationMs =
        toOptionalLong(getOption(options, "partitionExpirationMs").map(Long::valueOf));
    config.partitionRequireFilter =
        getOption(options, "partitionRequireFilter").map(Boolean::valueOf);
    config.partitionType = getOption(options, "partitionType");
    config.clusteredFields = getOption(options, "clusteredFields").map(s -> s.split(","));

    config.createDisposition =
        getOption(options, "createDisposition")
            .map(String::toUpperCase)
            .map(JobInfo.CreateDisposition::valueOf);

    config.optimizedEmptyProjection =
        getAnyBooleanOption(globalOptions, options, "optimizedEmptyProjection", true);

    boolean allowFieldAddition =
        getAnyBooleanOption(globalOptions, options, "allowFieldAddition", false);
    boolean allowFieldRelaxation =
        getAnyBooleanOption(globalOptions, options, "allowFieldRelaxation", false);
    ImmutableList.Builder<JobInfo.SchemaUpdateOption> loadSchemaUpdateOptions =
        ImmutableList.builder();
    if (allowFieldAddition) {
      loadSchemaUpdateOptions.add(JobInfo.SchemaUpdateOption.ALLOW_FIELD_ADDITION);
    }
    if (allowFieldRelaxation) {
      loadSchemaUpdateOptions.add(JobInfo.SchemaUpdateOption.ALLOW_FIELD_RELAXATION);
    }
    config.loadSchemaUpdateOptions = loadSchemaUpdateOptions.build();

    return config;
  }

  private static OptionalInt toOptionalInt(Optional<Integer> o) {
    return o.map(Stream::of).orElse(Stream.empty()).mapToInt(Integer::intValue).findFirst();
  }

  private static OptionalLong toOptionalLong(Optional<Long> o) {
    return o.map(Stream::of).orElse(Stream.empty()).mapToLong(Long::longValue).findFirst();
  }

  private static Supplier<String> defaultBilledProject() {
    return () -> BigQueryOptions.getDefaultInstance().getProjectId();
  }

  private static String getRequiredOption(DataSourceOptions options, String name) {
    return getOption(options, name, DEFAULT_FALLBACK)
        .orElseThrow(() -> new IllegalArgumentException(format("Option %s required.", name)));
  }

  private static String getRequiredOption(
      DataSourceOptions options, String name, Supplier<String> fallback) {
    return getOption(options, name, DEFAULT_FALLBACK).orElseGet(fallback);
  }

  private static Optional<String> getOption(DataSourceOptions options, String name) {
    return getOption(options, name, DEFAULT_FALLBACK);
  }

  private static Optional<String> getOption(
      DataSourceOptions options, String name, Supplier<Optional<String>> fallback) {
    return firstPresent(options.get(name), fallback.get());
  }

  private static Optional<String> getOptionFromMultipleParams(
      DataSourceOptions options, Collection<String> names, Supplier<Optional<String>> fallback) {
    return names.stream()
        .map(name -> getOption(options, name))
        .filter(Optional::isPresent)
        .findFirst()
        .orElseGet(fallback);
  }

  private static Optional<String> getAnyOption(
      ImmutableMap<String, String> globalOptions, DataSourceOptions options, String name) {
    return Optional.ofNullable(options.get(name).orElse(globalOptions.get(name)));
  }

  // gives the option to support old configurations as fallback
  // Used to provide backward compatibility
  private static Optional<String> getAnyOption(
      ImmutableMap<String, String> globalOptions,
      DataSourceOptions options,
      Collection<String> names) {
    return names.stream()
        .map(name -> getAnyOption(globalOptions, options, name))
        .filter(optional -> optional.isPresent())
        .findFirst()
        .orElse(Optional.empty());
  }

  private static boolean getAnyBooleanOption(
      ImmutableMap<String, String> globalOptions,
      DataSourceOptions options,
      String name,
      boolean defaultValue) {
    return getAnyOption(globalOptions, options, name).map(Boolean::valueOf).orElse(defaultValue);
  }

  public TableId getTableId() {
    return tableId;
  }

  @Override
  public Optional<String> getParentProjectId() {
    return parentProjectId;
  }

  @Override
  public Optional<String> getCredentialsKey() {
    return credentialsKey;
  }

  @Override
  public Optional<String> getCredentialsFile() {
    return credentialsKey;
  }

  @Override
  public Optional<String> getAccessToken() {
    return accessToken;
  }

  public Optional<String> getFilter() {
    return filter;
  }

  public Optional<StructType> getSchema() {
    return schema;
  }

  public OptionalInt getMaxParallelism() {
    return maxParallelism;
  }

  public int getDefaultParallelism() {
    return defaultParallelism;
  }

  public Optional<String> getTemporaryGcsBucket() {
    return temporaryGcsBucket;
  }

  public FormatOptions getIntermediateFormat() {
    return intermediateFormat;
  }

  public DataFormat getReadDataFormat() {
    return readDataFormat;
  }

  public boolean isCombinePushedDownFilters() {
    return combinePushedDownFilters;
  }

  public boolean isViewsEnabled() {
    return viewsEnabled;
  }

  @Override
  public Optional<String> getMaterializationProject() {
    return materializationProject;
  }

  @Override
  public Optional<String> getMaterializationDataset() {
    return materializationDataset;
  }

  public Optional<String> getPartitionField() {
    return partitionField;
  }

  public OptionalLong getPartitionExpirationMs() {
    return partitionExpirationMs;
  }

  public Optional<Boolean> getPartitionRequireFilter() {
    return partitionRequireFilter;
  }

  public Optional<String> getPartitionType() {
    return partitionType;
  }

  public Optional<String[]> getClusteredFields() {
    return clusteredFields;
  }

  public Optional<JobInfo.CreateDisposition> getCreateDisposition() {
    return createDisposition;
  }

  public boolean isOptimizedEmptyProjection() {
    return optimizedEmptyProjection;
  }

  public ImmutableList<JobInfo.SchemaUpdateOption> getLoadSchemaUpdateOptions() {
    return loadSchemaUpdateOptions;
  }

  public int getViewExpirationTimeInHours() {
    return viewExpirationTimeInHours;
  }

  public int getMaxReadRowsRetries() {
    return maxReadRowsRetries;
  }

  public RetrySettings getBigqueryDataWriteHelperRetrySettings() {
    return bigqueryDataWriteHelperRetrySettings;
  }

  public ReadSessionCreatorConfig toReadSessionCreatorConfig() {
    return new ReadSessionCreatorConfig(
        viewsEnabled,
        materializationProject,
        materializationDataset,
        viewExpirationTimeInHours,
        readDataFormat,
        maxReadRowsRetries,
        VIEWS_ENABLED_OPTION,
        maxParallelism,
        defaultParallelism);
  }
}
