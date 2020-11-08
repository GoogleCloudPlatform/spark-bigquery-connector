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

import com.google.auth.Credentials;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.connector.common.BigQueryConfig;
import com.google.cloud.bigquery.connector.common.BigQueryCredentialsSupplier;
import com.google.cloud.bigquery.connector.common.ReadSessionCreatorConfig;
import com.google.cloud.bigquery.storage.v1.DataFormat;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.execution.datasources.DataSource;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.cloud.bigquery.connector.common.BigQueryUtil.firstPresent;
import static com.google.cloud.bigquery.connector.common.BigQueryUtil.parseTableId;
import static java.lang.String.format;

// as the config needs to be Serializable, internally it uses
// com.google.common.base.Optional<String> but externally it uses the regular java.util.Optional
public class SparkBigQueryConfig implements BigQueryConfig, Serializable {

  public static final String VIEWS_ENABLED_OPTION = "viewsEnabled";
  public static final String DATE_PARTITION_PARAM = "datePartition";
  @VisibleForTesting static final DataFormat DEFAULT_READ_DATA_FORMAT = DataFormat.ARROW;

  @VisibleForTesting
  static final IntermediateFormat DEFAULT_INTERMEDIATE_FORMAT = IntermediateFormat.PARQUET;

  static final String GCS_CONFIG_CREDENTIALS_FILE_PROPERTY =
      "google.cloud.auth.service.account.json.keyfile";
  static final String GCS_CONFIG_PROJECT_ID_PROPERTY = "fs.gs.project.id";
  private static final String INTERMEDIATE_FORMAT_OPTION = "intermediateFormat";
  private static final String READ_DATA_FORMAT_OPTION = "readDataFormat";
  private static final ImmutableList<String> PERMITTED_READ_DATA_FORMATS =
      ImmutableList.of(DataFormat.ARROW.toString(), DataFormat.AVRO.toString());
  private static final Supplier<com.google.common.base.Optional<String>> DEFAULT_FALLBACK =
      () -> empty();
  private static final String CONF_PREFIX = "spark.datasource.bigquery.";
  TableId tableId;
  String parentProjectId;
  com.google.common.base.Optional<String> credentialsKey;
  com.google.common.base.Optional<String> credentialsFile;
  com.google.common.base.Optional<String> accessToken;
  com.google.common.base.Optional<String> filter = empty();
  com.google.common.base.Optional<StructType> schema = empty();
  int maxParallelism = 1;
  com.google.common.base.Optional<String> temporaryGcsBucket = empty();
  com.google.common.base.Optional<String> persistentGcsBucket = empty();
  com.google.common.base.Optional<String> persistentGcsPath = empty();

  IntermediateFormat intermediateFormat = DEFAULT_INTERMEDIATE_FORMAT;
  DataFormat readDataFormat = DEFAULT_READ_DATA_FORMAT;
  boolean combinePushedDownFilters = true;
  boolean viewsEnabled = false;
  com.google.common.base.Optional<String> materializationProject = empty();
  com.google.common.base.Optional<String> materializationDataset = empty();
  com.google.common.base.Optional<String> partitionField = empty();
  Long partitionExpirationMs = null;
  com.google.common.base.Optional<Boolean> partitionRequireFilter = empty();
  com.google.common.base.Optional<String> partitionType = empty();
  com.google.common.base.Optional<String[]> clusteredFields = empty();
  com.google.common.base.Optional<JobInfo.CreateDisposition> createDisposition = empty();
  boolean optimizedEmptyProjection = true;
  ImmutableList<JobInfo.SchemaUpdateOption> loadSchemaUpdateOptions = ImmutableList.of();
  int viewExpirationTimeInHours = 24;
  int maxReadRowsRetries = 3;

  @VisibleForTesting
  SparkBigQueryConfig() {
    // empty
  }

  @VisibleForTesting
  public static SparkBigQueryConfig from(
      Map<String, String> options,
      ImmutableMap<String, String> originalGlobalOptions,
      Configuration hadoopConfiguration,
      int defaultParallelism,
      SQLConf sqlConf,
      String sparkVersion,
      Optional<StructType> schema) {
    SparkBigQueryConfig config = new SparkBigQueryConfig();

    ImmutableMap<String, String> globalOptions = normalizeConf(originalGlobalOptions);

    String tableParam =
        getOptionFromMultipleParams(options, ImmutableList.of("table", "path"), DEFAULT_FALLBACK)
            .or(
                () -> {
                  throw new IllegalArgumentException("No table has been specified");
                });
    Optional<String> datasetParam = getOption(options, "dataset").toJavaUtil();
    Optional<String> projectParam =
        firstPresent(
            getOption(options, "project").toJavaUtil(),
            com.google.common.base.Optional.fromNullable(
                    hadoopConfiguration.get(GCS_CONFIG_PROJECT_ID_PROPERTY))
                .toJavaUtil());
    Optional<String> datePartitionParam = getOption(options, DATE_PARTITION_PARAM).toJavaUtil();
    datePartitionParam.ifPresent(date -> validateDateFormat(date, DATE_PARTITION_PARAM));
    config.tableId = parseTableId(tableParam, datasetParam, projectParam, datePartitionParam);

    config.parentProjectId =
        getAnyOption(globalOptions, options, "parentProject").or(defaultBilledProject());
    config.credentialsKey = getAnyOption(globalOptions, options, "credentials");
    config.credentialsFile =
        fromJavaUtil(
            firstPresent(
                getAnyOption(globalOptions, options, "credentialsFile").toJavaUtil(),
                com.google.common.base.Optional.fromNullable(
                        hadoopConfiguration.get(GCS_CONFIG_CREDENTIALS_FILE_PROPERTY))
                    .toJavaUtil()));
    config.accessToken = getAnyOption(globalOptions, options, "gcpAccessToken");
    config.filter = getOption(options, "filter");
    config.schema = fromJavaUtil(schema);
    config.maxParallelism =
        getOptionFromMultipleParams(
                options,
                ImmutableList.of("maxParallelism", "parallelism"),
                () -> com.google.common.base.Optional.of(String.valueOf(defaultParallelism)))
            .transform(Integer::valueOf)
            .get()
            .intValue();
    config.temporaryGcsBucket = getAnyOption(globalOptions, options, "temporaryGcsBucket");
    config.persistentGcsBucket = getAnyOption(globalOptions, options, "persistentGcsBucket");
    config.persistentGcsPath = getOption(options, "persistentGcsPath");
    config.intermediateFormat =
        getAnyOption(globalOptions, options, INTERMEDIATE_FORMAT_OPTION)
            .transform(String::toLowerCase)
            .transform(format -> IntermediateFormat.from(format, sparkVersion, sqlConf))
            .or(DEFAULT_INTERMEDIATE_FORMAT);
    String readDataFormatParam =
        getAnyOption(globalOptions, options, READ_DATA_FORMAT_OPTION)
            .transform(String::toUpperCase)
            .or(DEFAULT_READ_DATA_FORMAT.toString());
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
        getOption(options, "partitionExpirationMs").transform(Long::valueOf).orNull();
    config.partitionRequireFilter =
        getOption(options, "partitionRequireFilter").transform(Boolean::valueOf);
    config.partitionType = getOption(options, "partitionType");
    config.clusteredFields = getOption(options, "clusteredFields").transform(s -> s.split(","));

    config.createDisposition =
        getOption(options, "createDisposition")
            .transform(String::toUpperCase)
            .transform(JobInfo.CreateDisposition::valueOf);

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

  private static void validateDateFormat(String date, String optionName) {
    try {
      DateTimeFormatter.BASIC_ISO_DATE.parse(date);
    } catch (DateTimeParseException e) {
      throw new IllegalArgumentException(
          String.format("Invalid argument for option %s, format is YYYYMMDD", optionName));
    }
  }

  private static com.google.common.base.Supplier<String> defaultBilledProject() {
    return () -> BigQueryOptions.getDefaultInstance().getProjectId();
  }

  private static String getRequiredOption(Map<String, String> options, String name) {
    return getOption(options, name, DEFAULT_FALLBACK)
        .toJavaUtil()
        .orElseThrow(() -> new IllegalArgumentException(format("Option %s required.", name)));
  }

  private static String getRequiredOption(
      Map<String, String> options, String name, com.google.common.base.Supplier<String> fallback) {
    return getOption(options, name, DEFAULT_FALLBACK).or(fallback);
  }

  private static com.google.common.base.Optional<String> getOption(
      Map<String, String> options, String name) {
    return getOption(options, name, DEFAULT_FALLBACK);
  }

  private static com.google.common.base.Optional<String> getOption(
      Map<String, String> options,
      String name,
      Supplier<com.google.common.base.Optional<String>> fallback) {
    return fromJavaUtil(
        firstPresent(
            Optional.ofNullable(options.get(name.toLowerCase())), fallback.get().toJavaUtil()));
  }

  private static com.google.common.base.Optional<String> getOptionFromMultipleParams(
      Map<String, String> options,
      Collection<String> names,
      Supplier<com.google.common.base.Optional<String>> fallback) {
    return names.stream()
        .map(name -> getOption(options, name))
        .filter(com.google.common.base.Optional::isPresent)
        .findFirst()
        .orElseGet(fallback);
  }

  private static com.google.common.base.Optional<String> getAnyOption(
      ImmutableMap<String, String> globalOptions, Map<String, String> options, String name) {
    return com.google.common.base.Optional.fromNullable(options.get(name.toLowerCase()))
        .or(com.google.common.base.Optional.fromNullable(globalOptions.get(name)));
  }

  // gives the option to support old configurations as fallback
  // Used to provide backward compatibility
  private static com.google.common.base.Optional<String> getAnyOption(
      ImmutableMap<String, String> globalOptions,
      Map<String, String> options,
      Collection<String> names) {
    return names.stream()
        .map(name -> getAnyOption(globalOptions, options, name))
        .filter(optional -> optional.isPresent())
        .findFirst()
        .orElse(empty());
  }

  private static boolean getAnyBooleanOption(
      ImmutableMap<String, String> globalOptions,
      Map<String, String> options,
      String name,
      boolean defaultValue) {
    return getAnyOption(globalOptions, options, name).transform(Boolean::valueOf).or(defaultValue);
  }

  static ImmutableMap<String, String> normalizeConf(Map<String, String> conf) {
    Map<String, String> normalizeConf =
        conf.entrySet().stream()
            .filter(e -> e.getKey().startsWith(CONF_PREFIX))
            .collect(
                Collectors.toMap(
                    e -> e.getKey().substring(CONF_PREFIX.length()), e -> e.getValue()));
    Map<String, String> result = new HashMap<>(conf);
    result.putAll(normalizeConf);
    return ImmutableMap.copyOf(result);
  }

  private static com.google.common.base.Optional empty() {
    return com.google.common.base.Optional.absent();
  }

  private static com.google.common.base.Optional fromJavaUtil(Optional o) {
    return com.google.common.base.Optional.fromJavaUtil(o);
  }

  public Credentials createCredentials() {
    return new BigQueryCredentialsSupplier(
            accessToken.toJavaUtil(), credentialsKey.toJavaUtil(), credentialsFile.toJavaUtil())
        .getCredentials();
  }

  public TableId getTableId() {
    return tableId;
  }

  @Override
  public String getParentProjectId() {
    return parentProjectId;
  }

  @Override
  public Optional<String> getCredentialsKey() {
    return credentialsKey.toJavaUtil();
  }

  @Override
  public Optional<String> getCredentialsFile() {
    return credentialsFile.toJavaUtil();
  }

  @Override
  public Optional<String> getAccessToken() {
    return accessToken.toJavaUtil();
  }

  public Optional<String> getFilter() {
    return filter.toJavaUtil();
  }

  public Optional<StructType> getSchema() {
    return schema.toJavaUtil();
  }

  public int getMaxParallelism() {
    return maxParallelism;
  }

  public Optional<String> getTemporaryGcsBucket() {
    return temporaryGcsBucket.toJavaUtil();
  }

  public Optional<String> getPersistentGcsBucket() {
    return persistentGcsBucket.toJavaUtil();
  }

  public Optional<String> getPersistentGcsPath() {
    return persistentGcsPath.toJavaUtil();
  }

  public IntermediateFormat getIntermediateFormat() {
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
    return materializationProject.toJavaUtil();
  }

  @Override
  public Optional<String> getMaterializationDataset() {
    return materializationDataset.toJavaUtil();
  }

  public Optional<String> getPartitionField() {
    return partitionField.toJavaUtil();
  }

  public OptionalLong getPartitionExpirationMs() {
    return partitionExpirationMs == null
        ? OptionalLong.empty()
        : OptionalLong.of(partitionExpirationMs);
  }

  public Optional<Boolean> getPartitionRequireFilter() {
    return partitionRequireFilter.toJavaUtil();
  }

  public Optional<String> getPartitionType() {
    return partitionType.toJavaUtil();
  }

  public Optional<String[]> getClusteredFields() {
    return clusteredFields.toJavaUtil();
  }

  public Optional<JobInfo.CreateDisposition> getCreateDisposition() {
    return createDisposition.toJavaUtil();
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

  public ReadSessionCreatorConfig toReadSessionCreatorConfig() {
    return new ReadSessionCreatorConfig(
        viewsEnabled,
        materializationProject.toJavaUtil(),
        materializationDataset.toJavaUtil(),
        viewExpirationTimeInHours,
        readDataFormat,
        maxReadRowsRetries,
        VIEWS_ENABLED_OPTION,
        maxParallelism);
  }

  enum IntermediateFormat {
    AVRO("avro", FormatOptions.avro()),
    AVRO_2_3("com.databricks.spark.avro", FormatOptions.avro()),
    ORC("orc", FormatOptions.orc()),
    PARQUET("parquet", FormatOptions.parquet());

    private static Set<String> PERMITTED_DATA_SOURCES =
        Stream.of(values())
            .map(IntermediateFormat::getDataSource)
            .filter(dataSource -> !dataSource.contains("."))
            .collect(Collectors.toSet());

    private final String dataSource;
    private final FormatOptions formatOptions;

    IntermediateFormat(String dataSource, FormatOptions formatOptions) {
      this.dataSource = dataSource;
      this.formatOptions = formatOptions;
    }

    public static IntermediateFormat from(String format, String sparkVersion, SQLConf sqlConf) {
      Preconditions.checkArgument(
          PERMITTED_DATA_SOURCES.contains(format.toLowerCase()),
          "Data read format '%s' is not supported. Supported formats are %s",
          format,
          PERMITTED_DATA_SOURCES);

      if (format.equalsIgnoreCase("avro")) {
        IntermediateFormat intermediateFormat = isSpark24OrAbove(sparkVersion) ? AVRO : AVRO_2_3;

        try {
          DataSource.lookupDataSource(intermediateFormat.getDataSource(), sqlConf);
        } catch (Exception ae) {
          throw missingAvroException(sparkVersion, ae);
        }

        return intermediateFormat;
      }

      // we have made sure that the format exist in the precondition, so findFirst() will
      // always find an instance
      return Stream.of(values())
          .filter(intermediateFormat -> intermediateFormat.getDataSource().equalsIgnoreCase(format))
          .findFirst()
          .get();
    }

    static boolean isSpark24OrAbove(String sparkVersion) {
      return sparkVersion.compareTo("2.4") > 0;
    }

    // could not load the spark-avro data source
    private static IllegalStateException missingAvroException(
        String sparkVersion, Exception cause) {
      String avroPackage;
      if (isSpark24OrAbove(sparkVersion)) {
        String scalaVersion = scala.util.Properties.versionNumberString();
        String scalaShortVersion = scalaVersion.substring(0, scalaVersion.lastIndexOf('.'));
        avroPackage =
            String.format("org.apache.spark:spark-avro_%s:%s", scalaShortVersion, sparkVersion);
      } else {
        avroPackage = "com.databricks:spark-avro_2.11:4.0.0";
      }
      String message =
          String.format(
              "Avro writing is not supported, as the spark-avro has not been found. "
                  + "Please re-run spark with the --packages %s parameter",
              avroPackage);

      return new IllegalStateException(message, cause);
    }

    public String getDataSource() {
      return dataSource;
    }

    public FormatOptions getFormatOptions() {
      return formatOptions;
    }
  }
}
