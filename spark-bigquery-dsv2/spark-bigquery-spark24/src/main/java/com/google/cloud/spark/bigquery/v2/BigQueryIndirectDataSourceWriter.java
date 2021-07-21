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

import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Clustering;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.LoadJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TimePartitioning;
import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.bigquery.connector.common.BigQueryUtil;
import com.google.cloud.http.BaseHttpServiceException;
import com.google.cloud.spark.bigquery.AvroSchemaConverter;
import com.google.cloud.spark.bigquery.SchemaConverters;
import com.google.cloud.spark.bigquery.SparkBigQueryConfig;
import com.google.cloud.spark.bigquery.SparkBigQueryUtil;
import com.google.cloud.spark.bigquery.SupportedCustomDataType;
import java.util.function.Function;
import org.apache.beam.sdk.io.hadoop.SerializableConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter;
import org.apache.spark.sql.sources.v2.writer.DataWriterFactory;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A DataSourceWriter implemented by first writing the DataFrame's data into GCS in an intermediate
 * format, and then triggering a BigQuery load job on this data. Hence the "indirect" - the data
 * goes through an intermediate storage.
 */
public class BigQueryIndirectDataSourceWriter implements DataSourceWriter {

  private static final Logger logger =
      LoggerFactory.getLogger(BigQueryIndirectDataSourceWriter.class);

  private final BigQueryClient bigQueryClient;
  private final SparkBigQueryConfig config;
  private final Configuration hadoopConfiguration;
  private final StructType sparkSchema;
  private final String writeUUID;
  private final SaveMode saveMode;
  private final Path gcsPath;
  private final Optional<IntermediateDataCleaner> intermediateDataCleaner;

  public BigQueryIndirectDataSourceWriter(
      BigQueryClient bigQueryClient,
      SparkBigQueryConfig config,
      Configuration hadoopConfiguration,
      StructType sparkSchema,
      String writeUUID,
      SaveMode saveMode,
      Path gcsPath,
      Optional<IntermediateDataCleaner> intermediateDataCleaner) {
    this.bigQueryClient = bigQueryClient;
    this.config = config;
    this.hadoopConfiguration = hadoopConfiguration;
    this.sparkSchema = sparkSchema;
    this.writeUUID = writeUUID;
    this.saveMode = saveMode;
    this.gcsPath = gcsPath;
    this.intermediateDataCleaner = intermediateDataCleaner;
  }

  static <T> Iterable<T> wrap(final RemoteIterator<T> remoteIterator) {
    return () ->
        new Iterator<T>() {
          @Override
          public boolean hasNext() {
            try {
              return remoteIterator.hasNext();
            } catch (IOException e) {
              throw new UncheckedIOException(e);
            }
          }

          @Override
          public T next() {
            try {
              return remoteIterator.next();
            } catch (IOException e) {
              throw new UncheckedIOException(e);
            }
          }
        };
  }

  @Override
  public DataWriterFactory<InternalRow> createWriterFactory() {
    org.apache.avro.Schema avroSchema = AvroSchemaConverter.sparkSchemaToAvroSchema(sparkSchema);
    return new BigQueryIndirectDataWriterFactory(
        new SerializableConfiguration(hadoopConfiguration),
        gcsPath.toString(),
        sparkSchema,
        avroSchema.toString());
  }

  @Override
  public void commit(WriterCommitMessage[] messages) {
    logger.info(
        "Data has been successfully written to GCS. Going to load {} files to BigQuery",
        messages.length);
    try {
      List<String> sourceUris =
          Stream.of(messages)
              .map(msg -> ((BigQueryIndirectWriterCommitMessage) msg).getUri())
              .collect(Collectors.toList());
      loadDataToBigQuery(sourceUris);
      updateMetadataIfNeeded();
      logger.info("Data has been successfully loaded to BigQuery");
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    } finally {
      cleanTemporaryGcsPathIfNeeded();
    }
  }

  @Override
  public void abort(WriterCommitMessage[] messages) {
    try {
      logger.warn(
          "Aborting write {} for table {}",
          writeUUID,
          BigQueryUtil.friendlyTableName(config.getTableId()));
    } finally {
      cleanTemporaryGcsPathIfNeeded();
    }
  }

  void loadDataToBigQuery(List<String> sourceUris) throws IOException {
    // Solving Issue #248
    List<String> optimizedSourceUris = SparkBigQueryUtil.optimizeLoadUriListForSpark(sourceUris);

    LoadJobConfiguration.Builder jobConfiguration =
        LoadJobConfiguration.newBuilder(
                config.getTableId(),
                optimizedSourceUris,
                config.getIntermediateFormat().getFormatOptions())
            .setCreateDisposition(JobInfo.CreateDisposition.CREATE_IF_NEEDED)
            .setWriteDisposition(saveModeToWriteDisposition(saveMode))
            .setAutodetect(true);

    config.getCreateDisposition().ifPresent(jobConfiguration::setCreateDisposition);

    if (config.getPartitionField().isPresent() || config.getPartitionType().isPresent()) {
      TimePartitioning.Builder timePartitionBuilder =
          TimePartitioning.newBuilder(config.getPartitionTypeOrDefault());
      config.getPartitionExpirationMs().ifPresent(timePartitionBuilder::setExpirationMs);
      config.getPartitionRequireFilter().ifPresent(timePartitionBuilder::setRequirePartitionFilter);
      config.getPartitionField().ifPresent(timePartitionBuilder::setField);
      jobConfiguration.setTimePartitioning(timePartitionBuilder.build());
      config
          .getClusteredFields()
          .ifPresent(
              clusteredFields -> {
                Clustering clustering = Clustering.newBuilder().setFields(clusteredFields).build();
                jobConfiguration.setClustering(clustering);
              });
    }

    if (!config.getLoadSchemaUpdateOptions().isEmpty()) {
      jobConfiguration.setSchemaUpdateOptions(config.getLoadSchemaUpdateOptions());
    }

    Job finishedJob = bigQueryClient.createAndWaitFor(jobConfiguration);

    if (finishedJob.getStatus().getError() != null) {
      throw new BigQueryException(
          BaseHttpServiceException.UNKNOWN_CODE,
          String.format(
              "Failed to load to %s in job %s. BigQuery error was '%s'",
              BigQueryUtil.friendlyTableName(config.getTableId()),
              finishedJob.getJobId(),
              finishedJob.getStatus().getError().getMessage()),
          finishedJob.getStatus().getError());
    } else {
      logger.info(
          "Done loading to {}. jobId: {}",
          BigQueryUtil.friendlyTableName(config.getTableId()),
          finishedJob.getJobId());
    }
  }

  JobInfo.WriteDisposition saveModeToWriteDisposition(SaveMode saveMode) {
    if (saveMode == SaveMode.ErrorIfExists) {
      return JobInfo.WriteDisposition.WRITE_EMPTY;
    }
    // SaveMode.Ignore is handled in the data source level. If it has arrived here it means tha
    // table does not exist
    if (saveMode == SaveMode.Append || saveMode == SaveMode.Ignore) {
      return JobInfo.WriteDisposition.WRITE_APPEND;
    }
    if (saveMode == SaveMode.Overwrite) {
      return JobInfo.WriteDisposition.WRITE_TRUNCATE;
    }
    throw new UnsupportedOperationException(
        "SaveMode " + saveMode + " is currently not supported.");
  }

  void updateMetadataIfNeeded() {
    Map<String, StructField> fieldsToUpdate =
        Stream.of(sparkSchema.fields())
            .filter(
                field ->
                    SupportedCustomDataType.of(field.dataType()).isPresent()
                        || SchemaConverters.getDescriptionOrCommentOfField(field).isPresent())
            .collect(Collectors.toMap(StructField::name, Function.identity()));

    if (!fieldsToUpdate.isEmpty()) {
      logger.debug("updating schema, found fields to update: {}", fieldsToUpdate.keySet());
      TableInfo originalTableInfo = bigQueryClient.getTable(config.getTableIdWithoutThePartition());
      TableDefinition originalTableDefinition = originalTableInfo.getDefinition();
      Schema originalSchema = originalTableDefinition.getSchema();
      Schema updatedSchema =
          Schema.of(
              originalSchema.getFields().stream()
                  .map(
                      field ->
                          Optional.ofNullable(fieldsToUpdate.get(field.getName()))
                              .map(sparkSchemaField -> updatedField(field, sparkSchemaField))
                              .orElse(field))
                  .collect(Collectors.toList()));
      TableInfo.Builder updatedTableInfo =
          originalTableInfo
              .toBuilder()
              .setDefinition(originalTableDefinition.toBuilder().setSchema(updatedSchema).build());

      bigQueryClient.update(updatedTableInfo.build());
    }
  }

  Field updatedField(Field field, StructField sparkSchemaField) {
    Field.Builder newField = field.toBuilder();
    Optional<String> bqDescription =
        SchemaConverters.getDescriptionOrCommentOfField(sparkSchemaField);

    if (bqDescription.isPresent()) {
      newField.setDescription(bqDescription.get());
    } else {
      String description = field.getDescription();
      String marker = SupportedCustomDataType.of(sparkSchemaField.dataType()).get().getTypeMarker();

      if (description == null) {
        newField.setDescription(marker);
      } else if (!description.endsWith(marker)) {
        newField.setDescription(description + " " + marker);
      }
    }
    return newField.build();
  }

  void cleanTemporaryGcsPathIfNeeded() {
    intermediateDataCleaner.ifPresent(cleaner -> cleaner.deletePath());
  }
}
