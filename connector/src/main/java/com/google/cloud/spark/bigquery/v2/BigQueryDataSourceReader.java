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

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.connector.common.*;
import com.google.cloud.bigquery.storage.v1.DataFormat;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.cloud.spark.bigquery.ReadRowsResponseToInternalRowIteratorConverter;
import com.google.cloud.spark.bigquery.SchemaConverters;
import com.google.cloud.spark.bigquery.SparkFilterUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.v2.reader.*;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConversions;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class BigQueryDataSourceReader
    implements DataSourceReader,
        SupportsPushDownRequiredColumns,
        SupportsPushDownFilters,
        SupportsReportStatistics,
        SupportsScanColumnarBatch {

  private static Logger logger = LoggerFactory.getLogger(BigQueryDataSourceReader.class);

  private final TableInfo table;
  private final TableId tableId;
  private final ReadSessionCreatorConfig readSessionCreatorConfig;
  private final BigQueryClient bigQueryClient;
  private final BigQueryReadClientFactory bigQueryReadClientFactory;
  private final ReadSessionCreator readSessionCreator;
  private final Optional<String> globalFilter;
  private Optional<StructType> schema;
  private Filter[] pushedFilters = new Filter[] {};
  private Map<String, StructField> fields;

  public BigQueryDataSourceReader(
      TableInfo table,
      BigQueryClient bigQueryClient,
      BigQueryReadClientFactory bigQueryReadClientFactory,
      ReadSessionCreatorConfig readSessionCreatorConfig,
      Optional<String> globalFilter,
      Optional<StructType> schema) {
    this.table = table;
    this.tableId = table.getTableId();
    this.readSessionCreatorConfig = readSessionCreatorConfig;
    this.bigQueryClient = bigQueryClient;
    this.bigQueryReadClientFactory = bigQueryReadClientFactory;
    this.readSessionCreator =
        new ReadSessionCreator(readSessionCreatorConfig, bigQueryClient, bigQueryReadClientFactory);
    this.globalFilter = globalFilter;
    this.schema = schema;
    this.fields =
        JavaConversions.asJavaCollection(
                SchemaConverters.toSpark(table.getDefinition().getSchema()))
            .stream()
            .collect(Collectors.toMap(field -> field.name(), Function.identity()));
  }

  @Override
  public StructType readSchema() {
    // TODO: rely on Java code
    return schema.orElse(SchemaConverters.toSpark(table.getDefinition().getSchema()));
  }

  @Override
  public boolean enableBatchRead() {
    return readSessionCreatorConfig.getReadDataFormat() == DataFormat.ARROW && !isEmptySchema();
  }

  @Override
  public List<InputPartition<InternalRow>> planInputPartitions() {
    logger.debug("planInputPartitions() for {}", tableId);
    if (isEmptySchema()) {
      // create empty projection
      return createEmptyProjectionPartitions();
    }

    ImmutableList<String> selectedFields = getSelectedFields();
    Optional<String> filter = getCompiledFilter();
    ReadSessionResponse readSessionResponse =
        readSessionCreator.create(
            tableId, selectedFields, filter, readSessionCreatorConfig.getMaxParallelism());
    ReadSession readSession = readSessionResponse.getReadSession();
    return readSession.getStreamsList().stream()
        .map(
            stream ->
                new BigQueryInputPartition(
                    bigQueryReadClientFactory,
                    stream.getName(),
                    readSessionCreatorConfig.getMaxReadRowsRetries(),
                    createConverter(selectedFields, readSessionResponse)))
        .collect(Collectors.toList());
  }

  @Override
  public List<InputPartition<ColumnarBatch>> planBatchInputPartitions() {
    logger.debug("planBatchInputPartitions() for {}", tableId);
    if (!enableBatchRead()) {
      throw new IllegalStateException("Batch reads should not be enabled");
    }
    ImmutableList<String> selectedFields = getSelectedFields();
    Optional<String> filter = getCompiledFilter();
    ReadSessionResponse readSessionResponse =
        readSessionCreator.create(
            tableId, selectedFields, filter, readSessionCreatorConfig.getMaxParallelism());
    ReadSession readSession = readSessionResponse.getReadSession();

    if (selectedFields.isEmpty()) {
      // means select *
      Schema tableSchema = readSessionResponse.getReadTableInfo().getDefinition().getSchema();
      selectedFields =
          tableSchema.getFields().stream()
              .map(Field::getName)
              .collect(ImmutableList.toImmutableList());
    }

    ImmutableList<String> partitionSelectedFields = selectedFields;
    return readSession.getStreamsList().stream()
        .map(
            stream ->
                new ArrowInputPartition(
                    bigQueryReadClientFactory,
                    stream.getName(),
                    readSessionCreatorConfig.getMaxReadRowsRetries(),
                    partitionSelectedFields,
                    readSessionResponse))
        .collect(Collectors.toList());
  }

  private Optional<String> getCompiledFilter() {
    return emptyIfNeeded(
        SparkFilterUtils.getCompiledFilter(
            readSessionCreatorConfig.getReadDataFormat(), globalFilter, pushedFilters));
  }

  private ImmutableList<String> getSelectedFields() {
    return schema
        .map(requiredSchema -> ImmutableList.copyOf(requiredSchema.fieldNames()))
        .orElse(ImmutableList.of());
  }

  private boolean isEmptySchema() {
    return schema.map(StructType::isEmpty).orElse(false);
  }

  private ReadRowsResponseToInternalRowIteratorConverter createConverter(
      ImmutableList<String> selectedFields, ReadSessionResponse readSessionResponse) {
    ReadRowsResponseToInternalRowIteratorConverter converter;
    DataFormat format = readSessionCreatorConfig.getReadDataFormat();
    if (format == DataFormat.AVRO) {
      Schema schema = readSessionResponse.getReadTableInfo().getDefinition().getSchema();
      if (selectedFields.isEmpty()) {
        // means select *
        selectedFields =
            schema.getFields().stream()
                .map(Field::getName)
                .collect(ImmutableList.toImmutableList());
      } else {
        Set<String> requiredColumnSet = ImmutableSet.copyOf(selectedFields);
        schema =
            Schema.of(
                schema.getFields().stream()
                    .filter(field -> requiredColumnSet.contains(field.getName()))
                    .collect(Collectors.toList()));
      }
      return ReadRowsResponseToInternalRowIteratorConverter.avro(
          schema, selectedFields, readSessionResponse.getReadSession().getAvroSchema().getSchema());
    }
    throw new IllegalArgumentException(
        "No known converted for " + readSessionCreatorConfig.getReadDataFormat());
  }

  List<InputPartition<InternalRow>> createEmptyProjectionPartitions() {
    long rowCount =
        bigQueryClient.calculateTableSize(tableId, globalFilter).getQueriedNumberOfRows();
    int partitionsCount = readSessionCreatorConfig.getDefaultParallelism();
    int partitionSize = (int) (rowCount / partitionsCount);
    InputPartition<InternalRow>[] partitions =
        IntStream.range(0, partitionsCount)
            .mapToObj(ignored -> new BigQueryEmptyProjectionInputPartition(partitionSize))
            .toArray(BigQueryEmptyProjectionInputPartition[]::new);
    int firstPartitionSize = partitionSize + (int) (rowCount % partitionsCount);
    partitions[0] = new BigQueryEmptyProjectionInputPartition(firstPartitionSize);
    return ImmutableList.copyOf(partitions);
  }

  @Override
  public Filter[] pushFilters(Filter[] filters) {
    List<Filter> handledFilters = new ArrayList<>();
    List<Filter> unhandledFilters = new ArrayList<>();
    for (Filter filter : filters) {
      if (SparkFilterUtils.isTopLevelFieldHandled(
          filter, readSessionCreatorConfig.getReadDataFormat(), fields)) {
        handledFilters.add(filter);
      } else {
        unhandledFilters.add(filter);
      }
    }
    pushedFilters = handledFilters.stream().toArray(Filter[]::new);
    return unhandledFilters.stream().toArray(Filter[]::new);
  }

  @Override
  public Filter[] pushedFilters() {
    return pushedFilters;
  }

  @Override
  public void pruneColumns(StructType requiredSchema) {
    this.schema = Optional.ofNullable(requiredSchema);
  }

  Optional<String> emptyIfNeeded(String value) {
    return (value == null || value.length() == 0) ? Optional.empty() : Optional.of(value);
  }

  @Override
  public Statistics estimateStatistics() {
    logger.debug("estimateStatistics() for {}", tableId);
    Optional<String> filter = getCompiledFilter();
    Optional<ImmutableList<String>> selectFields = schema.map(ignored -> getSelectedFields());
    BigQueryEstimatedTableStatistics statistics =
        BigQueryEstimatedTableStatistics.newFactory(table, bigQueryClient)
            .setFilter(filter)
            .setSelectedColumns(selectFields)
            .create();
    return new BigQueryEstimatedTableStatisticsSparkAdapter(statistics);
  }
}
