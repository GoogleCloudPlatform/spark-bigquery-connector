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

import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import com.google.cloud.bigquery.storage.v1.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.cloud.bigquery.storage.v1.ReadSession.TableReadOptions;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Stream;

import static com.google.cloud.bigquery.connector.common.BigQueryErrorCode.UNSUPPORTED;
import static java.lang.String.format;

// A helper class, also handles view materialization
public class ReadSessionCreator {
  /**
   * Default parallelism to 1 reader per 400MB, which should be about the maximum allowed by the
   * BigQuery Storage API. The number of partitions returned may be significantly less depending on
   * a number of factors.
   */
  private static final int DEFAULT_BYTES_PER_PARTITION = 400 * 1000 * 1000;

  private static final Logger log = LoggerFactory.getLogger(ReadSessionCreator.class);

  private final ReadSessionCreatorConfig config;
  private final BigQueryClient bigQueryClient;
  private final BigQueryReadClientFactory bigQueryReadClientFactory;

  public ReadSessionCreator(
      ReadSessionCreatorConfig config,
      BigQueryClient bigQueryClient,
      BigQueryReadClientFactory bigQueryReadClientFactory) {
    this.config = config;
    this.bigQueryClient = bigQueryClient;
    this.bigQueryReadClientFactory = bigQueryReadClientFactory;
  }

  static int getMaxNumPartitionsRequested(
      OptionalInt maxParallelism, StandardTableDefinition tableDefinition) {
    return maxParallelism.orElse(
        Math.max((int) (tableDefinition.getNumBytes() / DEFAULT_BYTES_PER_PARTITION), 1));
  }

  /**
   * Creates a new ReadSession for parallel reads.
   *
   * <p>Some attributes are governed by the {@link ReadSessionCreatorConfig} that this object was
   * constructed with.
   *
   * @param table The table to create the session for.
   * @param selectedFields
   * @param filter
   * @return
   */
  public ReadSessionResponse create(
      TableId table, ImmutableList<String> selectedFields, Optional<String> filter) {
    TableInfo tableDetails = bigQueryClient.getTable(table);

    TableInfo actualTable = getActualTable(tableDetails, selectedFields, filter);
    StandardTableDefinition tableDefinition = actualTable.getDefinition();

    try (BigQueryReadClient bigQueryReadClient =
        bigQueryReadClientFactory.createBigQueryReadClient(config.endpoint())) {

      String tablePath = toTablePath(actualTable.getTableId());
      CreateReadSessionRequest request =
          config
              .getRequestEncodedBase()
              .map(
                  value -> {
                    try {
                      return com.google.cloud.bigquery.storage.v1.CreateReadSessionRequest
                          .parseFrom(java.util.Base64.getDecoder().decode(value));
                    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
                      throw new RuntimeException("Couldn't decode:" + value, e);
                    }
                  })
              .orElse(CreateReadSessionRequest.newBuilder().build());
      ReadSession.Builder requestedSession = request.getReadSession().toBuilder();
      TableReadOptions.Builder readOptions = requestedSession.getReadOptionsBuilder();
      if (!isInputTableAView(tableDetails)) {
        filter.ifPresent(readOptions::setRowRestriction);
      }
      readOptions.addAllSelectedFields(selectedFields);

      ReadSession readSession =
          bigQueryReadClient.createReadSession(
              request
                  .newBuilder()
                  .setParent("projects/" + bigQueryClient.getProjectId())
                  .setReadSession(
                      requestedSession
                          .setDataFormat(config.getReadDataFormat())
                          .setReadOptions(readOptions)
                          .setTable(tablePath)
                          .build())
                  .setMaxStreamCount(
                      getMaxNumPartitionsRequested(config.getMaxParallelism(), tableDefinition))
                  .build());

      return new ReadSessionResponse(readSession, actualTable);
    }
  }

  String toTablePath(TableId tableId) {
    return format(
        "projects/%s/datasets/%s/tables/%s",
        tableId.getProject(), tableId.getDataset(), tableId.getTable());
  }

  TableInfo getActualTable(
      TableInfo table, ImmutableList<String> requiredColumns, Optional<String> filter) {
    String[] filters = filter.map(Stream::of).orElseGet(Stream::empty).toArray(String[]::new);
    return getActualTable(table, requiredColumns, filters);
  }

  TableInfo getActualTable(
      TableInfo table, ImmutableList<String> requiredColumns, String[] filters) {
    TableDefinition tableDefinition = table.getDefinition();
    TableDefinition.Type tableType = tableDefinition.getType();
    if (TableDefinition.Type.TABLE == tableType) {
      return table;
    }
    if (isInputTableAView(table)) {
      // get it from the view
      String querySql = bigQueryClient.createSql(table.getTableId(), requiredColumns, filters);
      log.debug("querySql is %s", querySql);
      return bigQueryClient.materializeViewToTable(
          querySql, table.getTableId(), config.getMaterializationExpirationTimeInMinutes());
    } else {
      // not regular table or a view
      throw new BigQueryConnectorException(
          UNSUPPORTED,
          format(
              "Table type '%s' of table '%s.%s' is not supported",
              tableType, table.getTableId().getDataset(), table.getTableId().getTable()));
    }
  }

  private boolean isInputTableAView(TableInfo table) {
    TableDefinition tableDefinition = table.getDefinition();
    TableDefinition.Type tableType = tableDefinition.getType();

    if (TableDefinition.Type.VIEW == tableType
        || TableDefinition.Type.MATERIALIZED_VIEW == tableType) {
      if (!config.isViewsEnabled()) {
        throw new BigQueryConnectorException(
            UNSUPPORTED,
            format(
                "Views are not enabled. You can enable views by setting '%s' to true. Notice additional cost may occur.",
                config.getViewEnabledParamName()));
      }
      return true;
    }
    return false;
  }
}
