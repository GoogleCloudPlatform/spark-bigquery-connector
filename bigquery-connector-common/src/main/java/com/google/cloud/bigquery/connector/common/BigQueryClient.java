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

import com.google.cloud.BaseServiceException;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobConfiguration;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.http.BaseHttpServiceException;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.google.cloud.bigquery.connector.common.BigQueryErrorCode.BIGQUERY_VIEW_DESTINATION_TABLE_CREATION_FAILED;
import static com.google.cloud.bigquery.connector.common.BigQueryErrorCode.UNSUPPORTED;
import static com.google.cloud.bigquery.connector.common.BigQueryUtil.convertToBigQueryException;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.UUID.randomUUID;
import static java.util.stream.Collectors.joining;

// holds caches and mappings
// presto converts the dataset and table names to lower case, while BigQuery is case sensitive
// the mappings here keep the mappings
public class BigQueryClient {
  private static final Logger log = LoggerFactory.getLogger(BigQueryClient.class);

  private static Cache<String, TableInfo> destinationTableCache =
      CacheBuilder.newBuilder().expireAfterWrite(15, TimeUnit.MINUTES).maximumSize(1000).build();

  private final BigQuery bigQuery;
  private final Optional<String> materializationProject;
  private final Optional<String> materializationDataset;

  BigQueryClient(
      BigQuery bigQuery,
      Optional<String> materializationProject,
      Optional<String> materializationDataset) {
    this.bigQuery = bigQuery;
    this.materializationProject = materializationProject;
    this.materializationDataset = materializationDataset;
  }

  // return empty if no filters are used
  private static Optional<String> createWhereClause(String[] filters) {
    if (filters.length == 0) {
      return Optional.empty();
    }
    return Optional.of(Stream.of(filters).collect(Collectors.joining(") AND (", "(", ")")));
  }

  public TableInfo getTable(TableId tableId) {
    return bigQuery.getTable(tableId);
  }

  public TableInfo getReadTable(ReadTableOptions options) {
    Optional<String> query = options.query();
    // first, let check if this is a query
    if (query.isPresent()) {
      // in this case, let's materialize it and use it as the table
      validateViewsEnabled(options);
      String sql = query.get();
      return materializeQueryToTable(sql, options.expirationTimeInMinutes());
    }

    TableInfo table = getTable(options.tableId());
    if (table == null) {
      return null;
    }

    TableDefinition tableDefinition = table.getDefinition();
    TableDefinition.Type tableType = tableDefinition.getType();
    if (TableDefinition.Type.TABLE == tableType) {
      return table;
    }
    if (TableDefinition.Type.VIEW == tableType
        || TableDefinition.Type.MATERIALIZED_VIEW == tableType) {
      validateViewsEnabled(options);
      // view materialization is done in a lazy manner, so it can occur only when the data is read
      return table;
    }
    // not regular table or a view
    throw new BigQueryConnectorException(
        UNSUPPORTED,
        format(
            "Table type '%s' of table '%s.%s' is not supported",
            tableType, table.getTableId().getDataset(), table.getTableId().getTable()));
  }

  private void validateViewsEnabled(ReadTableOptions options) {
    if (!options.viewsEnabled()) {
      throw new BigQueryConnectorException(
          UNSUPPORTED,
          format(
              "Views are not enabled. You can enable views by setting '%s' to true. Notice additional cost may occur.",
              options.viewEnabledParamName()));
    }
  }

  DatasetId toDatasetId(TableId tableId) {
    return DatasetId.of(tableId.getProject(), tableId.getDataset());
  }

  public String getProjectId() {
    return bigQuery.getOptions().getProjectId();
  }

  Iterable<Dataset> listDatasets(String projectId) {
    return bigQuery.listDatasets(projectId).iterateAll();
  }

  Iterable<Table> listTables(DatasetId datasetId, TableDefinition.Type... types) {
    Set<TableDefinition.Type> allowedTypes = ImmutableSet.copyOf(types);
    Iterable<Table> allTables = bigQuery.listTables(datasetId).iterateAll();
    return StreamSupport.stream(allTables.spliterator(), false)
        .filter(table -> allowedTypes.contains(table.getDefinition().getType()))
        .collect(toImmutableList());
  }

  TableId createDestinationTable(
      Optional<String> referenceProject, Optional<String> referenceDataset) {
    String project = materializationProject.orElse(referenceProject.orElse(null));
    String dataset = materializationDataset.orElse(referenceDataset.orElse(null));
    String name = format("_bqc_%s", randomUUID().toString().toLowerCase(ENGLISH).replace("-", ""));
    return project == null ? TableId.of(dataset, name) : TableId.of(project, dataset, name);
  }

  public Table update(TableInfo table) {
    return bigQuery.update(table);
  }

  public Job createAndWaitFor(JobConfiguration.Builder jobConfiguration) {
    return createAndWaitFor(jobConfiguration.build());
  }

  public Job createAndWaitFor(JobConfiguration jobConfiguration) {
    JobInfo jobInfo = JobInfo.of(jobConfiguration);
    Job job = bigQuery.create(jobInfo);

    log.info("Submitted job {}. jobId: {}", jobConfiguration, job.getJobId());
    // TODO(davidrab): add retry options
    try {
      return job.waitFor();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new BigQueryException(
          BaseHttpServiceException.UNKNOWN_CODE, format("Failed to run the job [%s]", job), e);
    }
  }

  Job create(JobInfo jobInfo) {
    return bigQuery.create(jobInfo);
  }

  TableResult query(String sql) {
    try {
      return bigQuery.query(QueryJobConfiguration.of(sql));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new BigQueryException(
          BaseHttpServiceException.UNKNOWN_CODE, format("Failed to run the query [%s]", sql), e);
    }
  }

  String createSql(TableId table, ImmutableList<String> requiredColumns, String[] filters) {
    String columns =
        requiredColumns.isEmpty()
            ? "*"
            : requiredColumns.stream().map(column -> format("`%s`", column)).collect(joining(","));

    return createSql(table, columns, filters);
  }

  // assuming the SELECT part is properly formatted, can be used to call functions such as COUNT and
  // SUM
  String createSql(TableId table, String formattedQuery, String[] filters) {
    String tableName = fullTableName(table);

    String whereClause = createWhereClause(filters).map(clause -> "WHERE " + clause).orElse("");

    return format("SELECT %s FROM `%s` %s", formattedQuery, tableName, whereClause);
  }

  String fullTableName(TableId tableId) {
    return format("%s.%s.%s", tableId.getProject(), tableId.getDataset(), tableId.getTable());
  }

  public long calculateTableSize(TableId tableId, Optional<String> filter) {
    return calculateTableSize(getTable(tableId), filter);
  }

  public long calculateTableSize(TableInfo tableInfo, Optional<String> filter) {
    try {
      TableDefinition.Type type = tableInfo.getDefinition().getType();
      if (type == TableDefinition.Type.TABLE && !filter.isPresent()) {
        return tableInfo.getNumRows().longValue();
      } else if (type == TableDefinition.Type.VIEW
          || type == TableDefinition.Type.MATERIALIZED_VIEW
          || (type == TableDefinition.Type.TABLE && filter.isPresent())) {
        // run a query
        String table = fullTableName(tableInfo.getTableId());
        String sql = format("SELECT COUNT(*) from `%s` WHERE %s", table, filter.get());
        TableResult result = bigQuery.query(QueryJobConfiguration.of(sql));
        return result.iterateAll().iterator().next().get(0).getLongValue();
      } else {
        throw new IllegalArgumentException(
            format(
                "Unsupported table type %s for table %s",
                type, fullTableName(tableInfo.getTableId())));
      }
    } catch (InterruptedException e) {
      throw new BigQueryConnectorException(
          "Querying table size was interrupted on the client side", e);
    }
  }

  /**
   * Runs the provided query on BigQuery and saves the result in a temporary table.
   *
   * @param querySql the query to be run
   * @param expirationTimeInMinutes the time in minutes until the table is expired and auto-deleted
   * @return a reference to the table
   */
  public TableInfo materializeQueryToTable(String querySql, int expirationTimeInMinutes) {
    TableId tableId = createDestinationTable(Optional.empty(), Optional.empty());
    return materializeTable(querySql, tableId, expirationTimeInMinutes);
  }

  /**
   * Runs the provided query on BigQuery and saves the result in a temporary table. This method is
   * intended to be used to materialize views, so the view location (based on its TableId) is taken
   * as a location for the temporary table, removing the need to set the materializationProject and
   * materializationDataset properties
   *
   * @param querySql the query to be run
   * @param viewId the view the query came from
   * @param expirationTimeInMinutes the time in hours until the table is expired and auto-deleted
   * @return a reference to the table
   */
  public TableInfo materializeViewToTable(
      String querySql, TableId viewId, int expirationTimeInMinutes) {
    TableId tableId =
        createDestinationTable(
            Optional.ofNullable(viewId.getProject()), Optional.ofNullable(viewId.getDataset()));
    return materializeTable(querySql, tableId, expirationTimeInMinutes);
  }

  private TableInfo materializeTable(
      String querySql, TableId destinationTableId, int expirationTimeInMinutes) {
    try {
      return destinationTableCache.get(
          querySql,
          new DestinationTableBuilder(this, querySql, destinationTableId, expirationTimeInMinutes));
    } catch (Exception e) {
      throw new BigQueryConnectorException(
          BIGQUERY_VIEW_DESTINATION_TABLE_CREATION_FAILED,
          String.format(
              "Error creating destination table using the following query: [%s]", querySql),
          e);
    }
  }

  public interface ReadTableOptions {
    TableId tableId();

    Optional<String> query();

    boolean viewsEnabled();

    String viewEnabledParamName();

    int expirationTimeInMinutes();
  }

  static class DestinationTableBuilder implements Callable<TableInfo> {
    final BigQueryClient bigQueryClient;
    final String querySql;
    final TableId destinationTable;
    final int expirationTimeInMinutes;

    DestinationTableBuilder(
        BigQueryClient bigQueryClient,
        String querySql,
        TableId destinationTable,
        int expirationTimeInMinutes) {
      this.bigQueryClient = bigQueryClient;
      this.querySql = querySql;
      this.destinationTable = destinationTable;
      this.expirationTimeInMinutes = expirationTimeInMinutes;
    }

    @Override
    public TableInfo call() {
      return createTableFromQuery();
    }

    TableInfo createTableFromQuery() {
      log.debug("destinationTable is %s", destinationTable);
      JobInfo jobInfo =
          JobInfo.of(
              QueryJobConfiguration.newBuilder(querySql)
                  .setDestinationTable(destinationTable)
                  .build());
      log.debug("running query %s", jobInfo);
      Job job = waitForJob(bigQueryClient.create(jobInfo));
      log.debug("job has finished. %s", job);
      if (job.getStatus().getError() != null) {
        throw convertToBigQueryException(job.getStatus().getError());
      }
      // add expiration time to the table
      TableInfo createdTable = bigQueryClient.getTable(destinationTable);
      long expirationTime =
          createdTable.getCreationTime() + TimeUnit.MINUTES.toMillis(expirationTimeInMinutes);
      Table updatedTable =
          bigQueryClient.update(createdTable.toBuilder().setExpirationTime(expirationTime).build());
      return updatedTable;
    }

    Job waitForJob(Job job) {
      try {
        return job.waitFor();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new BigQueryException(
            BaseServiceException.UNKNOWN_CODE,
            format("Job %s has been interrupted", job.getJobId()),
            e);
      }
    }
  }
}
