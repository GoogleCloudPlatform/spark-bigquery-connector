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

import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.bigquery.*;
import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.bigquery.connector.common.BigQueryConnectorException;
import com.google.cloud.bigquery.connector.common.BigQueryWriteClientFactory;
import com.google.cloud.bigquery.storage.v1alpha2.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1alpha2.ProtoBufProto;
import com.google.cloud.bigquery.storage.v1alpha2.Storage;
import com.google.common.base.Preconditions;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter;
import org.apache.spark.sql.sources.v2.writer.DataWriterFactory;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

import static com.google.cloud.spark.bigquery.ProtobufUtils.toProtoSchema;
import static com.google.cloud.spark.bigquery.SchemaConverters.toBigQuerySchema;

public class BigQueryDataSourceWriter implements DataSourceWriter {

  final Logger logger = LoggerFactory.getLogger(BigQueryDataSourceWriter.class);

  private final BigQueryClient bigQueryClient;
  private final BigQueryWriteClientFactory writeClientFactory;
  private final TableId destinationTableId;
  private final StructType sparkSchema;
  private final ProtoBufProto.ProtoSchema protoSchema;
  private final String writeUUID;
  private final RetrySettings bigqueryDataWriterHelperRetrySettings;

  private final TableId temporaryTableId;
  private final String tablePathForBigQueryStorage;

  private BigQueryWriteClient writeClient;

  enum WritingMode {
    IGNORE_INPUTS,
    OVERWRITE,
    ALL_ELSE
  }

  private WritingMode writingMode = WritingMode.ALL_ELSE;

  public BigQueryDataSourceWriter(
      BigQueryClient bigQueryClient,
      BigQueryWriteClientFactory bigQueryWriteClientFactory,
      TableId destinationTableId,
      String writeUUID,
      SaveMode saveMode,
      StructType sparkSchema,
      RetrySettings bigqueryDataWriterHelperRetrySettings) {
    this.bigQueryClient = bigQueryClient;
    this.writeClientFactory = bigQueryWriteClientFactory;
    this.destinationTableId = destinationTableId;
    this.writeUUID = writeUUID;
    this.sparkSchema = sparkSchema;
    this.bigqueryDataWriterHelperRetrySettings = bigqueryDataWriterHelperRetrySettings;
    Schema bigQuerySchema = toBigQuerySchema(sparkSchema);
    try {
      this.protoSchema = toProtoSchema(sparkSchema);
    } catch (IllegalArgumentException e) {
      throw new InvalidSchemaException("Could not convert Spark schema to protobuf descriptor", e);
    }

    this.temporaryTableId = getOrCreateTable(saveMode, destinationTableId, bigQuerySchema);
    this.tablePathForBigQueryStorage =
        bigQueryClient.createTablePathForBigQueryStorage(temporaryTableId);

    if (!writingMode.equals(WritingMode.IGNORE_INPUTS)) {
      this.writeClient = writeClientFactory.createBigQueryWriteClient();
    }
  }

  /**
   * This function determines whether the destination table exists: if it doesn't, Spark will do the
   * writing directly to it; otherwise, if SaveMode = SaveMode.OVERWRITE then a temporary table will
   * be created, where the writing results will be stored before replacing the destination table
   * upon commit; this function also validates the destination table's schema matches the expected
   * schema, if applicable.
   *
   * @param saveMode the SaveMode supplied by the user.
   * @param destinationTableId the TableId, as was supplied by the user
   * @param bigQuerySchema the bigQuery schema
   * @return The TableId to which Spark will do the writing: whether that is the destinationTableID
   *     or the temporaryTableId.
   */
  private TableId getOrCreateTable(
      SaveMode saveMode, TableId destinationTableId, Schema bigQuerySchema) {
    if (bigQueryClient.tableExists(destinationTableId)) {
      Preconditions.checkArgument(
          bigQueryClient
              .getTable(destinationTableId)
              .getDefinition()
              .getSchema()
              .equals(bigQuerySchema),
          new SchemaValidationException(
              "Destination table's schema is not compatible with dataframe's schema"));
      switch (saveMode) {
        case Append:
          break;
        case Overwrite:
          writingMode = WritingMode.OVERWRITE;
          return bigQueryClient.createTempTable(destinationTableId, bigQuerySchema).getTableId();
        case Ignore:
          writingMode = WritingMode.IGNORE_INPUTS;
          break;
        case ErrorIfExists:
          throw new BigQueryConnectorException("Table already exists in BigQuery");
      }
      return bigQueryClient.getTable(destinationTableId).getTableId();
    } else {
      return bigQueryClient.createTable(destinationTableId, bigQuerySchema).getTableId();
    }
  }

  @Override
  public DataWriterFactory<InternalRow> createWriterFactory() {
    return new BigQueryDataWriterFactory(
        writeClientFactory,
        tablePathForBigQueryStorage,
        sparkSchema,
        protoSchema,
        writingMode.equals(WritingMode.IGNORE_INPUTS),
        bigqueryDataWriterHelperRetrySettings);
  }

  @Override
  public void onDataWriterCommit(WriterCommitMessage message) {}

  /**
   * This function will determine, based on the WritingMode: if in IGNORE_INPUTS mode, no work is to
   * be done; otherwise all streams will be batch committed using the BigQuery Storage Write API,
   * and then: if in OVERWRITE mode, the overwriteDestinationWithTemporary function from
   * BigQueryClient will be called to replace the destination table with all the data from the
   * temporary table; if in ALL_ELSE mode no more work needs to be done.
   *
   * @see WritingMode
   * @see BigQueryClient#overwriteDestinationWithTemporary(TableId temporaryTableId, TableId
   *     destinationTableId)
   * @param messages the BigQueryWriterCommitMessage array returned by the BigQueryDataWriter's.
   */
  @Override
  public void commit(WriterCommitMessage[] messages) {
    if (writingMode.equals(WritingMode.IGNORE_INPUTS)) return;
    logger.info(
        "BigQuery DataSource writer {} committed with messages:\n{}",
        writeUUID,
        Arrays.toString(messages));

    Storage.BatchCommitWriteStreamsRequest.Builder batchCommitWriteStreamsRequest =
        Storage.BatchCommitWriteStreamsRequest.newBuilder().setParent(tablePathForBigQueryStorage);
    for (WriterCommitMessage message : messages) {
      batchCommitWriteStreamsRequest.addWriteStreams(
          ((BigQueryWriterCommitMessage) message).getWriteStreamName());
    }
    Storage.BatchCommitWriteStreamsResponse batchCommitWriteStreamsResponse =
        writeClient.batchCommitWriteStreams(batchCommitWriteStreamsRequest.build());

    if (!batchCommitWriteStreamsResponse.hasCommitTime()) {
      throw new BigQueryConnectorException(
          "DataSource writer failed to batch commit its BigQuery write-streams");
    }

    logger.info(
        "BigQuery DataSource writer has committed at time: {}",
        batchCommitWriteStreamsResponse.getCommitTime());

    if (writingMode.equals(WritingMode.OVERWRITE)) {
      Job overwriteJob =
          bigQueryClient.overwriteDestinationWithTemporary(temporaryTableId, destinationTableId);
      bigQueryClient.waitForJob(overwriteJob);
      Preconditions.checkState(
          bigQueryClient.deleteTable(temporaryTableId),
          new BigQueryConnectorException(
              String.format(
                  "Could not delete temporary table %s from BigQuery", temporaryTableId)));
    }

    writeClient.shutdown();
  }

  /**
   * If not in WritingMode IGNORE_INPUTS, the BigQuery Storage Write API WriteClient is shut down.
   *
   * @see BigQueryWriteClient
   * @param messages the BigQueryWriterCommitMessage array returned by the BigQueryDataWriter's.
   */
  @Override
  public void abort(WriterCommitMessage[] messages) {
    logger.warn("BigQuery Data Source writer {} aborted", writeUUID);
    if (writingMode.equals(WritingMode.IGNORE_INPUTS)) return;
    if (writeClient != null && !writeClient.isShutdown()) {
      writeClient.shutdown();
    }
    // Deletes the preliminary table we wrote to (if it exists):
    if (bigQueryClient.tableExists(temporaryTableId)) {
      bigQueryClient.deleteTable(temporaryTableId);
    }
  }

  protected static class InvalidSchemaException extends BigQueryConnectorException {
    InvalidSchemaException(String message) {
      super(message);
    }

    InvalidSchemaException(String message, Throwable t) {
      super(message, t);
    }
  }

  protected static class SchemaValidationException extends InvalidSchemaException {
    SchemaValidationException(String message) {
      super(message);
    }

    SchemaValidationException(String message, Throwable t) {
      super(message, t);
    }
  }
}
