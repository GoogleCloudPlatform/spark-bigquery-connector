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
import com.google.cloud.bigquery.connector.common.BigQueryWriteClientFactory;
import com.google.cloud.bigquery.storage.v1alpha2.ProtoBufProto;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.writer.DataWriter;
import org.apache.spark.sql.sources.v2.writer.DataWriterFactory;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BigQueryDataWriterFactory implements DataWriterFactory<InternalRow> {

  final Logger logger = LoggerFactory.getLogger(BigQueryDataWriterFactory.class);

  private final BigQueryWriteClientFactory writeClientFactory;
  private final String tablePath;
  private final StructType sparkSchema;
  private final ProtoBufProto.ProtoSchema protoSchema;
  private final boolean ignoreInputs;
  private final RetrySettings bigqueryDataWriterHelperRetrySettings;

  public BigQueryDataWriterFactory(
      BigQueryWriteClientFactory writeClientFactory,
      String tablePath,
      StructType sparkSchema,
      ProtoBufProto.ProtoSchema protoSchema,
      boolean ignoreInputs,
      RetrySettings bigqueryDataWriterHelperRetrySettings) {
    this.writeClientFactory = writeClientFactory;
    this.tablePath = tablePath;
    this.sparkSchema = sparkSchema;
    this.protoSchema = protoSchema;
    this.ignoreInputs = ignoreInputs;
    this.bigqueryDataWriterHelperRetrySettings = bigqueryDataWriterHelperRetrySettings;
  }

  /**
   * If ignoreInputs is true, return a NoOpDataWriter, a stub class that performs no operations upon
   * the call of its methods; otherwise return BigQueryDataWriter.
   *
   * @see NoOpDataWriter
   * @see BigQueryDataWriter
   * @param partitionId The partitionId of the DataWriter to be created
   * @param taskId the taskId
   * @param epochId the epochId
   * @return The DataWriter to be used.
   */
  @Override
  public DataWriter<InternalRow> createDataWriter(int partitionId, long taskId, long epochId) {
    if (ignoreInputs) {
      return new NoOpDataWriter();
    }
    return new BigQueryDataWriter(
        partitionId,
        taskId,
        epochId,
        writeClientFactory,
        tablePath,
        sparkSchema,
        protoSchema,
        bigqueryDataWriterHelperRetrySettings);
  }
}
