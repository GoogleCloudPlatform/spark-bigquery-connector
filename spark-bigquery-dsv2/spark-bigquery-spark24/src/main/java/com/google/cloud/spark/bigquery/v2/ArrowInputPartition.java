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

import com.google.cloud.bigquery.connector.common.BigQueryReadClientFactory;
import com.google.cloud.bigquery.connector.common.BigQueryStorageReadRowsTracer;
import com.google.cloud.bigquery.connector.common.BigQueryTracerFactory;
import com.google.cloud.bigquery.connector.common.ReadRowsHelper;
import com.google.cloud.bigquery.connector.common.ReadSessionResponse;
import com.google.cloud.bigquery.storage.v1.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import static com.google.common.base.Optional.fromJavaUtil;

import java.util.Iterator;

public class ArrowInputPartition implements InputPartition<ColumnarBatch> {

  private final BigQueryReadClientFactory bigQueryReadClientFactory;
  private final BigQueryTracerFactory tracerFactory;
  private final List<String> streamNames;
  private final ReadRowsHelper.Options options;
  private final ImmutableList<String> selectedFields;
  private final ByteString serializedArrowSchema;
  private final com.google.common.base.Optional<StructType> userProvidedSchema;

  public ArrowInputPartition(
      BigQueryReadClientFactory bigQueryReadClientFactory,
      BigQueryTracerFactory tracerFactory,
      List<String> names,
      ReadRowsHelper.Options options,
      ImmutableList<String> selectedFields,
      ReadSessionResponse readSessionResponse,
      Optional<StructType> userProvidedSchema) {
    this.bigQueryReadClientFactory = bigQueryReadClientFactory;
    this.streamNames = names;
    this.options = options;
    this.selectedFields = selectedFields;
    this.serializedArrowSchema =
        readSessionResponse.getReadSession().getArrowSchema().getSerializedSchema();
    this.tracerFactory = tracerFactory;
    this.userProvidedSchema = fromJavaUtil(userProvidedSchema);
  }

  @Override
  public InputPartitionReader<ColumnarBatch> createPartitionReader() {
    BigQueryStorageReadRowsTracer tracer =
        tracerFactory.newReadRowsTracer(Joiner.on(",").join(streamNames));
    List<ReadRowsRequest.Builder> readRowsRequests =
        streamNames.stream()
            .map(name -> ReadRowsRequest.newBuilder().setReadStream(name))
            .collect(Collectors.toList());

    ReadRowsHelper readRowsHelper =
        new ReadRowsHelper(bigQueryReadClientFactory, readRowsRequests, options);
    tracer.startStream();
    Iterator<ReadRowsResponse> readRowsResponses = readRowsHelper.readRows();
    return new ArrowColumnBatchPartitionColumnBatchReader(
        readRowsResponses,
        serializedArrowSchema,
        readRowsHelper,
        selectedFields,
        tracer,
        userProvidedSchema.toJavaUtil(),
        options.numBackgroundThreads());
  }
}
