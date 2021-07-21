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

import com.google.cloud.bigquery.connector.common.ArrowUtil;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import org.apache.arrow.compression.CommonsCompressionFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.SequenceInputStream;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class ArrowBinaryIterator implements Iterator<InternalRow> {

  private static long maxAllocation = Long.MAX_VALUE;
  ArrowReaderIterator arrowReaderIterator;
  Iterator<InternalRow> currentIterator;
  List<String> columnsInOrder;
  Map<String, StructField> userProvidedFieldMap;

  public ArrowBinaryIterator(
      List<String> columnsInOrder,
      ByteString schema,
      ByteString rowsInBytes,
      Optional<StructType> userProvidedSchema) {
    BufferAllocator allocator =
        ArrowUtil.newRootAllocator(maxAllocation)
            .newChildAllocator("ArrowBinaryIterator", 0, maxAllocation);

    SequenceInputStream bytesWithSchemaStream =
        new SequenceInputStream(
            new ByteArrayInputStream(schema.toByteArray()),
            new ByteArrayInputStream(rowsInBytes.toByteArray()));

    ArrowStreamReader arrowStreamReader =
        new ArrowStreamReader(bytesWithSchemaStream, allocator, CommonsCompressionFactory.INSTANCE);
    arrowReaderIterator = new ArrowReaderIterator(arrowStreamReader);
    currentIterator = ImmutableList.<InternalRow>of().iterator();
    this.columnsInOrder = columnsInOrder;

    List<StructField> userProvidedFieldList =
        Arrays.stream(userProvidedSchema.orElse(new StructType()).fields())
            .collect(Collectors.toList());

    this.userProvidedFieldMap =
        userProvidedFieldList.stream()
            .collect(Collectors.toMap(StructField::name, Function.identity()));
  }

  @Override
  public boolean hasNext() {
    while (!currentIterator.hasNext()) {
      if (!arrowReaderIterator.hasNext()) {
        return false;
      }
      currentIterator = toArrowRows(arrowReaderIterator.next(), columnsInOrder);
    }

    return currentIterator.hasNext();
  }

  @Override
  public InternalRow next() {
    return currentIterator.next();
  }

  private Iterator<InternalRow> toArrowRows(VectorSchemaRoot root, List<String> namesInOrder) {
    ColumnVector[] columns =
        namesInOrder.stream()
            .map(name -> root.getVector(name))
            .map(
                vector ->
                    new ArrowSchemaConverter(vector, userProvidedFieldMap.get(vector.getName())))
            .collect(Collectors.toList())
            .toArray(new ColumnVector[0]);

    ColumnarBatch batch = new ColumnarBatch(columns);
    batch.setNumRows(root.getRowCount());
    return batch.rowIterator();
  }
}

class ArrowReaderIterator implements Iterator<VectorSchemaRoot> {

  private static final Logger log = LoggerFactory.getLogger(AvroBinaryIterator.class);
  boolean closed = false;
  VectorSchemaRoot current = null;
  ArrowReader reader;

  public ArrowReaderIterator(ArrowReader reader) {
    this.reader = reader;
  }

  @Override
  public boolean hasNext() {
    if (current != null) {
      return true;
    }

    if (closed) {
      return false;
    }

    try {
      boolean res = reader.loadNextBatch();
      if (res) {
        current = reader.getVectorSchemaRoot();
      } else {
        ensureClosed();
      }
      return res;
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to load the next arrow batch", e);
    }
  }

  @Override
  public VectorSchemaRoot next() {
    VectorSchemaRoot res = current;
    current = null;
    return res;
  }

  private void ensureClosed() throws IOException {
    if (!closed) {
      reader.close();
      closed = true;
    }
  }
}
