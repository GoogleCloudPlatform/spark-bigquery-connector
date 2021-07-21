/*
 * Copyright 2021 Google Inc. All Rights Reserved.
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

import static com.google.common.truth.Truth.assertThat;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.MoreExecutors;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;

public class ParallelArrowReaderTest {

  BufferAllocator allocator;

  @Before
  public void initializeAllocator() {
    allocator = ArrowUtil.newRootAllocator(Integer.MAX_VALUE);
  }

  @After
  public void closeAllocator() {
    allocator.close();
  }

  ArrowReader getReaderWithSequence(int... values) throws IOException {
    IntVector vector = new IntVector("vector_name", allocator);
    try (VectorSchemaRoot root = VectorSchemaRoot.of(vector)) {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      ArrowStreamWriter writer = new ArrowStreamWriter(root, /*dictionaryProvider=*/ null, baos);
      for (int x = 0; x < values.length; x++) {
        vector.allocateNew(1);
        vector.set(/*index=*/ 0, values[x]);
        vector.setValueCount(1);
        root.setRowCount(1);
        writer.writeBatch();
      }
      writer.close();
      return new ArrowStreamReader(
          new NonInterruptibleBlockingBytesChannel(new ByteArrayInputStream(baos.toByteArray())),
          allocator);
    }
  }

//  @Test
//  public void testReadsAllBatchesInRoundRobin() throws Exception {
//    ArrowReader r1 = getReaderWithSequence(0);
//    ArrowReader r2 = getReaderWithSequence(1, 3);
//    ArrowReader r3 = getReaderWithSequence(2, 4, 5);
//    ExecutorService executor = Executors.newFixedThreadPool(3);
//    List<Integer> read = new ArrayList<>();
//    try (VectorSchemaRoot root =
//        VectorSchemaRoot.create(r1.getVectorSchemaRoot().getSchema(), allocator)) {
//      VectorLoader loader = new VectorLoader(root);
//      ParallelArrowReader reader =
//          new ParallelArrowReader(
//              ImmutableList.of(r1, r2, r3),
//              executor,
//              loader,
//              new LoggingBigQueryStorageReadRowsTracer("stream_name", 2));
//
//      while (reader.next()) {
//        read.add(((IntVector) root.getVector(0)).get(0));
//      }
//      reader.close();
//    }
//
//    assertThat(read).containsExactlyElementsIn(ImmutableList.of(0, 1, 2, 3, 4, 5)).inOrder();
//    assertThat(executor.isShutdown()).isTrue();
//  }

//  @Test
//  public void testReadsAllBatchesInRoundRobinOneelement() throws Exception {
//
//    ColumnarBatch[] batches = new ColumnarBatch[6];
//    for (int x = 0; x < batches.length; x++) {
//      batches[x] = new ColumnarBatch(new ColumnVector[0]);
//    }
//    ArrowReader r1 = getReaderWithSequence();
//    ArrowReader r2 = getReaderWithSequence(0);
//    ArrowReader r3 = getReaderWithSequence();
//    ExecutorService executor = Executors.newFixedThreadPool(3);
//    List<Integer> read = new ArrayList<>();
//    try (VectorSchemaRoot root =
//        VectorSchemaRoot.create(r1.getVectorSchemaRoot().getSchema(), allocator)) {
//      VectorLoader loader = new VectorLoader(root);
//      ParallelArrowReader reader =
//          new ParallelArrowReader(
//              ImmutableList.of(r1, r2, r3),
//              executor,
//              loader,
//              new LoggingBigQueryStorageReadRowsTracer("stream_name", 2));
//
//      while (reader.next()) {
//        read.add(((IntVector) root.getVector(0)).get(0));
//      }
//      reader.close();
//    }
//
//    assertThat(read).containsExactlyElementsIn(ImmutableList.of(0)).inOrder();
//    assertThat(executor.isShutdown()).isTrue();
//  }

  @Test
  public void testExceptionIsPropagatedFromNext() throws Exception {

    IOException exception = new IOException("an exception");
    ArrowReader r1 = mock(ArrowReader.class);

    when(r1.loadNextBatch()).thenThrow(exception);

    ExecutorService executor = MoreExecutors.newDirectExecutorService();
    try (VectorSchemaRoot root = new VectorSchemaRoot(ImmutableList.of());
        VectorSchemaRoot root2 = new VectorSchemaRoot(ImmutableList.of())) {
      when(r1.getVectorSchemaRoot()).thenReturn(root2);
      ParallelArrowReader reader =
          new ParallelArrowReader(
              ImmutableList.of(r1),
              executor,
              new VectorLoader(root),
              new LoggingBigQueryStorageReadRowsTracer("stream_name", 2));
      IOException e = Assert.assertThrows(IOException.class, reader::next);
      assertThat(e).isSameInstanceAs(exception);
    }
  }

  @Test
  public void testInterruptsOnClose() throws Exception {
    try (VectorSchemaRoot root = VectorSchemaRoot.of()) {
      ArrowReader r1 = mock(ArrowReader.class);
      when(r1.loadNextBatch()).thenReturn(true);
      when(r1.getVectorSchemaRoot()).thenReturn(root);
      CountDownLatch latch = new CountDownLatch(1);
      ArrowReader r2 = mock(ArrowReader.class);
      when(r2.loadNextBatch())
          .thenAnswer(
              (InvocationOnMock invocation) -> {
                latch.countDown();
                MILLISECONDS.sleep(100);
                return true;
              });
      when(r2.getVectorSchemaRoot()).thenReturn(root);
      VectorLoader loader = mock(VectorLoader.class);

      ExecutorService executor = Executors.newSingleThreadExecutor();
      ParallelArrowReader reader =
          new ParallelArrowReader(
              ImmutableList.of(r1, r2),
              executor,
              loader,
              new LoggingBigQueryStorageReadRowsTracer("stream_name", 2));

      ExecutorService oneOff = Executors.newSingleThreadExecutor();
      Instant start = Instant.now();

      Future<Instant> endTime =
          oneOff.submit(
              () -> {
                try {
                  while (reader.next()) {}
                } catch (Exception e) {
                  if (e.getCause() == null || !(e.getCause() instanceof InterruptedException)) {
                    return Instant.ofEpochMilli(0);
                  }
                }
                return Instant.now();
              });

      // Wait until next gets called.
      latch.await();
      // Should interrupt blocking operations.
      oneOff.shutdownNow();
      reader.close();

      assertThat(endTime.get()).isGreaterThan(start);
      assertThat(Duration.between(start, endTime.get())).isLessThan(Duration.ofMillis(100));
    }
  }
}
