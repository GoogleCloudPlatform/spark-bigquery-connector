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

import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import com.google.cloud.bigquery.storage.v1.BigQueryReadGrpc.BigQueryReadImplBase;
import com.google.cloud.bigquery.storage.v1.BigQueryReadSettings;
import com.google.cloud.bigquery.storage.v1.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.cloud.bigquery.storage.v1.stub.EnhancedBigQueryReadStubSettings;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimaps;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

public class ReadRowsHelperTest {

  // it is not used, we just need the reference
  BigQueryReadClientFactory clientFactory = mock(BigQueryReadClientFactory.class);
  private ReadRowsRequest.Builder request = ReadRowsRequest.newBuilder().setReadStream("test");
  private ReadSessionCreatorConfig defaultConfig =
      new ReadSessionCreatorConfigBuilder().setMaxReadRowsRetries(1).build();

  private static FakeStorageService fakeService = new FakeStorageService();
  private static FakeStorageServer fakeServer;

  ReadRowsHelper helper;

  @BeforeClass
  public static void setupServer() throws IOException {
    fakeServer = new FakeStorageServer(fakeService);
  }

  @Before
  public void resetService() {
    fakeService.reset(ImmutableMap.of());
  }

  @After
  public void closeHelper() {
    if (helper != null) {
      helper.close();
    }
  }

  @AfterClass
  public static void teardownServer() throws InterruptedException {
    fakeServer.stop();
  }

  @Test
  public void testConfigSerializable() throws IOException {
    new ObjectOutputStream(new ByteArrayOutputStream())
        .writeObject(defaultConfig.toReadRowsHelperOptions());
  }

  BigQueryReadClient fakeServerClient() throws IOException {
    TransportChannelProvider transportationProvider =
        EnhancedBigQueryReadStubSettings.defaultGrpcTransportProviderBuilder()
            .setChannelConfigurator(
                (ManagedChannelBuilder a) -> {
                  a.usePlaintext();
                  return a;
                })
            .build();
    return BigQueryReadClient.create(
        BigQueryReadSettings.newBuilder()
            .setTransportChannelProvider(transportationProvider)
            .setCredentialsProvider(() -> null)
            .setEndpoint("127.0.0.1:" + fakeServer.port())
            .build());
  }

  @Test
  public void testNoFailures() throws IOException {
    MockResponsesBatch batch1 = new MockResponsesBatch();
    batch1.addResponse(ReadRowsResponse.newBuilder().setRowCount(10).build());
    batch1.addResponse(ReadRowsResponse.newBuilder().setRowCount(11).build());
    fakeService.reset(ImmutableMap.of(request.getReadStream(), batch1));

    when(clientFactory.createBigQueryReadClient(any())).thenReturn(fakeServerClient());

    // so we can run multiple tests
    helper = new ReadRowsHelper(clientFactory, request, defaultConfig.toReadRowsHelperOptions());
    ImmutableList<ReadRowsResponse> responses = ImmutableList.copyOf(helper.readRows());

    assertThat(responses.size()).isEqualTo(2);
    assertThat(responses.stream().mapToLong(ReadRowsResponse::getRowCount).sum()).isEqualTo(21);
  }

  @Test
  public void testCancel() throws IOException {
    MockResponsesBatch batch1 = new MockResponsesBatch();
    batch1.addResponse(ReadRowsResponse.newBuilder().setRowCount(10).build());
    batch1.addResponse(ReadRowsResponse.newBuilder().setRowCount(11).build());
    batch1.addResponse(ReadRowsResponse.newBuilder().setRowCount(11).build());
    batch1.addResponse(ReadRowsResponse.newBuilder().setRowCount(11).build());

    MockResponsesBatch batch2 = new MockResponsesBatch();
    batch2.addResponse(ReadRowsResponse.newBuilder().setRowCount(10).build());
    batch2.addResponse(ReadRowsResponse.newBuilder().setRowCount(11).build());
    batch2.addResponse(ReadRowsResponse.newBuilder().setRowCount(11).build());
    batch2.addResponse(ReadRowsResponse.newBuilder().setRowCount(11).build());
    ReadRowsRequest.Builder request2 = ReadRowsRequest.newBuilder().setReadStream("abc");

    fakeService.reset(
        ImmutableMap.of(request.getReadStream(), batch1, request2.getReadStream(), batch2));

    when(clientFactory.createBigQueryReadClient(any())).thenReturn(fakeServerClient());

    helper =
        new ReadRowsHelper(
            clientFactory,
            ImmutableList.of(request, request2),
            defaultConfig.toReadRowsHelperOptions());

    Iterator<ReadRowsResponse> responses = helper.readRows();
    // Try to make sure both requests are active.
    responses.next();
    responses.next();
    responses.next();
    helper.close();
    ImmutableList<ReadRowsResponse> remainingResponses = ImmutableList.copyOf(responses);

    assertThat(remainingResponses.size()).isLessThan(5);
  }

  @Test
  public void endpointIsPropagated() throws Exception {
    MockResponsesBatch batch1 = new MockResponsesBatch();
    batch1.addResponse(ReadRowsResponse.newBuilder().setRowCount(10).build());
    when(clientFactory.createBigQueryReadClient(any())).thenReturn(fakeServerClient());
    fakeService.reset(ImmutableMap.of(request.getReadStream(), batch1));
    ReadSessionCreatorConfig config =
        new ReadSessionCreatorConfigBuilder().setEndpoint(Optional.of("127.0.0.1:" + 123)).build();
    helper = new ReadRowsHelper(clientFactory, request, config.toReadRowsHelperOptions());
    assertThat(Iterators.getOnlyElement(helper.readRows()).getRowCount()).isEqualTo(10);
    ArgumentCaptor<Optional<String>> endpointCaptor = ArgumentCaptor.forClass(Optional.class);
    Mockito.verify(clientFactory, times(1)).createBigQueryReadClient(endpointCaptor.capture());
  }

  @Test
  public void testRetryOfSingleFailure() throws IOException {
    MockResponsesBatch batch1 = new MockResponsesBatch();
    batch1.addResponse(ReadRowsResponse.newBuilder().setRowCount(10).build());
    batch1.addException(
        new StatusRuntimeException(
            Status.INTERNAL.withDescription("HTTP/2 error code: INTERNAL_ERROR")));
    batch1.addResponse(ReadRowsResponse.newBuilder().setRowCount(11).build());
    fakeService.reset(ImmutableMap.of(request.getReadStream(), batch1));
    when(clientFactory.createBigQueryReadClient(any())).thenReturn(fakeServerClient());

    helper = new ReadRowsHelper(clientFactory, request, defaultConfig.toReadRowsHelperOptions());
    ImmutableList<ReadRowsResponse> responses = ImmutableList.copyOf(helper.readRows());

    assertThat(responses.size()).isEqualTo(2);
    assertThat(responses.stream().mapToLong(ReadRowsResponse::getRowCount).sum()).isEqualTo(21);

    List<ReadRowsRequest> requests = fakeService.requestsByStreamName.get(request.getReadStream());
    assertThat(requests)
        .containsExactly(request.setOffset(0).build(), request.setOffset(10).build())
        .inOrder();
  }

  @Test
  public void testCombinesStreamsTogether() throws IOException {
    MockResponsesBatch batch1 = new MockResponsesBatch();
    batch1.addResponse(ReadRowsResponse.newBuilder().setRowCount(10).build());
    batch1.addException(
        new StatusRuntimeException(
            Status.INTERNAL.withDescription("HTTP/2 error code: INTERNAL_ERROR")));
    batch1.addResponse(ReadRowsResponse.newBuilder().setRowCount(11).build());
    ReadRowsRequest.Builder request1 = ReadRowsRequest.newBuilder().setReadStream("r1");

    MockResponsesBatch batch2 = new MockResponsesBatch();
    batch2.addResponse(ReadRowsResponse.newBuilder().setRowCount(30).build());
    batch2.addException(
        new StatusRuntimeException(
            Status.INTERNAL.withDescription("HTTP/2 error code: INTERNAL_ERROR")));
    batch2.addResponse(ReadRowsResponse.newBuilder().setRowCount(45).build());
    ReadRowsRequest.Builder request2 = ReadRowsRequest.newBuilder().setReadStream("r2");
    fakeService.reset(
        ImmutableMap.of(
            request1.getReadStream(), batch1,
            request2.getReadStream(), batch2));
    when(clientFactory.createBigQueryReadClient(any())).thenReturn(fakeServerClient());

    helper =
        new ReadRowsHelper(
            clientFactory,
            ImmutableList.of(request1, request2),
            defaultConfig.toReadRowsHelperOptions());
    ImmutableList<ReadRowsResponse> responses = ImmutableList.copyOf(helper.readRows());

    assertThat(responses.size()).isEqualTo(4);
    assertThat(responses.stream().mapToLong(ReadRowsResponse::getRowCount).sum()).isEqualTo(96);

    List<ReadRowsRequest> requests1 =
        fakeService.requestsByStreamName.get(request1.getReadStream());
    assertThat(requests1)
        .containsExactly(request1.setOffset(0).build(), request1.setOffset(10).build())
        .inOrder();

    List<ReadRowsRequest> requests2 =
        fakeService.requestsByStreamName.get(request2.getReadStream());
    assertThat(requests2)
        .containsExactly(request2.setOffset(0).build(), request2.setOffset(30).build())
        .inOrder();
  }

  private static class FakeStorageService extends BigQueryReadImplBase {
    private Map<String, MockResponsesBatch> responsesByStreamName;
    private final ListMultimap<String, ReadRowsRequest> requestsByStreamName =
        Multimaps.synchronizedListMultimap(ArrayListMultimap.create());

    void reset(Map<String, MockResponsesBatch> responsesByStreamName) {
      this.responsesByStreamName = responsesByStreamName;
      requestsByStreamName.clear();
    }

    @Override
    public void readRows(
        ReadRowsRequest request, StreamObserver<ReadRowsResponse> responseObserver) {
      String name = request.getReadStream();
      requestsByStreamName.put(name, request);
      MockResponsesBatch responses = responsesByStreamName.get(name);

      if (responses == null) {
        responseObserver.onError(new RuntimeException("No responses for stream: " + name));
      }

      while (responses.hasNext()) {
        try {
          ReadRowsResponse next = responses.next();
          if (next == null) {
            responseObserver.onError(new RuntimeException("Null element found: " + name));
            return;
          }
          responseObserver.onNext(next);
        } catch (Exception e) {
          responseObserver.onError(e);
          return;
        }
      }
      responseObserver.onCompleted();
    }
  }

  private static class FakeStorageServer {
    private final Server server;

    FakeStorageServer(FakeStorageService service) throws IOException {
      // Zero should pick a random port.
      ServerBuilder<?> serverBuilder = NettyServerBuilder.forPort(0);
      server = serverBuilder.addService(service).build();
      server.start();
    }

    int port() {
      return server.getPort();
    }

    void stop() throws InterruptedException {
      server.shutdownNow();
      server.awaitTermination(10, TimeUnit.SECONDS);
    }
  }
}
