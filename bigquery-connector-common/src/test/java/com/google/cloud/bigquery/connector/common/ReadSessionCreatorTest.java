package com.google.cloud.bigquery.connector.common;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.notNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.core.ApiFuture;
import com.google.api.gax.rpc.ApiCallContext;
import com.google.api.gax.rpc.UnaryCallable;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import com.google.cloud.bigquery.storage.v1.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.cloud.bigquery.storage.v1.ReadSession.TableReadOptions;
import com.google.cloud.bigquery.storage.v1.stub.EnhancedBigQueryReadStub;
import com.google.common.collect.ImmutableList;
import java.util.Optional;
import java.util.OptionalInt;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class ReadSessionCreatorTest {
  EnhancedBigQueryReadStub stub = mock(EnhancedBigQueryReadStub.class);
  BigQueryClient bigQueryClient = mock(BigQueryClient.class);
  UnaryCallable<CreateReadSessionRequest, ReadSession> createReadSessionCall =
      mock(UnaryCallable.class);
  BigQueryReadClient readClient = BigQueryReadClient.create(stub);
  BigQueryReadClientFactory bigQueryReadClientFactory = mock(BigQueryReadClientFactory.class);
  TableInfo table =
      TableInfo.newBuilder(
              TableId.of("a", "b"),
              StandardTableDefinition.newBuilder()
                  .setSchema(Schema.of(Field.of("name", StandardSQLTypeName.BOOL)))
                  .setNumBytes(1L)
                  .build())
          .build();

  @Test
  public void testSerializedInstanceIsPropagated() throws Exception {
    TableReadOptions tableReadOptions = TableReadOptions.newBuilder().build();
    ReadSession readSession =
        ReadSession.newBuilder().setName("abc").setReadOptions(tableReadOptions).build();
    CreateReadSessionRequest request =
        CreateReadSessionRequest.newBuilder().setReadSession(readSession).build();
    Optional<String> encodedBase =
        Optional.of(java.util.Base64.getEncoder().encodeToString(request.toByteArray()));
    ReadSessionCreatorConfig config =
        new ReadSessionCreatorConfigBuilder().setRequestEncodedBase(encodedBase).build();
    ReadSessionCreator creator =
        new ReadSessionCreator(config, bigQueryClient, bigQueryReadClientFactory);
    when(bigQueryReadClientFactory.createBigQueryReadClient(eq(Optional.empty())))
        .thenReturn(readClient);
    when(bigQueryClient.getTable(any())).thenReturn(table);
    when(stub.createReadSessionCallable()).thenReturn(createReadSessionCall);

    creator
        .create(TableId.of("dataset", "table"), ImmutableList.of("col1", "col2"), Optional.empty())
        .getReadSession();

    ArgumentCaptor<CreateReadSessionRequest> requestCaptor =
        ArgumentCaptor.forClass(CreateReadSessionRequest.class);
    verify(createReadSessionCall, times(1)).call(requestCaptor.capture());

    ReadSession actual = requestCaptor.getValue().getReadSession();
    assertThat(actual.getName()).isEqualTo("abc");
    assertThat(actual.getReadOptions().getSelectedFieldsList()).containsExactly("col1", "col2");
  }
}
