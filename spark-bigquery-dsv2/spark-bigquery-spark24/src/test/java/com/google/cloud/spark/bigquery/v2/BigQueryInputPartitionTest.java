package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.bigquery.connector.common.ReadRowsHelper;
import com.google.cloud.bigquery.connector.common.ReadRowsHelper.Options;
import com.google.cloud.bigquery.connector.common.ReadSessionResponse;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.common.collect.Lists;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Optional;
import org.junit.Test;

public class BigQueryInputPartitionTest {
  @Test
  public void testSerializability() throws IOException {
    new ObjectOutputStream(new ByteArrayOutputStream())
        .writeObject(
            new ArrowInputPartition(
                /*bigQueryClientFactory=*/ null,
                /*tracerFactory=*/ null,
                Lists.newArrayList("streamName"),
                new ReadRowsHelper.Options(
                    /*maxRetries=*/ 5,
                    Optional.of("endpoint"),
                    /*backgroundParsingThreads=*/ 5,
                    /*prebufferResponses=*/ 1),
                null,
                new ReadSessionResponse(ReadSession.getDefaultInstance(), null),
                null));
  }
}
