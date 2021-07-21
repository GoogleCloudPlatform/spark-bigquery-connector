package com.google.cloud.bigquery.connector.common;

import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import java.io.InputStream;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Instrumented {@link java.util.Enumeration} for translating bytes received from the BQ Storage API
 * to continuous input streams.
 */
public class ReadRowsResponseInputStreamEnumeration implements java.util.Enumeration<InputStream> {

  private final Iterator<ReadRowsResponse> responses;
  private ReadRowsResponse currentResponse;
  private final BigQueryStorageReadRowsTracer tracer;

  public ReadRowsResponseInputStreamEnumeration(
      Iterator<ReadRowsResponse> responses, BigQueryStorageReadRowsTracer tracer) {
    this.responses = responses;
    this.tracer = tracer;
    loadNextResponse();
  }

  public boolean hasMoreElements() {
    return currentResponse != null;
  }

  public InputStream nextElement() {
    if (!hasMoreElements()) {
      throw new NoSuchElementException("No more responses");
    }
    ReadRowsResponse ret = currentResponse;
    loadNextResponse();
    return ret.getArrowRecordBatch().getSerializedRecordBatch().newInput();
  }

  void loadNextResponse() {
    // hasNext is actually the blocking call, so call  readRowsResponseRequested
    // here.
    tracer.readRowsResponseRequested();
    if (responses.hasNext()) {
      currentResponse = responses.next();
    } else {
      currentResponse = null;
    }
    tracer.readRowsResponseObtained(
        currentResponse == null
            ? 0
            : currentResponse.getArrowRecordBatch().getSerializedRecordBatch().size());
  }
}
