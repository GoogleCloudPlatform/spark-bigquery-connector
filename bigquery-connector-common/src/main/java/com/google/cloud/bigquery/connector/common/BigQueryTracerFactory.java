package com.google.cloud.bigquery.connector.common;

import java.io.Serializable;

/** Factory to create application level tracers for bigquery operations. */
public interface BigQueryTracerFactory extends Serializable {
  BigQueryStorageReadRowsTracer newReadRowsTracer(String streamName);
}
