package com.google.cloud.spark.bigquery;

import java.util.Map;

/**
 * A single place to put data source implementations customizations, in order to avoid a general
 * `if(dataSource==v1) {...} else {...}` throughout the code.
 */
public enum DataSourceVersion {
  V1,
  V2;

  public void updateOptionsMap(Map<String, String> optionsMap) {
    // these updates are v2 only
    if (this == V2) {
      // no need for the spark-avro module, we have an internal copy of avro
      optionsMap.put(SparkBigQueryConfig.VALIDATE_SPARK_AVRO_PARAM.toLowerCase(), "false");
      // DataSource V2 implementation uses Java only
      optionsMap.put(
          SparkBigQueryConfig.INTERMEDIATE_FORMAT_OPTION.toLowerCase(),
          SparkBigQueryConfig.IntermediateFormat.AVRO.toString());
    }
  }
}
