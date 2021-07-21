package com.google.cloud.spark.bigquery.spark3;

import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface DataFrameToRDDConverter {
  RDD<Row> convertToRDD(Dataset<Row> data);
}
