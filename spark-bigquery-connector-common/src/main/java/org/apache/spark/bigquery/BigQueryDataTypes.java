package org.apache.spark.bigquery;

import org.apache.spark.sql.types.DataType;

public class BigQueryDataTypes {
    public static final DataType BigNumericType = new BigNumericUDT();
}
