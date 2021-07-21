package org.apache.spark.bigquery;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.UserDefinedType;
import org.apache.spark.unsafe.types.UTF8String;

import java.math.BigDecimal;

public class BigNumericUDT extends UserDefinedType<BigNumeric>{

    @Override
    public DataType sqlType() {
        return DataTypes.StringType;
    }

    @Override
    public InternalRow serialize(BigNumeric obj) {
        InternalRow row = new GenericInternalRow(1);
        row.update(0, UTF8String.fromString(obj.getNumber().toPlainString()));
        return row;
    }

    @Override
    public BigNumeric deserialize(Object datum) {
        if(!(datum instanceof UTF8String)) {
            throw new IllegalArgumentException(
                    "Failed to deserialize, was expecting an instance of UTF8String, " +
                            "instead got an instance of " + datum.getClass());
        }

        UTF8String utf8str = (UTF8String)datum;
        BigNumeric bigNumeric = new BigNumeric(new BigDecimal(utf8str.toString()));
        return bigNumeric;
    }

    @Override
    public Class<BigNumeric> userClass() {
        return BigNumeric.class;
    }

    @Override
    public String pyUDT() {
        return "google.cloud.spark.bigquery.big_numeric_support.BigNumericUDT";
    }
}
