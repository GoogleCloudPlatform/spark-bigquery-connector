package org.apache.spark.bigquery;

import org.apache.spark.sql.types.SQLUserDefinedType;
import java.math.BigDecimal;

@SQLUserDefinedType(udt = BigNumericUDT.class)
public class BigNumeric {
    private final BigDecimal number;

    public BigNumeric(BigDecimal number) {
        this.number = number;
    }

    public BigDecimal getNumber() {
        return this.number;
    }

    @Override
    public String toString() {
        return this.number.toPlainString();
    }
}
