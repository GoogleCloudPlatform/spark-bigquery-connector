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
package com.google.cloud.spark.bigquery;

import com.google.common.collect.ImmutableList;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;

import static com.google.common.truth.Truth.assertThat;

public class AvroSchemaConverterTest {

  @Test
  public void testSchemaConversion() {
    StructType sparkSchema = TestConstants.ALL_TYPES_TABLE_SCHEMA;
    Schema avroSchema = AvroSchemaConverter.sparkSchemaToAvroSchema(sparkSchema);
    Schema.Field[] fields =
        avroSchema.getFields().toArray(new Schema.Field[avroSchema.getFields().size()]);
    checkField(fields[0], "int_req", Schema.create(Schema.Type.LONG));
    checkField(fields[1], "int_null", nullable(Schema.Type.LONG));
    checkField(fields[2], "bl", nullable(Schema.Type.BOOLEAN));
    checkField(fields[3], "str", nullable(Schema.Type.STRING));
    checkField(
        fields[4],
        "day",
        nullable(LogicalTypes.date().addToSchema(SchemaBuilder.builder().intType())));
    checkField(
        fields[5],
        "ts",
        nullable(LogicalTypes.timestampMicros().addToSchema(SchemaBuilder.builder().longType())));
    checkField(fields[6], "dt", nullable(Schema.Type.STRING));
    checkField(fields[7], "tm", nullable(Schema.Type.LONG));
    checkField(fields[8], "binary", nullable(Schema.Type.BYTES));
    checkField(fields[9], "float", nullable(Schema.Type.DOUBLE));
    checkField(
        fields[10],
        "nums",
        nullable(
            Schema.createRecord(
                "nums",
                null,
                null,
                false,
                ImmutableList.of(
                    new Schema.Field("min", nullable(decimal("min")), null, (Object) null),
                    new Schema.Field("max", nullable(decimal("max")), null, (Object) null),
                    new Schema.Field("pi", nullable(decimal("pi")), null, (Object) null),
                    new Schema.Field(
                        "big_pi", nullable(decimal("big_pi")), null, (Object) null)))));

    checkField(
        fields[11],
        "big_numeric_nums",
        nullable(
            Schema.createRecord(
                "big_numeric_nums",
                null,
                null,
                false,
                ImmutableList.of(
                    new Schema.Field("min", nullable(Schema.Type.STRING), null, (Object) null),
                    new Schema.Field("max", nullable(Schema.Type.STRING), null, (Object) null)))));

    checkField(fields[12], "int_arr", nullable(Schema.createArray(nullable(Schema.Type.LONG))));
    checkField(
        fields[13],
        "int_struct_arr",
        nullable(
            Schema.createArray(
                nullable(
                    Schema.createRecord(
                        "int_struct_arr",
                        null,
                        null,
                        false,
                        ImmutableList.of(
                            new Schema.Field(
                                "i", nullable(Schema.Type.LONG), null, (Object) null)))))));
  }

  @Test
  public void testConvertIntegers() {
    InternalRow row =
        new GenericInternalRow(
            new Object[] {
              Byte.valueOf("0"), Short.valueOf("1"), Integer.valueOf(2), Long.valueOf(3)
            });
    StructType sparkSchema =
        DataTypes.createStructType(
            ImmutableList.of(
                DataTypes.createStructField("byte_f", DataTypes.ByteType, false),
                DataTypes.createStructField("short_f", DataTypes.ShortType, false),
                DataTypes.createStructField("int_f", DataTypes.IntegerType, false),
                DataTypes.createStructField("long_f", DataTypes.LongType, false)));

    Schema avroSchema =
        SchemaBuilder.record("root")
            .fields() //
            .name("byte_f")
            .type(SchemaBuilder.builder().longType())
            .noDefault() //
            .name("short_f")
            .type(SchemaBuilder.builder().longType())
            .noDefault() //
            .name("int_f")
            .type(SchemaBuilder.builder().longType())
            .noDefault() //
            .name("long_f")
            .type(SchemaBuilder.builder().longType())
            .noDefault() //
            .endRecord();
    GenericData.Record result =
        AvroSchemaConverter.sparkRowToAvroGenericData(row, sparkSchema, avroSchema);
    assertThat(result.getSchema()).isEqualTo(avroSchema);
    assertThat(result.get(0)).isEqualTo(Long.valueOf(0));
    assertThat(result.get(1)).isEqualTo(Long.valueOf(1));
    assertThat(result.get(2)).isEqualTo(Long.valueOf(2));
    assertThat(result.get(3)).isEqualTo(Long.valueOf(3));
  }

  @Test
  public void testConvertNull() {
    InternalRow row = new GenericInternalRow(new Object[] {null});
    StructType sparkSchema =
        DataTypes.createStructType(
            ImmutableList.of(DataTypes.createStructField("null_f", DataTypes.LongType, true)));

    Schema avroSchema =
        SchemaBuilder.record("root")
            .fields() //
            .name("long_f")
            .type(
                SchemaBuilder.unionOf()
                    .type(SchemaBuilder.builder().longType())
                    .and()
                    .nullType()
                    .endUnion())
            .noDefault() //
            .endRecord();
    GenericData.Record result =
        AvroSchemaConverter.sparkRowToAvroGenericData(row, sparkSchema, avroSchema);
    assertThat(result.getSchema()).isEqualTo(avroSchema);
    assertThat(result.get(0)).isNull();
  }

  @Test
  public void testConvertNullable() {
    InternalRow row = new GenericInternalRow(new Object[] {Long.valueOf(0)});
    StructType sparkSchema =
        DataTypes.createStructType(
            ImmutableList.of(DataTypes.createStructField("null_f", DataTypes.LongType, true)));

    Schema avroSchema =
        SchemaBuilder.record("root")
            .fields() //
            .name("long_f")
            .type(
                SchemaBuilder.unionOf()
                    .type(SchemaBuilder.builder().longType())
                    .and()
                    .nullType()
                    .endUnion())
            .noDefault() //
            .endRecord();
    GenericData.Record result =
        AvroSchemaConverter.sparkRowToAvroGenericData(row, sparkSchema, avroSchema);
    assertThat(result.getSchema()).isEqualTo(avroSchema);
    assertThat(result.get(0)).isEqualTo(Long.valueOf(0));
  }

  @Test
  public void testConvertDecimal() {
    InternalRow row =
        new GenericInternalRow(
            new Object[] {
              Decimal.apply(BigDecimal.valueOf(123.456), SchemaConverters.BQ_NUMERIC_PRECISION, 3)
            });
    StructType sparkSchema =
        DataTypes.createStructType(
            ImmutableList.of(
                DataTypes.createStructField(
                    "decimal_f",
                    DataTypes.createDecimalType(SchemaConverters.BQ_NUMERIC_PRECISION, 3),
                    false)));

    Schema avroSchema =
        SchemaBuilder.record("root")
            .fields() //
            .name("decimal_f")
            .type(decimal("decimal_f"))
            .noDefault() //
            .endRecord();
    GenericData.Record result =
        AvroSchemaConverter.sparkRowToAvroGenericData(row, sparkSchema, avroSchema);
    assertThat(result.getSchema()).isEqualTo(avroSchema);
    Conversions.DecimalConversion decimalConversion = new Conversions.DecimalConversion();
    assertThat(
            decimalConversion.fromBytes(
                (ByteBuffer) result.get(0),
                avroSchema.getField("decimal_f").schema(),
                LogicalTypes.decimal(SchemaConverters.BQ_NUMERIC_PRECISION, 3)))
        .isEqualTo(BigDecimal.valueOf(123.456));
  }

  @Test
  public void testConvertDoubles() {
    InternalRow row =
        new GenericInternalRow(new Object[] {Float.valueOf("0.0"), Double.valueOf("1.1")});
    StructType sparkSchema =
        DataTypes.createStructType(
            ImmutableList.of(
                DataTypes.createStructField("float_f", DataTypes.FloatType, false),
                DataTypes.createStructField("double_f", DataTypes.DoubleType, false)));

    Schema avroSchema =
        SchemaBuilder.record("root")
            .fields() //
            .name("float_f")
            .type(SchemaBuilder.builder().doubleType())
            .noDefault() //
            .name("double_f")
            .type(SchemaBuilder.builder().doubleType())
            .noDefault() //
            .endRecord();
    GenericData.Record result =
        AvroSchemaConverter.sparkRowToAvroGenericData(row, sparkSchema, avroSchema);
    assertThat(result.getSchema()).isEqualTo(avroSchema);
    assertThat(result.get(0)).isEqualTo(Double.valueOf(0.0));
    assertThat(result.get(1)).isEqualTo(Double.valueOf(1.1));
  }

  @Test
  public void testConvertDateTime() {
    InternalRow row =
        new GenericInternalRow(new Object[] {Integer.valueOf(15261), Long.valueOf(1318608914000L)});
    StructType sparkSchema =
        DataTypes.createStructType(
            ImmutableList.of(
                DataTypes.createStructField("date_f", DataTypes.DateType, false),
                DataTypes.createStructField("ts_f", DataTypes.TimestampType, false)));
    Schema avroSchema =
        SchemaBuilder.record("root")
            .fields() //
            .name("date_f")
            .type(LogicalTypes.date().addToSchema(SchemaBuilder.builder().intType()))
            .noDefault() //
            .name("ts_f")
            .type(LogicalTypes.timestampMicros().addToSchema(SchemaBuilder.builder().longType()))
            .noDefault() //
            .endRecord();
    GenericData.Record result =
        AvroSchemaConverter.sparkRowToAvroGenericData(row, sparkSchema, avroSchema);
    assertThat(result.getSchema()).isEqualTo(avroSchema);
    assertThat(result.get(0)).isEqualTo(15261);
    assertThat(result.get(1)).isEqualTo(1318608914000L);
  }

  @Test
  public void testComparisonToSparkAvro() {}

  private void checkField(Schema.Field field, String name, Schema schema) {
    assertThat(field.name()).isEqualTo(name);
    assertThat(field.schema()).isEqualTo(schema);
  }

  private Schema decimal(String name) {
    return LogicalTypes.decimal(38, 9).addToSchema(SchemaBuilder.builder().bytesType());
  }

  Schema nullable(Schema schema) {
    return Schema.createUnion(schema, Schema.create(Schema.Type.NULL));
  }

  Schema nullable(Schema.Type type) {
    return nullable(Schema.create(type));
  }
}
