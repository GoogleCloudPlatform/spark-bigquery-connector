/*
 * Copyright 2020 Google Inc. All Rights Reserved.
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

import com.google.common.base.Preconditions;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.util.Utf8;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.SpecializedGetters;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.BooleanType;
import org.apache.spark.sql.types.ByteType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.FloatType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.NullType;
import org.apache.spark.sql.types.ShortType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.TimestampType;
import org.apache.spark.sql.types.UserDefinedType;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;

public class AvroSchemaConverter {

  private static final Schema NULL = Schema.create(Schema.Type.NULL);
  private static final Conversions.DecimalConversion DECIMAL_CONVERSIONS =
      new Conversions.DecimalConversion();

  public static Schema sparkSchemaToAvroSchema(StructType sparkSchema) {
    return sparkTypeToRawAvroType(sparkSchema, false, "root");
  }

  static Schema sparkTypeToRawAvroType(DataType dataType, boolean nullable, String recordName) {
    SchemaBuilder.TypeBuilder<Schema> builder = SchemaBuilder.builder();
    Schema avroType = sparkTypeToRawAvroType(dataType, recordName, builder);

    if (nullable) {
      avroType = Schema.createUnion(avroType, NULL);
    }

    return avroType;
  }

  static Schema sparkTypeToRawAvroType(
      DataType dataType, String recordName, SchemaBuilder.TypeBuilder<Schema> builder) {
    if (dataType instanceof BinaryType) {
      return builder.bytesType();
    }
    if (dataType instanceof ByteType
        || dataType instanceof ShortType
        || dataType instanceof IntegerType
        || dataType instanceof LongType) {
      return builder.longType();
    }
    if (dataType instanceof BooleanType) {
      return builder.booleanType();
    }
    if (dataType instanceof FloatType || dataType instanceof DoubleType) {
      return builder.doubleType();
    }
    if (dataType instanceof DecimalType) {
      DecimalType decimalType = (DecimalType) dataType;
      if (decimalType.precision() <= SchemaConverters.BQ_NUMERIC_PRECISION
          && decimalType.scale() <= SchemaConverters.BQ_NUMERIC_SCALE) {
        return LogicalTypes.decimal(decimalType.precision(), decimalType.scale())
            .addToSchema(builder.bytesType());
      } else {
        throw new IllegalArgumentException(
            "Decimal type is too wide to fit in BigQuery Numeric format");
      }
    }
    if (dataType instanceof StringType) {
      return builder.stringType();
    }
    if (dataType instanceof TimestampType) {
      // return builder.TIMESTAMP; FIXME: Restore this correct conversion when the Vortex
      // team adds microsecond support to their backend
      return LogicalTypes.timestampMicros().addToSchema(builder.longType());
    }
    if (dataType instanceof DateType) {
      return LogicalTypes.date().addToSchema(builder.intType());
    }
    if (dataType instanceof ArrayType) {
      return builder
          .array()
          .items(
              sparkTypeToRawAvroType(
                  ((ArrayType) dataType).elementType(),
                  ((ArrayType) dataType).containsNull(),
                  recordName));
    }
    if (dataType instanceof StructType) {
      SchemaBuilder.FieldAssembler<Schema> fieldsAssembler = builder.record(recordName).fields();
      for (StructField field : ((StructType) dataType).fields()) {
        Schema avroType = sparkTypeToRawAvroType(field.dataType(), field.nullable(), field.name());
        fieldsAssembler.name(field.name()).type(avroType).noDefault();
      }
      return fieldsAssembler.endRecord();
    }
    if (dataType instanceof UserDefinedType) {
      DataType userDefinedType = ((UserDefinedType) dataType).sqlType();
      return sparkTypeToRawAvroType(userDefinedType, recordName, builder);
    }
    if (dataType instanceof MapType) {
      throw new IllegalArgumentException(SchemaConverters.MAPTYPE_ERROR_MESSAGE);
    } else {
      throw new IllegalArgumentException("Data type not supported: " + dataType.simpleString());
    }
  }

  public static GenericData.Record sparkRowToAvroGenericData(
      InternalRow row, StructType sparkSchema, Schema avroSchema) {
    StructConverter structConverter = new StructConverter(sparkSchema, avroSchema);
    return structConverter.convert(row);
  }

  static Schema resolveNullableType(Schema avroType, boolean nullable) {
    if (nullable && avroType.getType() != Schema.Type.NULL) {
      // avro uses union to represent nullable type.
      List<Schema> fields = avroType.getTypes();
      Preconditions.checkArgument(
          fields.size() == 2, "Avro nullable filed should be represented by a union of size 2");
      Optional<Schema> actualType =
          fields.stream().filter(field -> field.getType() != Schema.Type.NULL).findFirst();
      return actualType.orElseThrow(
          () -> new IllegalArgumentException("No actual type has been found in " + avroType));
    } else {
      return avroType;
    }
  }

  static Converter createConverterFor(DataType sparkType, Schema avroType) {
    if (sparkType instanceof NullType && avroType.getType() == Schema.Type.NULL) {
      return (getter, ordinal) -> null;
    }
    if (sparkType instanceof BooleanType && avroType.getType() == Schema.Type.BOOLEAN) {
      return (getter, ordinal) -> getter.getBoolean(ordinal);
    }
    if (sparkType instanceof ByteType && avroType.getType() == Schema.Type.LONG) {
      return (getter, ordinal) -> Long.valueOf(getter.getByte(ordinal));
    }
    if (sparkType instanceof ShortType && avroType.getType() == Schema.Type.LONG) {
      return (getter, ordinal) -> Long.valueOf(getter.getShort(ordinal));
    }
    if (sparkType instanceof IntegerType && avroType.getType() == Schema.Type.LONG) {
      return (getter, ordinal) -> Long.valueOf(getter.getInt(ordinal));
    }
    if (sparkType instanceof LongType && avroType.getType() == Schema.Type.LONG) {
      return (getter, ordinal) -> getter.getLong(ordinal);
    }
    if (sparkType instanceof FloatType && avroType.getType() == Schema.Type.DOUBLE) {
      return (getter, ordinal) -> Double.valueOf(getter.getFloat(ordinal));
    }
    if (sparkType instanceof DoubleType && avroType.getType() == Schema.Type.DOUBLE) {
      return (getter, ordinal) -> getter.getDouble(ordinal);
    }
    if (sparkType instanceof DecimalType && avroType.getType() == Schema.Type.BYTES) {
      DecimalType decimalType = (DecimalType) sparkType;
      return (getter, ordinal) -> {
        Decimal decimal = getter.getDecimal(ordinal, decimalType.precision(), decimalType.scale());
        return DECIMAL_CONVERSIONS.toBytes(
            decimal.toJavaBigDecimal(),
            avroType,
            LogicalTypes.decimal(decimalType.precision(), decimalType.scale()));
      };
    }
    if (sparkType instanceof StringType && avroType.getType() == Schema.Type.STRING) {
      return (getter, ordinal) -> new Utf8(getter.getUTF8String(ordinal).getBytes());
    }
    if (sparkType instanceof BinaryType && avroType.getType() == Schema.Type.FIXED) {
      int size = avroType.getFixedSize();
      return (getter, ordinal) -> {
        byte[] data = getter.getBinary(ordinal);
        if (data.length != size) {
          throw new IllegalArgumentException(
              String.format(
                  "Cannot write %s bytes of binary data into FIXED Type with size of %s bytes",
                  data.length, size));
        }
        return new GenericData.Fixed(avroType, data);
      };
    }
    if (sparkType instanceof BinaryType && avroType.getType() == Schema.Type.BYTES) {
      return (getter, ordinal) -> ByteBuffer.wrap(getter.getBinary(ordinal));
    }

    if (sparkType instanceof DateType && avroType.getType() == Schema.Type.INT) {
      return (getter, ordinal) -> getter.getInt(ordinal);
    }

    if (sparkType instanceof TimestampType && avroType.getType() == Schema.Type.LONG) {
      return (getter, ordinal) -> getter.getLong(ordinal);
    }

    if (sparkType instanceof ArrayType && avroType.getType() == Schema.Type.ARRAY) {
      DataType et = ((ArrayType) sparkType).elementType();
      boolean containsNull = ((ArrayType) sparkType).containsNull();

      Converter elementConverter =
          createConverterFor(et, resolveNullableType(avroType.getElementType(), containsNull));
      return (getter, ordinal) -> {
        ArrayData arrayData = getter.getArray(ordinal);
        int len = arrayData.numElements();
        Object[] result = new Object[len];
        for (int i = 0; i < len; i++) {
          if (containsNull && arrayData.isNullAt(i)) {
            result[i] = null;
          } else {
            result[i] = elementConverter.convert(arrayData, i);
          }
        }
        // avro writer is expecting a Java Collection, so we convert it into
        // `ArrayList` backed by the specified array without data copying.
        return java.util.Arrays.asList(result);
      };
    }
    if (sparkType instanceof StructType && avroType.getType() == Schema.Type.RECORD) {
      StructType sparkStruct = (StructType) sparkType;

      StructConverter structConverter = new StructConverter(sparkStruct, avroType);
      int numFields = sparkStruct.length();
      return (getter, ordinal) -> structConverter.convert(getter.getStruct(ordinal, numFields));
    }
    if (sparkType instanceof UserDefinedType) {
      UserDefinedType userDefinedType = (UserDefinedType) sparkType;
      return createConverterFor(userDefinedType.sqlType(), avroType);
    }
    throw new IllegalArgumentException(
        String.format("Cannot convert Catalyst type %s to Avro type %s", sparkType, avroType));
  }

  @FunctionalInterface
  interface Converter {
    Object convert(SpecializedGetters getters, int ordinal);
  }

  static class StructConverter {
    private final StructType sparkStruct;
    private final Schema avroStruct;

    StructConverter(StructType sparkStruct, Schema avroStruct) {
      this.sparkStruct = sparkStruct;
      this.avroStruct = avroStruct;
      Preconditions.checkArgument(
          avroStruct.getType() == Schema.Type.RECORD
              && avroStruct.getFields().size() == sparkStruct.length(),
          "Cannot convert Catalyst type %s to Avro type %s.",
          sparkStruct,
          avroStruct);
    }

    GenericData.Record convert(InternalRow row) {
      int numFields = sparkStruct.length();
      Converter[] fieldConverters = new Converter[numFields];
      StructField[] sparkFields = sparkStruct.fields();
      Schema.Field[] avroFields = avroStruct.getFields().toArray(new Schema.Field[numFields]);

      GenericData.Record result = new GenericData.Record(avroStruct);

      for (int i = 0; i < numFields; i++) {
        if (row.isNullAt(i)) {
          result.put(i, null);
        } else {
          Converter fieldConverter =
              AvroSchemaConverter.createConverterFor(
                  sparkFields[i].dataType(),
                  AvroSchemaConverter.resolveNullableType(
                      avroFields[i].schema(), sparkFields[i].nullable()));
          result.put(i, fieldConverter.convert(row, i));
        }
      }
      return result;
    }
  }
}
