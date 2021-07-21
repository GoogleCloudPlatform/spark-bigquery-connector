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

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.storage.v1beta2.ProtoRows;
import com.google.cloud.bigquery.storage.v1beta2.ProtoSchema;
import com.google.cloud.bigquery.storage.v1beta2.ProtoSchemaConverter;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.AssumptionViolatedException;
import org.junit.Test;

import static com.google.cloud.spark.bigquery.ProtobufUtils.buildDescriptorProtoWithFields;
import static com.google.cloud.spark.bigquery.ProtobufUtils.buildSingleRowMessage;
import static com.google.cloud.spark.bigquery.ProtobufUtils.toDescriptor;
import static com.google.cloud.spark.bigquery.ProtobufUtils.toProtoRows;
import static com.google.cloud.spark.bigquery.ProtobufUtils.toProtoSchema;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.fail;

public class ProtobufUtilsTest {

  // Numeric is a fixed precision Decimal Type with 38 digits of precision and 9 digits of scale.
  // See https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#numeric-type
  private static final int BQ_NUMERIC_PRECISION = 38;
  private static final int BQ_NUMERIC_SCALE = 9;
  private static final DecimalType NUMERIC_SPARK_TYPE =
      DataTypes.createDecimalType(BQ_NUMERIC_PRECISION, BQ_NUMERIC_SCALE);
  // The maximum nesting depth of a BigQuery RECORD:
  private static final int MAX_BIGQUERY_NESTED_DEPTH = 15;

  private final Logger logger = LogManager.getLogger("com.google.cloud.spark");

  @Test
  public void testBigQueryRecordToDescriptor() throws Exception {
    logger.setLevel(Level.DEBUG);

    DescriptorProtos.DescriptorProto expected = NESTED_STRUCT_DESCRIPTOR.setName("Struct").build();
    DescriptorProtos.DescriptorProto converted =
        buildDescriptorProtoWithFields(
            DescriptorProtos.DescriptorProto.newBuilder().setName("Struct"),
            BIGQUERY_NESTED_STRUCT_FIELD.getSubFields(),
            0);

    assertThat(converted).isEqualTo(expected);
  }

  @Test
  public void testBigQueryToProtoSchema() throws Exception {
    logger.setLevel(Level.DEBUG);

    ProtoSchema converted = toProtoSchema(BIG_BIGQUERY_SCHEMA);
    ProtoSchema expected =
        ProtoSchemaConverter.convert(
            Descriptors.FileDescriptor.buildFrom(
                    DescriptorProtos.FileDescriptorProto.newBuilder()
                        .addMessageType(
                            DescriptorProtos.DescriptorProto.newBuilder()
                                .addField(PROTO_INTEGER_FIELD.clone().setNumber(1))
                                .addField(PROTO_STRING_FIELD.clone().setNumber(2))
                                .addField(PROTO_ARRAY_FIELD.clone().setNumber(3))
                                .addNestedType(NESTED_STRUCT_DESCRIPTOR.clone())
                                .addField(PROTO_STRUCT_FIELD.clone().setNumber(4))
                                .addField(PROTO_DOUBLE_FIELD.clone().setName("Float").setNumber(5))
                                .addField(PROTO_BOOLEAN_FIELD.clone().setNumber(6))
                                .addField(PROTO_BYTES_FIELD.clone().setNumber(7))
                                .addField(PROTO_INTEGER_FIELD.clone().setName("Date").setNumber(8))
                                .addField(
                                    PROTO_INTEGER_FIELD.clone().setName("TimeStamp").setNumber(9))
                                .setName("Schema")
                                .build())
                        .build(),
                    new Descriptors.FileDescriptor[] {})
                .getMessageTypes()
                .get(0));

    logger.debug("Expected schema: " + expected.getProtoDescriptor());
    logger.debug("Actual schema: " + converted.getProtoDescriptor());

    for (int i = 0; i < expected.getProtoDescriptor().getFieldList().size(); i++) {
      assertThat(converted.getProtoDescriptor().getField(i))
          .isEqualTo(expected.getProtoDescriptor().getField(i));
    }
  }

  @Test
  public void testSparkStructRowToDynamicMessage() throws Exception {
    logger.setLevel(Level.DEBUG);

    StructType schema = new StructType().add(SPARK_NESTED_STRUCT_FIELD);
    Descriptors.Descriptor schemaDescriptor = toDescriptor(schema);
    Message converted = buildSingleRowMessage(schema, schemaDescriptor, STRUCT_INTERNAL_ROW);
    DynamicMessage expected = StructRowMessage;

    assertThat(converted.toString()).isEqualTo(expected.toString());
  }

  @Test
  public void testSparkRowToProtoRow() throws Exception {
    logger.setLevel(Level.DEBUG);

    ProtoRows converted =
        toProtoRows(
            BIG_SPARK_SCHEMA,
            new InternalRow[] {
              new GenericInternalRow(
                  new Object[] {
                    1,
                    UTF8String.fromString("A"),
                    ArrayData.toArrayData(new int[] {0, 1, 2}),
                    INTERNAL_STRUCT_DATA,
                    3.14,
                    true,
                    new byte[] {11, 0x7F},
                    1594080000000L,
                    1594080000000L
                  })
            });

    ProtoRows expected = MY_PROTO_ROWS;

    assertThat(converted.getSerializedRows(0).toByteArray())
        .isEqualTo(expected.getSerializedRows(0).toByteArray());
  }

  @Test
  public void testSettingARequiredFieldAsNull() throws Exception {
    logger.setLevel(Level.DEBUG);

    try {
      ProtoRows converted =
          toProtoRows(
              new StructType()
                  .add(new StructField("String", DataTypes.StringType, false, Metadata.empty())),
              new InternalRow[] {new GenericInternalRow(new Object[] {null})});
      fail("Convert did not assert field's /'Required/' status");
    } catch (Exception e) {
    }
    try {
      ProtoRows converted =
          toProtoRows(
              new StructType()
                  .add(new StructField("String", DataTypes.StringType, true, Metadata.empty())),
              new InternalRow[] {new GenericInternalRow(new Object[] {null})});
    } catch (Exception e) {
      fail("A nullable field could not be set to null.");
    }
  }

  public final StructType MY_STRUCT =
      DataTypes.createStructType(
          new StructField[] {
            new StructField("Number", DataTypes.IntegerType, true, Metadata.empty()),
            new StructField("String", DataTypes.StringType, true, Metadata.empty())
          });

  public final StructField SPARK_INTEGER_FIELD =
      new StructField("Number", DataTypes.IntegerType, true, Metadata.empty());
  public final StructField SPARK_STRING_FIELD =
      new StructField("String", DataTypes.StringType, false, Metadata.empty());
  public final StructField SPARK_NESTED_STRUCT_FIELD =
      new StructField("Struct", MY_STRUCT, true, Metadata.empty());
  public final StructField SPARK_ARRAY_FIELD =
      new StructField(
          "Array", DataTypes.createArrayType(DataTypes.IntegerType), true, Metadata.empty());
  public final StructField SPARK_DOUBLE_FIELD =
      new StructField("Float", DataTypes.DoubleType, true, Metadata.empty());
  public final StructField SPARK_BOOLEAN_FIELD =
      new StructField("Boolean", DataTypes.BooleanType, true, Metadata.empty());
  public final StructField SPARK_BINARY_FIELD =
      new StructField("Binary", DataTypes.BinaryType, true, Metadata.empty());
  public final StructField SPARK_DATE_FIELD =
      new StructField("Date", DataTypes.DateType, true, Metadata.empty());
  public final StructField SPARK_TIMESTAMP_FIELD =
      new StructField("TimeStamp", DataTypes.TimestampType, true, Metadata.empty());

  public final StructType BIG_SPARK_SCHEMA =
      new StructType()
          .add(SPARK_INTEGER_FIELD)
          .add(SPARK_STRING_FIELD)
          .add(SPARK_ARRAY_FIELD)
          .add(SPARK_NESTED_STRUCT_FIELD)
          .add(SPARK_DOUBLE_FIELD)
          .add(SPARK_BOOLEAN_FIELD)
          .add(SPARK_BINARY_FIELD)
          .add(SPARK_DATE_FIELD)
          .add(SPARK_TIMESTAMP_FIELD);

  public final Field BIGQUERY_INTEGER_FIELD =
      Field.newBuilder("Number", LegacySQLTypeName.INTEGER, (FieldList) null)
          .setMode(Field.Mode.NULLABLE)
          .build();
  public final Field BIGQUERY_STRING_FIELD =
      Field.newBuilder("String", LegacySQLTypeName.STRING, (FieldList) null)
          .setMode(Field.Mode.REQUIRED)
          .build();
  public final Field BIGQUERY_NESTED_STRUCT_FIELD =
      Field.newBuilder(
              "Struct",
              LegacySQLTypeName.RECORD,
              Field.newBuilder("Number", LegacySQLTypeName.INTEGER, (FieldList) null)
                  .setMode(Field.Mode.NULLABLE)
                  .build(),
              Field.newBuilder("String", LegacySQLTypeName.STRING, (FieldList) null)
                  .setMode(Field.Mode.NULLABLE)
                  .build())
          .setMode(Field.Mode.NULLABLE)
          .build();
  public final Field BIGQUERY_ARRAY_FIELD =
      Field.newBuilder("Array", LegacySQLTypeName.INTEGER, (FieldList) null)
          .setMode(Field.Mode.REPEATED)
          .build();
  public final Field BIGQUERY_FLOAT_FIELD =
      Field.newBuilder("Float", LegacySQLTypeName.FLOAT, (FieldList) null)
          .setMode(Field.Mode.NULLABLE)
          .build();
  public final Field BIGQUERY_BOOLEAN_FIELD =
      Field.newBuilder("Boolean", LegacySQLTypeName.BOOLEAN, (FieldList) null)
          .setMode(Field.Mode.NULLABLE)
          .build();
  public final Field BIGQUERY_BYTES_FIELD =
      Field.newBuilder("Binary", LegacySQLTypeName.BYTES, (FieldList) null)
          .setMode(Field.Mode.NULLABLE)
          .build();
  public final Field BIGQUERY_DATE_FIELD =
      Field.newBuilder("Date", LegacySQLTypeName.DATE, (FieldList) null)
          .setMode(Field.Mode.NULLABLE)
          .build();
  public final Field BIGQUERY_TIMESTAMP_FIELD =
      Field.newBuilder("TimeStamp", LegacySQLTypeName.TIMESTAMP, (FieldList) null)
          .setMode(Field.Mode.NULLABLE)
          .build();

  public final Schema BIG_BIGQUERY_SCHEMA =
      Schema.of(
          BIGQUERY_INTEGER_FIELD,
          BIGQUERY_STRING_FIELD,
          BIGQUERY_ARRAY_FIELD,
          BIGQUERY_NESTED_STRUCT_FIELD,
          BIGQUERY_FLOAT_FIELD,
          BIGQUERY_BOOLEAN_FIELD,
          BIGQUERY_BYTES_FIELD,
          BIGQUERY_DATE_FIELD,
          BIGQUERY_TIMESTAMP_FIELD);

  public final DescriptorProtos.FieldDescriptorProto.Builder PROTO_INTEGER_FIELD =
      DescriptorProtos.FieldDescriptorProto.newBuilder()
          .setName("Number")
          .setNumber(1)
          .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64)
          .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL);
  public final DescriptorProtos.FieldDescriptorProto.Builder PROTO_STRING_FIELD =
      DescriptorProtos.FieldDescriptorProto.newBuilder()
          .setName("String")
          .setNumber(1)
          .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)
          .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_REQUIRED);
  public final DescriptorProtos.FieldDescriptorProto.Builder PROTO_ARRAY_FIELD =
      DescriptorProtos.FieldDescriptorProto.newBuilder()
          .setName("Array")
          .setNumber(1)
          .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64)
          .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED);
  public final DescriptorProtos.DescriptorProto.Builder NESTED_STRUCT_DESCRIPTOR =
      DescriptorProtos.DescriptorProto.newBuilder()
          .setName("STRUCT4")
          .addField(PROTO_INTEGER_FIELD.clone())
          .addField(
              PROTO_STRING_FIELD
                  .clone()
                  .setNumber(2)
                  .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));
  public final DescriptorProtos.FieldDescriptorProto.Builder PROTO_STRUCT_FIELD =
      DescriptorProtos.FieldDescriptorProto.newBuilder()
          .setName("Struct")
          .setNumber(1)
          .setTypeName("STRUCT4")
          .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL);
  public final DescriptorProtos.FieldDescriptorProto.Builder PROTO_DOUBLE_FIELD =
      DescriptorProtos.FieldDescriptorProto.newBuilder()
          .setName("Double")
          .setNumber(1)
          .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_DOUBLE)
          .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL);
  public final DescriptorProtos.FieldDescriptorProto.Builder PROTO_BOOLEAN_FIELD =
      DescriptorProtos.FieldDescriptorProto.newBuilder()
          .setName("Boolean")
          .setNumber(1)
          .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_BOOL)
          .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL);
  public final DescriptorProtos.FieldDescriptorProto.Builder PROTO_BYTES_FIELD =
      DescriptorProtos.FieldDescriptorProto.newBuilder()
          .setName("Binary")
          .setNumber(1)
          .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_BYTES)
          .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL);

  public final DescriptorProtos.DescriptorProto DESCRIPTOR_PROTO_INTEGER =
      DescriptorProtos.DescriptorProto.newBuilder()
          .addField(PROTO_INTEGER_FIELD)
          .setName("Schema")
          .build();
  public final DescriptorProtos.DescriptorProto DESCRIPTOR_PROTO_STRING =
      DescriptorProtos.DescriptorProto.newBuilder()
          .addField(PROTO_STRING_FIELD)
          .setName("Schema")
          .build();
  public final DescriptorProtos.DescriptorProto DESCRIPTOR_PROTO_ARRAY =
      DescriptorProtos.DescriptorProto.newBuilder()
          .addField(PROTO_ARRAY_FIELD)
          .setName("Schema")
          .build();
  public final DescriptorProtos.DescriptorProto DESCRIPTOR_PROTO_STRUCT =
      DescriptorProtos.DescriptorProto.newBuilder()
          .addNestedType(NESTED_STRUCT_DESCRIPTOR)
          .addField(PROTO_STRUCT_FIELD)
          .setName("Schema")
          .build();

  public final InternalRow INTEGER_INTERNAL_ROW = new GenericInternalRow(new Object[] {1});
  public final InternalRow STRING_INTERNAL_ROW =
      new GenericInternalRow(new Object[] {UTF8String.fromString("A")});
  public final InternalRow ARRAY_INTERNAL_ROW =
      new GenericInternalRow(new Object[] {ArrayData.toArrayData(new int[] {0, 1, 2})});
  public final InternalRow INTERNAL_STRUCT_DATA =
      new GenericInternalRow(new Object[] {1, UTF8String.fromString("A")});
  public final InternalRow STRUCT_INTERNAL_ROW =
      new GenericInternalRow(new Object[] {INTERNAL_STRUCT_DATA});

  public Descriptors.Descriptor INTEGER_SCHEMA_DESCRIPTOR = createIntegerSchemaDescriptor();

  public Descriptors.Descriptor createIntegerSchemaDescriptor() {
    try {
      return toDescriptor(new StructType().add(SPARK_INTEGER_FIELD));
    } catch (Descriptors.DescriptorValidationException e) {
      throw new AssumptionViolatedException("Could not create INTEGER_SCHEMA_DESCRIPTOR", e);
    }
  }

  public Descriptors.Descriptor STRING_SCHEMA_DESCRIPTOR = createStringSchemaDescriptor();

  public Descriptors.Descriptor createStringSchemaDescriptor() {
    try {
      return toDescriptor(new StructType().add(SPARK_STRING_FIELD));
    } catch (Descriptors.DescriptorValidationException e) {
      throw new AssumptionViolatedException("Could not create STRING_SCHEMA_DESCRIPTOR", e);
    }
  }

  public Descriptors.Descriptor ARRAY_SCHEMA_DESCRIPTOR = createArraySchemaDescriptor();

  public Descriptors.Descriptor createArraySchemaDescriptor() {
    try {
      return toDescriptor(new StructType().add(SPARK_ARRAY_FIELD));
    } catch (Descriptors.DescriptorValidationException e) {
      throw new AssumptionViolatedException("Could not create ARRAY_SCHEMA_DESCRIPTOR", e);
    }
  }

  public Descriptors.Descriptor STRUCT_SCHEMA_DESCRIPTOR = createStructSchemaDescriptor();

  public Descriptors.Descriptor createStructSchemaDescriptor() {
    try {
      return toDescriptor(new StructType().add(SPARK_NESTED_STRUCT_FIELD));
    } catch (Descriptors.DescriptorValidationException e) {
      throw new AssumptionViolatedException("Could not create STRUCT_SCHEMA_DESCRIPTOR", e);
    }
  }

  Descriptors.Descriptor STRUCT_DESCRIPTOR = createStructDescriptor();

  public Descriptors.Descriptor createStructDescriptor() throws AssumptionViolatedException {
    try {
      return toDescriptor(MY_STRUCT);
    } catch (Descriptors.DescriptorValidationException e) {
      throw new AssumptionViolatedException("Could not create STRUCT_DESCRIPTOR.", e);
    }
  }

  public final DynamicMessage INTEGER_ROW_MESSAGE =
      DynamicMessage.newBuilder(INTEGER_SCHEMA_DESCRIPTOR)
          .setField(INTEGER_SCHEMA_DESCRIPTOR.findFieldByNumber(1), 1L)
          .build();
  public final DynamicMessage STRING_ROW_MESSAGE =
      DynamicMessage.newBuilder(STRING_SCHEMA_DESCRIPTOR)
          .setField(STRING_SCHEMA_DESCRIPTOR.findFieldByNumber(1), "A")
          .build();
  public final DynamicMessage ARRAY_ROW_MESSAGE =
      DynamicMessage.newBuilder(ARRAY_SCHEMA_DESCRIPTOR)
          .addRepeatedField(ARRAY_SCHEMA_DESCRIPTOR.findFieldByNumber(1), 0L)
          .addRepeatedField(ARRAY_SCHEMA_DESCRIPTOR.findFieldByNumber(1), 1L)
          .addRepeatedField(ARRAY_SCHEMA_DESCRIPTOR.findFieldByNumber(1), 2L)
          .build();
  public DynamicMessage StructRowMessage =
      DynamicMessage.newBuilder(STRUCT_SCHEMA_DESCRIPTOR)
          .setField(
              STRUCT_SCHEMA_DESCRIPTOR.findFieldByNumber(1),
              buildSingleRowMessage(MY_STRUCT, STRUCT_DESCRIPTOR, INTERNAL_STRUCT_DATA))
          .build();

  public Descriptors.Descriptor BIG_SCHEMA_ROW_DESCRIPTOR = createBigSchemaRowDescriptor();

  public Descriptors.Descriptor createBigSchemaRowDescriptor() {
    try {
      return toDescriptor(BIG_SPARK_SCHEMA);
    } catch (Descriptors.DescriptorValidationException e) {
      throw new AssumptionViolatedException("Could not create BIG_SCHEMA_ROW_DESCRIPTOR", e);
    }
  }

  public ProtoRows MY_PROTO_ROWS =
      ProtoRows.newBuilder()
          .addSerializedRows(
              DynamicMessage.newBuilder(BIG_SCHEMA_ROW_DESCRIPTOR)
                  .setField(BIG_SCHEMA_ROW_DESCRIPTOR.findFieldByNumber(1), 1L)
                  .setField(BIG_SCHEMA_ROW_DESCRIPTOR.findFieldByNumber(2), "A")
                  .addRepeatedField(BIG_SCHEMA_ROW_DESCRIPTOR.findFieldByNumber(3), 0L)
                  .addRepeatedField(BIG_SCHEMA_ROW_DESCRIPTOR.findFieldByNumber(3), 1L)
                  .addRepeatedField(BIG_SCHEMA_ROW_DESCRIPTOR.findFieldByNumber(3), 2L)
                  .setField(
                      BIG_SCHEMA_ROW_DESCRIPTOR.findFieldByNumber(4),
                      buildSingleRowMessage(MY_STRUCT, STRUCT_DESCRIPTOR, INTERNAL_STRUCT_DATA))
                  .setField(BIG_SCHEMA_ROW_DESCRIPTOR.findFieldByNumber(5), 3.14)
                  .setField(BIG_SCHEMA_ROW_DESCRIPTOR.findFieldByNumber(6), true)
                  .setField(BIG_SCHEMA_ROW_DESCRIPTOR.findFieldByNumber(7), new byte[] {11, 0x7F})
                  .setField(BIG_SCHEMA_ROW_DESCRIPTOR.findFieldByNumber(8), 1594080000000L)
                  .setField(BIG_SCHEMA_ROW_DESCRIPTOR.findFieldByNumber(9), 1594080000000L)
                  .build()
                  .toByteString())
          .build();
}
