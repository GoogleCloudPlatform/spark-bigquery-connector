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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.types.*;
import org.apache.spark.unsafe.types.UTF8String;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class ProtobufUtils {

  static final Logger logger = LoggerFactory.getLogger(ProtobufUtils.class);

  // The maximum nesting depth of a BigQuery RECORD:
  private static final int MAX_BIGQUERY_NESTED_DEPTH = 15;
  // For every message, a nested type is name "STRUCT"+i, where i is the
  // number of the corresponding field that is of this type in the containing message.
  private static final String RESERVED_NESTED_TYPE_NAME = "STRUCT";
  private static final String MAPTYPE_ERROR_MESSAGE = "MapType is unsupported.";

  /** BigQuery Schema ==> ProtoSchema converter utils: */
  public static ProtoSchema toProtoSchema(Schema schema) throws IllegalArgumentException {
    try {
      Descriptors.Descriptor descriptor = toDescriptor(schema);
      ProtoSchema protoSchema = ProtoSchemaConverter.convert(descriptor);
      return protoSchema;
    } catch (Descriptors.DescriptorValidationException e) {
      throw new IllegalArgumentException("Could not build Proto-Schema from Spark schema.", e);
    }
  }

  private static Descriptors.Descriptor toDescriptor(Schema schema)
      throws Descriptors.DescriptorValidationException {
    DescriptorProtos.DescriptorProto.Builder descriptorBuilder =
        DescriptorProtos.DescriptorProto.newBuilder().setName("Schema");

    FieldList fields = schema.getFields();

    int initialDepth = 0;
    DescriptorProtos.DescriptorProto descriptorProto =
        buildDescriptorProtoWithFields(descriptorBuilder, fields, initialDepth);

    return createDescriptorFromProto(descriptorProto);
  }

  private static Descriptors.Descriptor createDescriptorFromProto(
      DescriptorProtos.DescriptorProto descriptorProto)
      throws Descriptors.DescriptorValidationException {
    DescriptorProtos.FileDescriptorProto fileDescriptorProto =
        DescriptorProtos.FileDescriptorProto.newBuilder().addMessageType(descriptorProto).build();

    Descriptors.Descriptor descriptor =
        Descriptors.FileDescriptor.buildFrom(
                fileDescriptorProto, new Descriptors.FileDescriptor[] {})
            .getMessageTypes()
            .get(0);

    return descriptor;
  }

  @VisibleForTesting
  protected static DescriptorProtos.DescriptorProto buildDescriptorProtoWithFields(
      DescriptorProtos.DescriptorProto.Builder descriptorBuilder, FieldList fields, int depth) {
    Preconditions.checkArgument(
        depth < MAX_BIGQUERY_NESTED_DEPTH,
        "Tried to convert a BigQuery schema that exceeded BigQuery maximum nesting depth");
    int messageNumber = 1;
    for (Field field : fields) {
      String fieldName = field.getName();
      DescriptorProtos.FieldDescriptorProto.Label fieldLabel = toProtoFieldLabel(field.getMode());
      FieldList subFields = field.getSubFields();

      if (field.getType() == LegacySQLTypeName.RECORD) {
        String recordTypeName =
            RESERVED_NESTED_TYPE_NAME
                + messageNumber; // TODO: Change or assert this to be a reserved name. No column can
        // have this name.
        DescriptorProtos.DescriptorProto.Builder nestedFieldTypeBuilder =
            descriptorBuilder.addNestedTypeBuilder();
        nestedFieldTypeBuilder.setName(recordTypeName);
        DescriptorProtos.DescriptorProto nestedFieldType =
            buildDescriptorProtoWithFields(nestedFieldTypeBuilder, subFields, depth + 1);

        descriptorBuilder.addField(
            createProtoFieldBuilder(fieldName, fieldLabel, messageNumber)
                .setTypeName(recordTypeName));
      } else {
        DescriptorProtos.FieldDescriptorProto.Type fieldType = toProtoFieldType(field.getType());
        descriptorBuilder.addField(
            createProtoFieldBuilder(fieldName, fieldLabel, messageNumber, fieldType));
      }
      messageNumber++;
    }
    return descriptorBuilder.build();
  }

  private static DescriptorProtos.FieldDescriptorProto.Builder createProtoFieldBuilder(
      String fieldName, DescriptorProtos.FieldDescriptorProto.Label fieldLabel, int messageNumber) {
    return DescriptorProtos.FieldDescriptorProto.newBuilder()
        .setName(fieldName)
        .setLabel(fieldLabel)
        .setNumber(messageNumber);
  }

  @VisibleForTesting
  protected static DescriptorProtos.FieldDescriptorProto.Builder createProtoFieldBuilder(
      String fieldName,
      DescriptorProtos.FieldDescriptorProto.Label fieldLabel,
      int messageNumber,
      DescriptorProtos.FieldDescriptorProto.Type fieldType) {
    return createProtoFieldBuilder(fieldName, fieldLabel, messageNumber).setType(fieldType);
  }

  private static DescriptorProtos.FieldDescriptorProto.Label toProtoFieldLabel(Field.Mode mode) {
    switch (mode) {
      case NULLABLE:
        return DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL;
      case REPEATED:
        return DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED;
      case REQUIRED:
        return DescriptorProtos.FieldDescriptorProto.Label.LABEL_REQUIRED;
      default:
        throw new IllegalArgumentException("A BigQuery Field Mode was invalid: " + mode.name());
    }
  }

  // NOTE: annotations for DATETIME and TIMESTAMP objects are currently unsupported for external
  // users,
  // but if they become available, it would be advisable to append an annotation to the
  // protoFieldBuilder
  // for these and other types.
  private static DescriptorProtos.FieldDescriptorProto.Type toProtoFieldType(
      LegacySQLTypeName bqType) {
    DescriptorProtos.FieldDescriptorProto.Type protoFieldType;
    if (LegacySQLTypeName.INTEGER.equals(bqType)
        || LegacySQLTypeName.DATE.equals(bqType)
        || LegacySQLTypeName.DATETIME.equals(bqType)
        || LegacySQLTypeName.TIMESTAMP.equals(bqType)) {
      return DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64;
    }
    if (LegacySQLTypeName.BOOLEAN.equals(bqType)) {
      return DescriptorProtos.FieldDescriptorProto.Type.TYPE_BOOL;
    }
    if (LegacySQLTypeName.STRING.equals(bqType)) {
      return DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING;
    }
    if (LegacySQLTypeName.GEOGRAPHY.equals(bqType)
        || LegacySQLTypeName.BYTES.equals(bqType)
        || LegacySQLTypeName.NUMERIC.equals(bqType)) {
      return DescriptorProtos.FieldDescriptorProto.Type.TYPE_BYTES;
    }
    if (LegacySQLTypeName.FLOAT.equals(bqType)) {
      return DescriptorProtos.FieldDescriptorProto.Type.TYPE_DOUBLE;
    }

    if (LegacySQLTypeName.RECORD.equals(bqType)) {
      throw new IllegalStateException(
          "Program attempted to return an atomic data-type for a RECORD");
    }

    throw new IllegalArgumentException("Unexpected type: " + bqType.name());
  }

  /**
   * Spark Row --> ProtoRows converter utils: To be used by the DataWriters facing the BigQuery
   * Storage Write API
   */
  public static ProtoRows toProtoRows(StructType sparkSchema, InternalRow[] rows) {
    try {
      Descriptors.Descriptor schemaDescriptor = toDescriptor(sparkSchema);
      ProtoRows.Builder protoRows = ProtoRows.newBuilder();
      for (InternalRow row : rows) {
        DynamicMessage rowMessage = buildSingleRowMessage(sparkSchema, schemaDescriptor, row);
        protoRows.addSerializedRows(rowMessage.toByteString());
      }
      return protoRows.build();
    } catch (Exception e) {
      throw new RuntimeException("Could not convert Internal Rows to Proto Rows.", e);
    }
  }

  public static DynamicMessage buildSingleRowMessage(
      StructType schema, Descriptors.Descriptor schemaDescriptor, InternalRow row) {
    DynamicMessage.Builder messageBuilder = DynamicMessage.newBuilder(schemaDescriptor);

    for (int fieldIndex = 0; fieldIndex < schemaDescriptor.getFields().size(); fieldIndex++) {
      int protoFieldNumber = fieldIndex + 1;

      StructField sparkField = schema.fields()[fieldIndex];
      DataType sparkType = sparkField.dataType();

      Object sparkValue = row.get(fieldIndex, sparkType);
      boolean nullable = sparkField.nullable();
      Descriptors.Descriptor nestedTypeDescriptor =
          schemaDescriptor.findNestedTypeByName(RESERVED_NESTED_TYPE_NAME + (protoFieldNumber));
      Object protoValue =
          convertToProtoRowValue(sparkType, sparkValue, nullable, nestedTypeDescriptor);

      logger.debug("Converted value {} to proto-value: {}", sparkValue, protoValue);

      if (protoValue == null) {
        continue;
      }

      messageBuilder.setField(schemaDescriptor.findFieldByNumber(protoFieldNumber), protoValue);
    }

    return messageBuilder.build();
  }

  public static Descriptors.Descriptor toDescriptor(StructType schema)
      throws Descriptors.DescriptorValidationException {
    DescriptorProtos.DescriptorProto.Builder descriptorBuilder =
        DescriptorProtos.DescriptorProto.newBuilder().setName("Schema");

    int initialDepth = 0;
    DescriptorProtos.DescriptorProto descriptorProto =
        buildDescriptorProtoWithFields(descriptorBuilder, schema.fields(), initialDepth);

    return createDescriptorFromProto(descriptorProto);
  }

  /*
  Takes a value in Spark format and converts it into ProtoRows format (to eventually be given to BigQuery).
   */
  private static Object convertToProtoRowValue(
      DataType sparkType,
      Object sparkValue,
      boolean nullable,
      Descriptors.Descriptor nestedTypeDescriptor) {
    logger.debug("Converting type: {}", sparkType.json());
    if (sparkValue == null) {
      if (!nullable) {
        throw new IllegalArgumentException("Non-nullable field was null.");
      } else {
        return null;
      }
    }

    if (sparkType instanceof ArrayType) {
      ArrayType arrayType = (ArrayType) sparkType;
      DataType elementType = arrayType.elementType();
      Object[] sparkArrayData = ((ArrayData) sparkValue).toObjectArray(elementType);
      boolean containsNull = arrayType.containsNull();
      List<Object> protoValue = new ArrayList<>();
      for (Object sparkElement : sparkArrayData) {
        Object converted =
            convertToProtoRowValue(elementType, sparkElement, containsNull, nestedTypeDescriptor);
        if (converted == null) {
          continue;
        }
        protoValue.add(converted);
      }
      return protoValue;
    }

    if (sparkType instanceof StructType) {
      return buildSingleRowMessage(
          (StructType) sparkType, nestedTypeDescriptor, (InternalRow) sparkValue);
    }

    if (sparkType instanceof ByteType
        || sparkType instanceof ShortType
        || sparkType instanceof IntegerType
        || sparkType instanceof LongType
        || sparkType instanceof TimestampType
        || sparkType instanceof DateType) {
      return ((Number) sparkValue).longValue();
    } // TODO: CalendarInterval

    if (sparkType instanceof FloatType || sparkType instanceof DoubleType) {
      return ((Number) sparkValue).doubleValue();
    }

    if (sparkType instanceof DecimalType) {
      return ((Decimal) sparkValue).toDouble();
    }

    if (sparkType instanceof BooleanType || sparkType instanceof BinaryType) {
      return sparkValue;
    }

    if (sparkType instanceof StringType) {
      return new String(((UTF8String) sparkValue).getBytes());
    }

    if (sparkType instanceof MapType) {
      throw new IllegalArgumentException(MAPTYPE_ERROR_MESSAGE);
    }

    throw new IllegalStateException("Unexpected type: " + sparkType);
  }

  private static DescriptorProtos.DescriptorProto buildDescriptorProtoWithFields(
      DescriptorProtos.DescriptorProto.Builder descriptorBuilder, StructField[] fields, int depth) {
    Preconditions.checkArgument(
        depth < MAX_BIGQUERY_NESTED_DEPTH, "Spark Schema exceeds BigQuery maximum nesting depth.");
    int messageNumber = 1;
    for (StructField field : fields) {
      String fieldName = field.name();
      DescriptorProtos.FieldDescriptorProto.Label fieldLabel =
          field.nullable()
              ? DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL
              : DescriptorProtos.FieldDescriptorProto.Label.LABEL_REQUIRED;

      DataType sparkType = field.dataType();

      if (sparkType instanceof ArrayType) {
        ArrayType arrayType = (ArrayType) sparkType;
        /* DescriptorProtos.FieldDescriptorProto.Label elementLabel = arrayType.containsNull() ?
        DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL :
        DescriptorProtos.FieldDescriptorProto.Label.LABEL_REQUIRED; TODO: how to support null instances inside an array (repeated field) in BigQuery?*/
        sparkType = arrayType.elementType();
        fieldLabel = DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED;
      }

      DescriptorProtos.FieldDescriptorProto.Builder protoFieldBuilder;
      if (sparkType instanceof StructType) {
        StructType structType = (StructType) sparkType;
        String nestedName =
            RESERVED_NESTED_TYPE_NAME
                + messageNumber; // TODO: this should be a reserved name. No column can have this
        // name.
        StructField[] subFields = structType.fields();

        DescriptorProtos.DescriptorProto.Builder nestedFieldTypeBuilder =
            descriptorBuilder.addNestedTypeBuilder().setName(nestedName);
        buildDescriptorProtoWithFields(nestedFieldTypeBuilder, subFields, depth + 1);

        protoFieldBuilder =
            createProtoFieldBuilder(fieldName, fieldLabel, messageNumber).setTypeName(nestedName);
      } else {
        DescriptorProtos.FieldDescriptorProto.Type fieldType =
            sparkAtomicTypeToProtoFieldType(sparkType);
        protoFieldBuilder =
            createProtoFieldBuilder(fieldName, fieldLabel, messageNumber, fieldType);
      }
      descriptorBuilder.addField(protoFieldBuilder);
      messageNumber++;
    }
    return descriptorBuilder.build();
  }

  // NOTE: annotations for DATETIME and TIMESTAMP objects are currently unsupported for external
  // users,
  // but if they become available, it would be advisable to append an annotation to the
  // protoFieldBuilder
  // for these and other types.
  // This function only converts atomic Spark DataTypes
  private static DescriptorProtos.FieldDescriptorProto.Type sparkAtomicTypeToProtoFieldType(
      DataType sparkType) {
    if (sparkType instanceof ByteType
        || sparkType instanceof ShortType
        || sparkType instanceof IntegerType
        || sparkType instanceof LongType
        || sparkType instanceof TimestampType
        || sparkType instanceof DateType) {
      return DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64;
    }

    if (sparkType instanceof FloatType
        || sparkType instanceof DoubleType
        || sparkType instanceof DecimalType) {
      return DescriptorProtos.FieldDescriptorProto.Type.TYPE_DOUBLE;
      /* TODO: an annotation to distinguish between decimals that are doubles, and decimals that are
      NUMERIC (Bytes types) */
    }

    if (sparkType instanceof BooleanType) {
      return DescriptorProtos.FieldDescriptorProto.Type.TYPE_BOOL;
    }

    if (sparkType instanceof BinaryType) {
      return DescriptorProtos.FieldDescriptorProto.Type.TYPE_BYTES;
    }

    if (sparkType instanceof StringType) {
      return DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING;
    }

    if (sparkType instanceof MapType) {
      throw new IllegalArgumentException(MAPTYPE_ERROR_MESSAGE);
    }

    throw new IllegalStateException("Unexpected type: " + sparkType);
  }
}
