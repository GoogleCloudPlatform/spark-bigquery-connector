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
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TimePartitioning;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.ml.linalg.SQLDataTypes;
import org.apache.spark.sql.types.*;
import org.junit.Test;

import java.util.Optional;

import static com.google.cloud.spark.bigquery.SchemaConverters.*;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.fail;

public class SchemaConverterTest {

  // Numeric is a fixed precision Decimal Type with 38 digits of precision and 9 digits of scale.
  // See https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#numeric-type
  private static final int BQ_NUMERIC_PRECISION = 38;
  private static final int BQ_NUMERIC_SCALE = 9;
  private static final DecimalType NUMERIC_SPARK_TYPE =
      DataTypes.createDecimalType(BQ_NUMERIC_PRECISION, BQ_NUMERIC_SCALE);
  // The maximum nesting depth of a BigQuery RECORD:
  private static final int MAX_BIGQUERY_NESTED_DEPTH = 15;

  private final Logger logger = LogManager.getLogger("com.google.cloud.spark");

  /*
  BigQuery -> Spark tests, translated from SchemaConvertersSuite.scala
   */
  @Test
  public void testEmptySchemaBigQueryToSparkConversion() throws Exception {
    Schema bqSchema = Schema.of();
    StructType expected = new StructType();
    StructType result = SchemaConverters.toSpark(bqSchema);
    assertThat(result).isEqualTo(expected);
  }

  @Test
  public void testSingleFieldSchemaBigQueryToSparkConversion() throws Exception {
    Schema bqSchema = Schema.of(Field.of("foo", LegacySQLTypeName.STRING));
    StructType expected =
        new StructType().add(new StructField("foo", DataTypes.StringType, true, Metadata.empty()));
    StructType result = SchemaConverters.toSpark(bqSchema);
    assertThat(result).isEqualTo(expected);
  }

  @Test
  public void testFullFieldSchemaBigQueryToSparkConversion() throws Exception {
    Schema bqSchema = BIG_BIGQUERY_SCHEMA2;

    StructType expected = BIG_SPARK_SCHEMA2;

    StructType result = SchemaConverters.toSpark(bqSchema);
    assertThat(result).isEqualTo(expected);
  }

  @Test
  public void testFieldHasDescriptionBigQueryToSpark() throws Exception {
    Schema bqSchema =
        Schema.of(
            Field.newBuilder("name", LegacySQLTypeName.STRING)
                .setDescription("foo")
                .setMode(Field.Mode.NULLABLE)
                .build());
    StructType expected =
        new StructType()
            .add(
                new StructField(
                    "name",
                    DataTypes.StringType,
                    true,
                    new MetadataBuilder()
                        .putString("description", "foo")
                        .putString("comment", "foo")
                        .build()));

    StructType result = SchemaConverters.toSpark(bqSchema);
    assertThat(result).isEqualTo(expected);
  }

  @Test
  public void testGetSchemaWithPseudoColumns() throws Exception {
    Schema result =
        SchemaConverters.getSchemaWithPseudoColumns(buildTableInfo(BIG_BIGQUERY_SCHEMA2, null));
    assertThat(result).isEqualTo(BIG_BIGQUERY_SCHEMA2);

    result =
        SchemaConverters.getSchemaWithPseudoColumns(
            buildTableInfo(
                BIG_BIGQUERY_SCHEMA2,
                TimePartitioning.newBuilder(TimePartitioning.Type.DAY).setField("foo").build()));
    assertThat(result).isEqualTo(BIG_BIGQUERY_SCHEMA2);

    result =
        SchemaConverters.getSchemaWithPseudoColumns(
            buildTableInfo(BIG_BIGQUERY_SCHEMA2, TimePartitioning.of(TimePartitioning.Type.DAY)));
    assertThat(result).isEqualTo(BIG_BIGQUERY_SCHEMA2_WITH_PSEUDO_COLUMNS);
  }

  public TableInfo buildTableInfo(Schema schema, TimePartitioning timePartitioning) {
    return TableInfo.of(
        TableId.of("project", "dataset", "table"),
        StandardTableDefinition.newBuilder()
            .setSchema(schema)
            .setTimePartitioning(timePartitioning)
            .build());
  }

  /*
  Spark -> BigQuery conversion tests:
   */
  @Test
  public void testSparkToBQSchema() throws Exception {
    StructType schema = BIG_SPARK_SCHEMA;
    Schema expected = BIG_BIGQUERY_SCHEMA;

    Schema converted = toBigQuerySchema(schema);

    for (int i = 0; i < expected.getFields().size(); i++) {
      if (i == 8) continue; // FIXME: delete this line when Timestamp conversion can be restored.
      assertThat(converted.getFields().get(i)).isEqualTo(expected.getFields().get(i));
    }
  }

  @Test
  public void testSparkMapException() throws Exception {
    try {
      createBigQueryColumn(SPARK_MAP_FIELD, 0);
      fail("Did not throw an error for an unsupported map-type");
    } catch (IllegalArgumentException e) {
    }
  }

  @Test
  public void testDecimalTypeConversion() throws Exception {
    assertThat(toBigQueryType(NUMERIC_SPARK_TYPE)).isEqualTo(LegacySQLTypeName.NUMERIC);

    try {
      DecimalType wayTooBig = DataTypes.createDecimalType(38, 38);
      toBigQueryType(wayTooBig);
      fail("Did not throw an error for a decimal that's too wide for big-query");
    } catch (IllegalArgumentException e) {
    }
  }

  @Test
  public void testTimeTypesConversions() throws Exception {
    // FIXME: restore this check when the Vortex team adds microsecond precision, and Timestamp
    // conversion can be fixed.
    // assertThat(toBigQueryType(DataTypes.TimestampType)).isEqualTo(LegacySQLTypeName.TIMESTAMP);
    assertThat(toBigQueryType(DataTypes.DateType)).isEqualTo(LegacySQLTypeName.DATE);
  }

  @Test
  public void testDescriptionConversion() throws Exception {
    String description = "I love bananas";
    Field result =
        createBigQueryColumn(
            new StructField(
                "Field",
                DataTypes.IntegerType,
                true,
                new MetadataBuilder().putString("description", description).build()),
            0);

    assertThat(result.getDescription().equals(description));
  }

  @Test
  public void testMaximumNestingDepthError() throws Exception {
    StructType inner = new StructType();
    StructType superRecursiveSchema = inner;
    for (int i = 0; i < MAX_BIGQUERY_NESTED_DEPTH + 1; i++) {
      StructType outer =
          new StructType()
              .add(new StructField("struct" + i, superRecursiveSchema, true, Metadata.empty()));
      superRecursiveSchema = outer;
    }

    try {
      createBigQueryColumn(superRecursiveSchema.fields()[0], 0);
      fail("Did not detect super-recursive schema of depth = 16.");
    } catch (IllegalArgumentException e) {
    }
  }

  @Test
  public void testGetCustomDataType() {
    Field.Builder field =
        Field.newBuilder(
            "test", LegacySQLTypeName.RECORD, Field.of("sub", LegacySQLTypeName.INTEGER));
    // no description
    assertThat(SchemaConverters.getCustomDataType(field.build()).isPresent()).isFalse();
    // empty marker
    assertThat(SchemaConverters.getCustomDataType(field.setDescription("foo").build()).isPresent())
        .isFalse();
    // only marker
    assertThat(
            SchemaConverters.getCustomDataType(field.setDescription("{spark.type=vector}").build()))
        .isEqualTo(Optional.of(SQLDataTypes.VectorType()));
    // description and marker
    assertThat(
            SchemaConverters.getCustomDataType(
                field.setDescription("foo {spark.type=matrix}").build()))
        .isEqualTo(Optional.of(SQLDataTypes.MatrixType()));
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
  public final StructField SPARK_MAP_FIELD =
      new StructField(
          "Map",
          DataTypes.createMapType(DataTypes.IntegerType, DataTypes.StringType),
          true,
          Metadata.empty());

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

  public final StructType BIG_SPARK_SCHEMA2 =
      new StructType()
          .add(new StructField("foo", DataTypes.StringType, true, Metadata.empty()))
          .add(new StructField("bar", DataTypes.LongType, true, Metadata.empty()))
          .add(new StructField("required", DataTypes.BooleanType, false, Metadata.empty()))
          .add(
              new StructField(
                  "binary_arr",
                  DataTypes.createArrayType(DataTypes.BinaryType, true),
                  true,
                  Metadata.empty()))
          .add(new StructField("float", DataTypes.DoubleType, true, Metadata.empty()))
          .add(
              new StructField(
                  "numeric", DataTypes.createDecimalType(38, 9), true, Metadata.empty()))
          .add(new StructField("date", DataTypes.DateType, true, Metadata.empty()))
          .add(
              new StructField(
                  "times",
                  new StructType()
                      .add(new StructField("time", DataTypes.LongType, true, Metadata.empty()))
                      .add(
                          new StructField(
                              "timestamp", DataTypes.TimestampType, true, Metadata.empty()))
                      .add(
                          new StructField(
                              "datetime", DataTypes.StringType, true, Metadata.empty())),
                  true,
                  Metadata.empty()));

  public final Schema BIG_BIGQUERY_SCHEMA2 =
      Schema.of(
          Field.of("foo", LegacySQLTypeName.STRING),
          Field.of("bar", LegacySQLTypeName.INTEGER),
          Field.newBuilder("required", LegacySQLTypeName.BOOLEAN)
              .setMode(Field.Mode.REQUIRED)
              .build(),
          Field.newBuilder("binary_arr", LegacySQLTypeName.BYTES)
              .setMode(Field.Mode.REPEATED)
              .build(),
          Field.of("float", LegacySQLTypeName.FLOAT),
          Field.of("numeric", LegacySQLTypeName.NUMERIC),
          Field.of("date", LegacySQLTypeName.DATE),
          Field.of(
              "times",
              LegacySQLTypeName.RECORD,
              Field.of("time", LegacySQLTypeName.TIME),
              Field.of("timestamp", LegacySQLTypeName.TIMESTAMP),
              Field.of("datetime", LegacySQLTypeName.DATETIME)));

  public final Schema BIG_BIGQUERY_SCHEMA2_WITH_PSEUDO_COLUMNS =
      Schema.of(
          Field.of("foo", LegacySQLTypeName.STRING),
          Field.of("bar", LegacySQLTypeName.INTEGER),
          Field.newBuilder("required", LegacySQLTypeName.BOOLEAN)
              .setMode(Field.Mode.REQUIRED)
              .build(),
          Field.newBuilder("binary_arr", LegacySQLTypeName.BYTES)
              .setMode(Field.Mode.REPEATED)
              .build(),
          Field.of("float", LegacySQLTypeName.FLOAT),
          Field.of("numeric", LegacySQLTypeName.NUMERIC),
          Field.of("date", LegacySQLTypeName.DATE),
          Field.of(
              "times",
              LegacySQLTypeName.RECORD,
              Field.of("time", LegacySQLTypeName.TIME),
              Field.of("timestamp", LegacySQLTypeName.TIMESTAMP),
              Field.of("datetime", LegacySQLTypeName.DATETIME)),
          Field.newBuilder("_PARTITIONTIME", LegacySQLTypeName.TIMESTAMP)
              .setMode(Field.Mode.NULLABLE)
              .build(),
          Field.newBuilder("_PARTITIONDATE", LegacySQLTypeName.DATE)
              .setMode(Field.Mode.NULLABLE)
              .build());

  /* TODO: translate BigQuery to Spark row conversion tests, from SchemaIteratorSuite.scala
  private final List<String> BIG_SCHEMA_NAMES_INORDER = Arrays.asList(
          new String[]{"Number", "String", "Array", "Struct", "Float", "Boolean", "Numeric"});

  private final org.apache.avro.Schema AVRO_SCHEMA = createAvroSchema();
  private final org.apache.avro.Schema createAvroSchema() throws AssumptionViolatedException {
      try {
          org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().
                  parse(this.getClass().getResourceAsStream("/alltypes.avroschema.json"));
          return avroSchema;
      } catch (IOException e) {
          throw new AssumptionViolatedException("Could not create AVRO_SCHEMA", e);
      }
  }
   */
}
