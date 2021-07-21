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
package com.google.cloud.spark.bigquery


import com.google.cloud.bigquery.Field.Mode
import com.google.cloud.bigquery.LegacySQLTypeName._
import com.google.cloud.bigquery.{Field, Schema}
import org.apache.spark.sql.types._

class SchemaConvertersSuite extends org.scalatest.FunSuite {

  test("empty schema conversion") {
    val bqSchema = Schema.of()
    val expected = StructType(Seq())
    val result = SchemaConverters.toSpark(bqSchema)
    assert(expected == result)
  }

  test("single field schema conversion") {
    val bqSchema = Schema.of(Field.of("foo", STRING))
    val expected = StructType(Seq(StructField("foo", StringType)))
    val result = SchemaConverters.toSpark(bqSchema)
    assert(expected == result)
  }

  test("full field schema conversion") {
    val bqSchema = Schema.of(
      Field.of("foo", STRING),
      Field.of("bar", INTEGER),
      Field.newBuilder("required", BOOLEAN).setMode(Mode.REQUIRED).build(),
      Field.newBuilder("binary_arr", BYTES).setMode(Mode.REPEATED).build(),
      Field.of("float", FLOAT),
      Field.of("numeric", NUMERIC),
      Field.of("date", DATE),
      Field.of("times", RECORD,
        Field.of("time", TIME),
        Field.of("timestamp", TIMESTAMP),
        Field.of("datetime", DATETIME)))

    val expected = StructType(Seq(
      StructField("foo", StringType),
      StructField("bar", LongType),
      StructField("required", BooleanType, nullable = false),
      StructField("binary_arr", ArrayType(BinaryType, containsNull = true)),
      StructField("float", DoubleType),
      StructField("numeric", DataTypes.createDecimalType(38, 9)),
      StructField("date", DateType),
      StructField("times", StructType(Seq(
        StructField("time", LongType),
        StructField("timestamp", TimestampType),
        StructField("datetime", StringType))))))

    val result = SchemaConverters.toSpark(bqSchema)
    assert(expected == result)
  }

  test("field has description") {
    val bqSchema = Schema.of(
      Field.newBuilder("name", STRING)
        .setDescription("foo")
        .setMode(Mode.NULLABLE)
        .build)
    val expected = StructType(Seq(
      StructField("name", StringType, true,
        new MetadataBuilder()
          .putString("description", "foo")
          .putString("comment", "foo")
          .build)
    ))
    val result = SchemaConverters.toSpark(bqSchema)
    assert(expected == result)
  }
}

