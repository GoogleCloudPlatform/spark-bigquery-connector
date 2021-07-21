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

import java.util.Optional
import com.google.cloud.bigquery.Field.Mode
import com.google.cloud.bigquery.LegacySQLTypeName.{BOOLEAN, BYTES, DATE, DATETIME, FLOAT, INTEGER, NUMERIC, RECORD, STRING, TIME, TIMESTAMP}
import com.google.cloud.bigquery.{Field, Schema}
import com.google.common.io.ByteStreams.toByteArray
import com.google.protobuf.ByteString
import org.apache.avro.{Schema => AvroSchema}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{ArrayType, BinaryType}
import org.apache.spark.sql.types._
import org.scalatest.FunSuite

import scala.collection.JavaConverters._

/**
 * A test for ensuring that Arrow and Avros Schema generate same results for
 * underlying BigQuery source
 */
class SchemaIteratorSuite extends FunSuite {

  test("compare arrow and avro results") {
    // rows in the form of bytes string in both arrow and avro format
    val avroByteString = ByteString.copyFrom(
      toByteArray(getClass.getResourceAsStream("/alltypes.avro")))
    val arrowByteString = ByteString.copyFrom(
      toByteArray(getClass.getResourceAsStream("/alltypes.arrow")))

    // avro and arrow schemas required to read rows from bigquery
    val arrowSchema = ByteString.copyFrom(toByteArray(getClass.getResourceAsStream("/alltypes.arrowschema")))
    val avroSchema = new AvroSchema.Parser().
      parse(getClass.getResourceAsStream("/alltypes.avroschema.json"))

    val columnsInOrder = Seq("int_req", "int_null", "bl", "str", "day", "ts", "dt", "tm", "binary",
      "float", "nums", "int_arr", "int_struct_arr")

    val bqSchema = Schema.of(
      Field.newBuilder("int_req", INTEGER).setMode(Mode.REQUIRED).build(),
      Field.of("int_null", INTEGER),
      Field.of("bl", BOOLEAN),
      Field.of("str", STRING),
      Field.of("day", DATE),
      Field.of("ts", TIMESTAMP),
      Field.of("dt", DATETIME),
      Field.of("tm", TIME),
      Field.of("binary", BYTES),
      Field.of("float", FLOAT),
      Field.of("nums", RECORD,
        Field.of("min", NUMERIC),
        Field.of("max", NUMERIC),
        Field.of("pi", NUMERIC),
        Field.of("big_pi", NUMERIC)),
      Field.newBuilder("int_arr", INTEGER).setMode(Mode.REPEATED).build(),
      Field.newBuilder("int_struct_arr", RECORD,
        Field.of("i", INTEGER)).setMode(Mode.REPEATED).build())

    val schemaFields = SchemaConverters.toSpark(bqSchema).fields

    var avroSparkRow: InternalRow = null
    var arrowSparkRow : InternalRow = null

    val arrowBinaryIterator = new ArrowBinaryIterator(columnsInOrder.asJava,
      arrowSchema,
      arrowByteString,
      Optional.empty()).asScala

    if (arrowBinaryIterator.hasNext) {
       arrowSparkRow = arrowBinaryIterator.next();
    }

    val avroBinaryIterator = new AvroBinaryIterator(bqSchema,
      columnsInOrder.asJava, avroSchema, avroByteString, Optional.empty())

    if (avroBinaryIterator.hasNext) {
      avroSparkRow = avroBinaryIterator.next()
    }

    for (col <- 0 to 11)
    {
        if (arrowSparkRow.isNullAt(col))
        {
            assert(avroSparkRow.isNullAt(col))
        }
        else
        {
            val schemaFieldDataType = schemaFields.apply(col).dataType

            if (schemaFieldDataType == BinaryType)
            {
                avroSparkRow.getBinary(col).equals(arrowSparkRow.getBinary(col))
            }
            else
            if (schemaFieldDataType == (ArrayType(LongType, true)))
            {
                val arr1 = avroSparkRow.getArray(col).array
                val arr2 = arrowSparkRow.getArray(col).array

                assert(arr1 sameElements arr2)
            }
            else
            if (schemaFieldDataType.typeName == s"struct") {
              for (fieldI <- 0 to 3) {
                assert(avroSparkRow.getStruct(col, 4).getDecimal(fieldI, 38, 9)
                  .equals(arrowSparkRow.getStruct(col, 4).getDecimal(fieldI, 38, 9)))
              }
            }
            else
            {
              assert(avroSparkRow.get(col, schemaFieldDataType).equals(
                arrowSparkRow.get(col, schemaFieldDataType)))
            }
        }
    }

    // handling last field specially because of its complex nature

    val x = arrowSparkRow.getArray(12).getStruct(0, 1).getLong(0)
    val y = avroSparkRow.getArray(12).getStruct(0, 1).getLong(0)

    assert (x == y)
    assert(arrowBinaryIterator.hasNext == false)
    assert(avroBinaryIterator.hasNext == false)
  }

  test("Validating DateTime conversion for Arrow for time prior to 1970-01-01"){
    val actualDateTimeIterator =
      Iterator(
        "1289-12-01T01:12:45.575123",
        "0889-12-03T01:17:25.975",
        "1970-01-01T00:00",
        "2001-12-01T01:01:01")

    val arrowByteString =
      ByteString.copyFrom(
        toByteArray(getClass.getResourceAsStream("/arrowDateTimeRowsInBytes")))

    val arrowSchema =
      ByteString.copyFrom(
        toByteArray(getClass.getResourceAsStream("/arrowDateTimeSchema")))

    val columnsInOrder = Seq("col_date_time")

    val arrowBinaryIterator =
      new ArrowBinaryIterator(
        columnsInOrder.asJava, arrowSchema, arrowByteString, Optional.empty()).asScala

    while (arrowBinaryIterator.hasNext) {
      val arrowSparkRow = arrowBinaryIterator.next()
      val readDateTime = arrowSparkRow.get(0, StringType).toString
      val actualDateTime = actualDateTimeIterator.next
      assert(readDateTime.equals(actualDateTime))
    }
  }


}