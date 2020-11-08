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

import java.sql.{Date, Timestamp}

import com.google.cloud.bigquery._
import com.google.cloud.bigquery.storage.v1.DataFormat
import com.google.cloud.spark.bigquery.direct.DirectBigQueryRelation
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.mockito.Mockito._
import org.mockito.{Mock, MockitoAnnotations}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.{BeforeAndAfter, Matchers}

class DirectBigQueryRelationSuite
  extends AnyFunSuite with BeforeAndAfter with Matchers {

  private val PROJECT_ID = "test_project"
  private val ID = TableId.of("test_project", "test_dataset", "test_table")
  private val TABLE_NAME = "test_project:test_dataset.test_table"
  @Mock
  private var sqlCtx: SQLContext = _

  private val TABLE = TableInfo.of(
    ID,
    StandardTableDefinition.newBuilder()
      .setSchema(Schema.of(
        Field.of("foo", LegacySQLTypeName.STRING),
        Field.of("bar", LegacySQLTypeName.INTEGER))
      )
      .setNumBytes(42L * 1000 * 1000 * 1000) // 42GB
      .build())
  private var bigQueryRelation: DirectBigQueryRelation = _

  before {
    val options = defaultOptions
    options.readDataFormat = DataFormat.AVRO
    MockitoAnnotations.initMocks(this)
    bigQueryRelation = new DirectBigQueryRelation(options, TABLE)(sqlCtx)
  }

  after {
    validateMockitoUsage()
  }

  test("size in bytes") {
    assert(42L * 1000 * 1000 * 1000 == bigQueryRelation.sizeInBytes)
  }

  test("schema") {
    val expectedSchema = StructType(Seq(
      StructField("foo", StringType), StructField("bar", LongType)))
    val schema = bigQueryRelation.schema
    assert(expectedSchema == schema)
  }

  // We don't have to be this permissive in user schemas, but we should at least allow
  // Long -> Int type changes
  test("user defined schema") {
    val expectedSchema = StructType(Seq(StructField("baz", ShortType)))
    val options = defaultOptions
    options.schema = com.google.common.base.Optional.of(expectedSchema)
    bigQueryRelation = new DirectBigQueryRelation(options, TABLE)(sqlCtx)
    val schema = bigQueryRelation.schema
    assert(expectedSchema == schema)
  }

  test("valid filters for Avro") {
    val validFilters = Seq(
      EqualTo("foo", "manatee"),
      GreaterThan("foo", "aardvark"),
      GreaterThanOrEqual("bar", 2),
      LessThan("foo", "zebra"),
      LessThanOrEqual("bar", 1),
      In("foo", Array(1, 2, 3)),
      IsNull("foo"),
      IsNotNull("foo"),
      And(IsNull("foo"), IsNotNull("bar")),
      Or(IsNull("foo"), IsNotNull("foo")),
      Not(IsNull("foo")),
      StringStartsWith("foo", "abc"),
      StringEndsWith("foo", "def"),
      StringContains("foo", "abcdef")
    )
    validFilters.foreach { f =>
      assert(bigQueryRelation.unhandledFilters(Array(f)).isEmpty)
    }
  }

  test("valid filters for Arrow") {
    val options = defaultOptions
    options.readDataFormat = DataFormat.ARROW
    val bigQueryRelation = new DirectBigQueryRelation(options, TABLE)(sqlCtx)

    val validFilters = Seq(
      EqualTo("foo", "manatee"),
      GreaterThan("foo", "aardvark"),
      GreaterThanOrEqual("bar", 2),
      LessThan("foo", "zebra"),
      LessThanOrEqual("bar", 1),
      In("foo", Array(1, 2, 3)),
      IsNull("foo"),
      IsNotNull("foo"),
      And(IsNull("foo"), IsNotNull("bar")),
      Not(IsNull("foo")),
      StringStartsWith("foo", "abc"),
      StringEndsWith("foo", "def"),
      StringContains("foo", "abcdef")
    )
    validFilters.foreach { f =>
      assert(bigQueryRelation.unhandledFilters(Array(f)).isEmpty)
    }
  }

  test("multiple valid filters are handled") {
    val valid1 = EqualTo("foo", "bar")
    val valid2 = EqualTo("bar", 1)
    assert(bigQueryRelation.unhandledFilters(Array(valid1, valid2)).isEmpty)
  }

  test("invalid filters with Avro") {
    val valid1 = EqualTo("foo", "bar")
    val valid2 = EqualTo("bar", 1)
    val invalid1 = EqualNullSafe("foo", "bar")
    val invalid2 = And(EqualTo("foo", "bar"), Not(EqualNullSafe("bar", 1)))
    val unhandled = bigQueryRelation.unhandledFilters(Array(valid1, valid2, invalid1, invalid2))
    unhandled should contain allElementsOf Array(invalid1, invalid2)
  }

  test("invalid filters with Arrow") {
    val options = defaultOptions
    options.readDataFormat = DataFormat.ARROW
    val bigQueryRelation = new DirectBigQueryRelation(options, TABLE)(sqlCtx)

    val valid1 = EqualTo("foo", "bar")
    val valid2 = EqualTo("bar", 1)
    val invalid1 = EqualNullSafe("foo", "bar")
    val invalid2 = And(EqualTo("foo", "bar"), Not(EqualNullSafe("bar", 1)))
    val invalid3 = Or(IsNull("foo"), IsNotNull("foo"))
    val unhandled = bigQueryRelation.unhandledFilters(Array(valid1, valid2,
      invalid1, invalid2, invalid3))
    unhandled should contain allElementsOf Array(invalid1, invalid2, invalid3)
  }

  test("old filter behaviour, with filter option") {
    val options = defaultOptions
    options.combinePushedDownFilters = false
    options.filter = com.google.common.base.Optional.of("f>1")
    val r = new DirectBigQueryRelation(options, TABLE)(sqlCtx)
    checkFilters(r, "f>1", Array(GreaterThan("a", 2)), "f>1")
  }

  test("old filter behaviour, no filter option") {
    val options = defaultOptions
    options.combinePushedDownFilters = false
    val r = new DirectBigQueryRelation(options, TABLE)(sqlCtx)
    checkFilters(r, "", Array(GreaterThan("a", 2)), "a > 2")
  }

  test("new filter behaviour, with filter option") {
    val options = defaultOptions
    options.filter = com.google.common.base.Optional.of("f>1")
    val r = new DirectBigQueryRelation(options, TABLE)(sqlCtx)
    checkFilters(r, "(f>1)", Array(GreaterThan("a", 2)), "(f>1) AND (a > 2)")
  }

  test("new filter behaviour, no filter option") {
    val r = new DirectBigQueryRelation(
      defaultOptions, TABLE)(sqlCtx)
    checkFilters(r, "", Array(GreaterThan("a", 2)), "(a > 2)")
  }

  test("filter on date and timestamp fields") {
    val options = defaultOptions
    val r = new DirectBigQueryRelation(options, TABLE)(sqlCtx)
    val inFilter = In("datefield", Array(Date.valueOf("2020-09-01"), Date.valueOf("2020-11-03")))
    val equalFilter = EqualTo("tsField", Timestamp.valueOf("2020-01-25 02:10:10"))
    checkFilters(r, "", Array(inFilter, equalFilter),
      "(`datefield` IN UNNEST([DATE '2020-09-01', DATE '2020-11-03']) AND " +
        "`tsField` = TIMESTAMP '2020-01-25 02:10:10.0')")
  }

  def checkFilters(
        relation: DirectBigQueryRelation,
        resultWithoutFilters: String,
        filters: Array[Filter],
        resultWithFilters: String
      ): Unit = {
    val result1 = relation.getCompiledFilter(Array())
    result1 shouldBe resultWithoutFilters
    val result2 = relation.getCompiledFilter(filters)
    result2 shouldBe resultWithFilters
  }

  private def defaultOptions = {
    val config = new SparkBigQueryConfig
    config.tableId = ID
    config.parentProjectId = PROJECT_ID
    config
  }


}

