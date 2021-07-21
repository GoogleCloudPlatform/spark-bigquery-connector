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
package com.google.cloud.spark.bigquery.it

import com.google.cloud.bigquery._
import com.google.cloud.spark.bigquery.TestUtils
import org.apache.spark.sql.SparkSession
import org.scalatest.concurrent.TimeLimits
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuite, Ignore, Matchers}

@Ignore
class SparkBigQueryEndToEndReadFromQueryITSuite extends FunSuite
  with BeforeAndAfter
  with BeforeAndAfterAll
  with Matchers
  with TimeLimits
  with TableDrivenPropertyChecks {

  val bq = BigQueryOptions.getDefaultInstance.getService
  private val ALL_TYPES_TABLE_NAME = "all_types"
  private var spark: SparkSession = _
  private var testDataset: String = _

  before {
    // have a fresh table for each test
    testTable = s"test_${System.nanoTime()}"
  }

  private var testTable: String = _

  override def beforeAll: Unit = {
    spark = TestUtils.getOrCreateSparkSession(getClass.getSimpleName)
    testDataset = s"spark_bigquery_${getClass.getSimpleName}_${System.currentTimeMillis()}"
    IntegrationTestUtils.createDataset(testDataset)
  }

  private def testReadFromQueryInternal(format: String, query: String) {
    val df = spark.read.format(format)
      .option("viewsEnabled", true)
      .option("materializationDataset", testDataset)
      .load(query)

    val totalRows = df.count
    totalRows should equal(9)

    val corpuses = df.select("corpus").collect().map(row => row(0).toString).sorted
    val expectedCorpuses = Array("2kinghenryvi", "3kinghenryvi", "allswellthatendswell", "hamlet",
      "juliuscaesar", "kinghenryv", "kinglear", "periclesprinceoftyre", "troilusandcressida")
    corpuses should equal(expectedCorpuses)

  }

  private def testReadFromQuery(format: String): Unit = {
    // the query suffix is to make sure that each format will have
    // a different table created due to the destination table cache
    testReadFromQueryInternal(format,
      "SELECT corpus, corpus_date FROM `bigquery-public-data.samples.shakespeare` " +
        s"WHERE word='spark' AND '$format'='$format'")
  }

  private def testReadFromQueryWithNewLine(format: String) {
    // the query suffix is to make sure that each format will have
    // a different table created due to the destination table cache
    testReadFromQueryInternal(format,
      """SELECT
        |corpus, corpus_date
        |FROM `bigquery-public-data.samples.shakespeare` """.stripMargin +
        s"WHERE word='spark' AND '$format'='$format'")
  }

  def testQueryOption(format: String) {
    // the query suffix is to make sure that each format will have
    // a different table created due to the destination table cache
    val sql = "SELECT corpus, word_count FROM `bigquery-public-data.samples.shakespeare` " +
      s"WHERE word='spark' AND '$format'='$format'";
    val df = spark.read.format(format)
      .option("viewsEnabled", true)
      .option("materializationDataset", testDataset)
      .option("query", sql)
      .load()

    val totalRows = df.count
    totalRows should equal(9)

    val corpuses = df.select("corpus").collect().map(row => row(0).toString).sorted
    val expectedCorpuses = Array("2kinghenryvi", "3kinghenryvi", "allswellthatendswell", "hamlet",
      "juliuscaesar", "kinghenryv", "kinglear", "periclesprinceoftyre", "troilusandcressida")
    corpuses should equal(expectedCorpuses)
  }

  def testBadQuery(format: String): Unit = {
    val badSql = "SELECT bogus_column FROM `bigquery-public-data.samples.shakespeare`"
    // v1 throws BigQueryConnectorException
    // v2 throws Guice ProviderException, as the table is materialized in teh module
    intercept[RuntimeException] {
      spark.read.format(format)
        .option("viewsEnabled", true)
        .option("materializationDataset", testDataset)
        .load(badSql)
    }
  }

  test("test read from query - v1") {
    testReadFromQuery("bigquery")
  }

  test("test read from query - v2") {
    testReadFromQuery("com.google.cloud.spark.bigquery.v2.BigQueryDataSourceV2")
  }

  test("test read from query with newline - v1") {
    testReadFromQueryWithNewLine("bigquery")
  }

  test("test read from query with newline - v2") {
    testReadFromQueryWithNewLine("com.google.cloud.spark.bigquery.v2.BigQueryDataSourceV2")
  }

  test("test query option - v1") {
    testQueryOption("bigquery")
  }

  test("test query option - v2") {
    testQueryOption("com.google.cloud.spark.bigquery.v2.BigQueryDataSourceV2")
  }

  test("test bad query - v1") {
    testBadQuery("bigquery")
  }

  test("test bad query - v2") {
    testBadQuery("com.google.cloud.spark.bigquery.v2.BigQueryDataSourceV2")
  }

  override def afterAll: Unit = {
    IntegrationTestUtils.deleteDatasetAndTables(testDataset)
  }

}

