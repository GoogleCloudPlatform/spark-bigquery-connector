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
import com.google.cloud.spark.bigquery.{TestConstants, TestUtils}
import com.google.cloud.spark.bigquery.direct.DirectBigQueryRelation
import org.apache.spark.bigquery.BigNumeric
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.scalatest.concurrent.TimeLimits
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.time.SpanSugar._
import org.scalatest._

@Ignore
class SparkBigQueryEndToEndReadITSuite extends FunSuite
  with BeforeAndAfter
  with BeforeAndAfterAll
  with Matchers
  with TimeLimits
  with TableDrivenPropertyChecks {

  val filterData = Table(
    ("condition", "elements"),
    ("word_count == 4", Seq("'A", "'But", "'Faith")),
    ("word_count > 3", Seq("'", "''Tis", "'A")),
    ("word_count >= 2", Seq("'", "''Lo", "''O")),
    ("word_count < 3", Seq("''All", "''Among", "''And")),
    ("word_count <= 5", Seq("'", "''All", "''Among")),
    ("word_count in(8, 9)", Seq("'", "'Faith", "'Tis")),
    ("word_count is null", Seq()),
    ("word_count is not null", Seq("'", "''All", "''Among")),
    ("word_count == 4 and corpus == 'twelfthnight'", Seq("'Thou", "'em", "Art")),
    ("word_count == 4 or corpus > 'twelfthnight'", Seq("'", "''Tis", "''twas")),
    ("not word_count in(8, 9)", Seq("'", "''All", "''Among")),
    ("corpus like 'king%'", Seq("'", "'A", "'Affectionate")),
    ("corpus like '%kinghenryiv'", Seq("'", "'And", "'Anon")),
    ("corpus like '%king%'", Seq("'", "'A", "'Affectionate"))
  )
  val temporaryGcsBucket = "davidrab-sandbox"
  val bq = BigQueryOptions.getDefaultInstance.getService
  private val LIBRARIES_PROJECTS_TABLE = "bigquery-public-data.libraries_io.projects"
  private val SHAKESPEARE_TABLE = "bigquery-public-data.samples.shakespeare"
  private val SHAKESPEARE_TABLE_NUM_ROWS = 164656L
  private val SHAKESPEARE_TABLE_SCHEMA = StructType(Seq(
    StructField("word", StringType, nullable = false, metadata("description",
      "A single unique word (where whitespace is the delimiter) extracted from a corpus.")),
    StructField("word_count", LongType, nullable = false, metadata("description",
      "The number of times this word appears in this corpus.")),
    StructField("corpus", StringType, nullable = false, metadata("description",
      "The work from which this word was extracted.")),
    StructField("corpus_date", LongType, nullable = false, metadata("description",
      "The year in which this corpus was published."))))

  private val SHAKESPEARE_TABLE_SCHEMA_WITH_METADATA_COMMENT = StructType(
    SHAKESPEARE_TABLE_SCHEMA.fields.map(
      field => {
        val metadata =
          new MetadataBuilder()
            .withMetadata(field.metadata)
            .putString("comment", field.metadata.getString("description"))
            .build()
        StructField(field.name, field.dataType, field.nullable, metadata)
      }
    )
  )

  private val LARGE_TABLE = "bigquery-public-data.samples.natality"
  private val LARGE_TABLE_FIELD = "is_male"
  private val LARGE_TABLE_NUM_ROWS = 33271914L
  private val NON_EXISTENT_TABLE = "non-existent.non-existent.non-existent"
  private val STRUCT_COLUMN_ORDER_TEST_TABLE_NAME = "struct_column_order"
  private val ALL_TYPES_TABLE_NAME = "all_types"
  private val ALL_TYPES_VIEW_NAME = "all_types_view"
  private var spark: SparkSession = _
  private var testDataset: String = _

  private def metadata(key: String, value: String): Metadata = metadata(Map(key -> value))

  private def metadata(map: Map[String, String]): Metadata = {
    val metadata = new MetadataBuilder()
    for ((key, value) <- map) {
      metadata.putString(key, value)
    }
    metadata.build()
  }

  before {
    // have a fresh table for each test
    testTable = s"test_${System.nanoTime()}"
  }

  private var testTable: String = _

  testShakespeare("implicit read method") {
    import com.google.cloud.spark.bigquery._
    spark.read.bigquery(SHAKESPEARE_TABLE)
  }

  testShakespeare("explicit format") {
    spark.read.format("com.google.cloud.spark.bigquery")
      .option("table", SHAKESPEARE_TABLE)
      .load()
  }

  testShakespeare("short format") {
    spark.read.format("bigquery").option("table", SHAKESPEARE_TABLE).load()
  }

  testShakespeare("simplified api") {
    spark.read.format("bigquery").load(SHAKESPEARE_TABLE)
  }

  testShakespeare("DataSource v2") {
    spark.read.format("com.google.cloud.spark.bigquery.v2.BigQueryDataSourceV2")
      .option("table", SHAKESPEARE_TABLE).load()
  }

  testShakespeare("DataSource v2 - compressed") {
    val df = spark.read.format("com.google.cloud.spark.bigquery.v2.BigQueryDataSourceV2")
      .option("table", SHAKESPEARE_TABLE)
      .option("bqEncodedCreateReadSessionRequest", "EgZCBBoCEAI=")
      .load()
    // Test early termination succeeds
    df.head
    df
  }


  testShakespeare("DataSource v2 - compressed 1 background threads") {
    val df = spark.read.format("com.google.cloud.spark.bigquery.v2.BigQueryDataSourceV2")
      .option("table", SHAKESPEARE_TABLE)
      .option("bqEncodedCreateReadSessionRequest", "EgZCBBoCEAI=")
      .option("bqBackgroundThreadsPerStream", "1")
      .load()
    // Test early termination succeeds
    df.head
    df
  }

  testShakespeare("DataSource v2 - compressed 4 background threads") {
    val df = spark.read.format("com.google.cloud.spark.bigquery.v2.BigQueryDataSourceV2")
      .option("table", SHAKESPEARE_TABLE)
      .option("bqEncodedCreateReadSessionRequest", "EgZCBBoCEAI=")
      .option("bqBackgroundThreadsPerStream", "4")
      .load()
    // Test early termination succeeds
    df.head
    df

  }

  for (
    dataFormat <- Seq("avro", "arrow");
    dataSourceFormat <- Seq("bigquery", "com.google.cloud.spark.bigquery.v2.BigQueryDataSourceV2")
  ) {
    testsWithReadInFormat(dataSourceFormat, dataFormat)
  }

  override def beforeAll: Unit = {
    spark = TestUtils.getOrCreateSparkSession(getClass.getSimpleName)
    testDataset = s"spark_bigquery_${getClass.getSimpleName}_${System.currentTimeMillis()}"
    IntegrationTestUtils.createDataset(testDataset)
    IntegrationTestUtils.runQuery(
      TestConstants.ALL_TYPES_TABLE_QUERY_TEMPLATE.format(s"$testDataset.$ALL_TYPES_TABLE_NAME"))
    IntegrationTestUtils.createView(testDataset, ALL_TYPES_TABLE_NAME, ALL_TYPES_VIEW_NAME)
    IntegrationTestUtils.runQuery(
      TestConstants
        .STRUCT_COLUMN_ORDER_TEST_TABLE_QUERY_TEMPLATE
        .format(s"$testDataset.$STRUCT_COLUMN_ORDER_TEST_TABLE_NAME"))
  }

  test("test filters") {
    import com.google.cloud.spark.bigquery._
    val sparkImportVal = spark
    import sparkImportVal.implicits._
    forAll(filterData) { (condition, expectedElements) =>
      val df = spark.read.bigquery(SHAKESPEARE_TABLE)
      assert(SHAKESPEARE_TABLE_SCHEMA_WITH_METADATA_COMMENT == df.schema)
      assert(SHAKESPEARE_TABLE_NUM_ROWS == df.count)
      val firstWords = df.select("word")
        .where(condition)
        .distinct
        .as[String]
        .sort("word")
        .take(3)
      firstWords should contain theSameElementsInOrderAs expectedElements
    }
  }

  def testsWithReadInFormat(dataSourceFormat: String, dataFormat: String): Unit = {

    test(("testing view with different columns for select and filter. " +
      "DataSource %s. Data Format %s").format(dataSourceFormat, dataFormat)) {

      val df = getViewDataFrame(dataSourceFormat, dataFormat)

      // filer and select are pushed down to BQ
      val result = df
        .select("int_req")
        .filter("str = 'string'")
        .collect()

      result.size shouldBe 1
      result.filter(row => row(0) == 42).size shouldBe 1
    }

    test(("testing cached view with different columns for select and filter. " +
      "DataSource %s. Data Format %s").format(dataSourceFormat, dataFormat)) {

      val df = getViewDataFrame(dataSourceFormat, dataFormat)
      val cachedDF = df.cache()

      // filter and select are run on the spark side as the view was cached
      val result = cachedDF
        .select("int_req")
        .filter("str = 'string'")
        .collect()

      result.size shouldBe 1
      result.filter(row => row(0) == 42).size shouldBe 1
    }

    test("out of order columns. DataSource %s. Data Format %s"
      .format(dataSourceFormat, dataFormat)) {
      val row = spark.read.format(dataSourceFormat)
        .option("table", SHAKESPEARE_TABLE)
        .option("readDataFormat", dataFormat).load()
        .select("word_count", "word").head
      assert(row(0).isInstanceOf[Long])
      assert(row(1).isInstanceOf[String])
    }

    test("select all columns from a table. DataSource %s. Data Format %s"
      .format(dataSourceFormat, dataFormat)) {
      val row = spark.read.format(dataSourceFormat)
        .option("table", SHAKESPEARE_TABLE)
        .option("readDataFormat", dataFormat).load()
        .select("word_count", "word", "corpus", "corpus_date").head
      assert(row(0).isInstanceOf[Long])
      assert(row(1).isInstanceOf[String])
      assert(row(2).isInstanceOf[String])
      assert(row(3).isInstanceOf[Long])
    }

    //    test("cache data frame in DataSource %s. Data Format %s"
    //    .format(dataSourceFormat, dataFormat)) {
    //      val allTypesTable = readAllTypesTable("bigquery")
    //      writeToBigQuery(allTypesTable, SaveMode.Overwrite, "avro")
    //
    //      val df = spark.read.format("bigquery")
    //        .option("dataset", testDataset)
    //        .option("table", testTable)
    //        .option("readDataFormat", "arrow")
    //        .load().cache()
    //
    //      assert(df.head() == allTypesTable.head())
    //
    //      // read from cache
    //      assert(df.head() == allTypesTable.head())
    //      assert(df.schema == allTypesTable.schema)
    //    }

    test("number of partitions. DataSource %s. Data Format %s"
      .format(dataSourceFormat, dataFormat)) {
      val df = spark.read.format("com.google.cloud.spark.bigquery")
        .option("table", LARGE_TABLE)
        .option("parallelism", "5")
        .option("readDataFormat", dataFormat)
        .load()
      assert(5 == df.rdd.getNumPartitions)
    }

    test("default number of partitions. DataSource %s. Data Format %s"
      .format(dataSourceFormat, dataFormat)) {
      val df = spark.read.format(dataSourceFormat)
        .option("table", LARGE_TABLE)
        .option("readDataFormat", dataFormat)
        .load()

      assert(df.rdd.getNumPartitions == 58)
    }

    test("balanced partitions. DataSource %s. Data Format %s"
      .format(dataSourceFormat, dataFormat)) {
      import com.google.cloud.spark.bigquery._
      failAfter(120 seconds) {
        // Select first partition
        val df = spark.read
          .option("parallelism", 5)
          .option("readDataFormat", dataFormat)
          .option("filter", "year > 2000")
          .bigquery(LARGE_TABLE)
          .select(LARGE_TABLE_FIELD) // minimize payload
        val sizeOfFirstPartition = df.rdd.mapPartitionsWithIndex {
          case (_, it) => Iterator(it.size)
        }.collect().head

        // Since we are only reading from a single stream, we can expect to get
        // at least as many rows
        // in that stream as a perfectly uniform distribution would command.
        // Note that the assertion
        // is on a range of rows because rows are assigned to streams on the
        // server-side in
        // indivisible units of many rows.

        val numRowsLowerBound = LARGE_TABLE_NUM_ROWS / df.rdd.getNumPartitions
        assert(numRowsLowerBound <= sizeOfFirstPartition &&
          sizeOfFirstPartition < (numRowsLowerBound * 1.1).toInt)
      }
    }

    test("test optimized count(*). DataSource %s. Data Format %s"
      .format(dataSourceFormat, dataFormat)) {
      DirectBigQueryRelation.emptyRowRDDsCreated = 0
      val oldMethodCount = spark.read.format(dataSourceFormat)
        .option("table", "bigquery-public-data.samples.shakespeare")
        .option("optimizedEmptyProjection", "false")
        .option("readDataFormat", dataFormat)
        .load()
        .select("corpus_date")
        .where("corpus_date > 0")
        .count()

      assert(DirectBigQueryRelation.emptyRowRDDsCreated == 0)

      assertResult(oldMethodCount) {
        spark.read.format(dataSourceFormat)
          .option("table", "bigquery-public-data.samples.shakespeare")
          .option("readDataFormat", dataFormat)
          .load()
          .where("corpus_date > 0")
          .count()
      }

      if ("bigquery" == dataSourceFormat) {
        assert(DirectBigQueryRelation.emptyRowRDDsCreated == 1)
      }
    }

    test("test optimized count(*) with filter. DataSource %s. Data Format %s"
      .format(dataSourceFormat, dataFormat)) {
      DirectBigQueryRelation.emptyRowRDDsCreated = 0
      val oldMethodCount = spark.read.format(dataSourceFormat)
        .option("table", "bigquery-public-data.samples.shakespeare")
        .option("optimizedEmptyProjection", "false")
        .option("readDataFormat", dataFormat)
        .load()
        .select("corpus_date")
        .count()

      assert(DirectBigQueryRelation.emptyRowRDDsCreated == 0)

      assertResult(oldMethodCount) {
        spark.read.format(dataSourceFormat)
          .option("table", "bigquery-public-data.samples.shakespeare")
          .option("readDataFormat", dataFormat)
          .load()
          .count()
      }
      if ("bigquery" == dataSourceFormat) {
        assert(DirectBigQueryRelation.emptyRowRDDsCreated == 1)
      }
    }

    test("keeping filters behaviour. DataSource %s. Data Format %s"
      .format(dataSourceFormat, dataFormat)) {
      val newBehaviourWords = extractWords(
        spark.read.format(dataSourceFormat)
          .option("table", "bigquery-public-data.samples.shakespeare")
          .option("filter", "length(word) = 1")
          .option("combinePushedDownFilters", "true")
          .option("readDataFormat", dataFormat)
          .load())

      val oldBehaviourWords = extractWords(
        spark.read.format(dataSourceFormat)
          .option("table", "bigquery-public-data.samples.shakespeare")
          .option("filter", "length(word) = 1")
          .option("combinePushedDownFilters", "false")
          .option("readDataFormat", dataFormat)
          .load())

      newBehaviourWords should equal(oldBehaviourWords)
    }

    test("column order of struct. DataSource %s. Data Format %s"
      .format(dataSourceFormat, dataFormat)) {
      val sqlContext = spark.sqlContext
      val schema = Encoders.bean(classOf[TestConstants.ColumnOrderTestClass]).schema

      val dataset = spark.read
        .schema(schema)
        .option("dataset", testDataset)
        .option("table", STRUCT_COLUMN_ORDER_TEST_TABLE_NAME)
        .format(dataSourceFormat)
        .option("readDataFormat", dataFormat)
        .load()
        .as(Encoders.bean(classOf[TestConstants.ColumnOrderTestClass]))

      val row = Seq(dataset.head())(0)
      assert(row == TestConstants.STRUCT_COLUMN_ORDER_TEST_TABLE_COLS)
    }
  }

  def getViewDataFrame(dataSourceFormat: String, dataFormat: String): DataFrame =
    spark.read.format(dataSourceFormat)
      .option("table", ALL_TYPES_VIEW_NAME)
      .option("viewsEnabled", "true")
      .option("viewMaterializationProject", System.getenv("GOOGLE_CLOUD_PROJECT"))
      .option("viewMaterializationDataset", testDataset)
      .option("readDataFormat", dataFormat)
      .load()

  def readAllTypesTable(dataSourceFormat: String): DataFrame =
    spark.read.format(dataSourceFormat)
      .option("dataset", testDataset)
      .option("table", ALL_TYPES_TABLE_NAME)
      .load()


  Seq("bigquery", "com.google.cloud.spark.bigquery.v2.BigQueryDataSourceV2")
    .foreach(testsWithDataSource)

  def testsWithDataSource(dataSourceFormat: String) {

    test("OR across columns with Arrow. DataSource %s".format(dataSourceFormat)) {

      val avroResults = spark.read.format("bigquery")
        .option("table", "bigquery-public-data.samples.shakespeare")
        .option("filter", "word_count = 1 OR corpus_date = 0")
        .option("readDataFormat", "AVRO")
        .load().collect()

      val arrowResults = spark.read.format("bigquery")
        .option("table", "bigquery-public-data.samples.shakespeare")
        .option("readDataFormat", "ARROW")
        .load().where("word_count = 1 OR corpus_date = 0")
        .collect()

      avroResults should equal(arrowResults)
    }

    // Disabling the test until the merge the master
    // TODO: enable it
    /*
    test("Count with filters - Arrow. DataSource %s".format(dataSourceFormat)) {

      val countResults = spark.read.format(dataSourceFormat)
        .option("table", "bigquery-public-data.samples.shakespeare")
        .option("readDataFormat", "ARROW")
        .load().where("word_count = 1 OR corpus_date = 0")
        .count()

      val countAfterCollect = spark.read.format(dataSourceFormat)
        .option("table", "bigquery-public-data.samples.shakespeare")
        .option("readDataFormat", "ARROW")
        .load().where("word_count = 1 OR corpus_date = 0")
        .collect().size

      countResults should equal(countAfterCollect)
    }
    */
    test("read data types. DataSource %s".format(dataSourceFormat)) {
      // temporarily skipping for v2
      if (dataSourceFormat.equals("bigquery")) {
        val allTypesTable = readAllTypesTable(dataSourceFormat)
        val expectedRow = spark.range(1).select(TestConstants.ALL_TYPES_TABLE_COLS: _*).head.toSeq
        val rows = allTypesTable.head.toSeq

        var i = 0
        for (row <- rows) {

          if (i == TestConstants.BIG_NUMERIC_COLUMN_POSITION) {
            for (j <- 0 to 1) {
              val bigNumericValue =
                row.asInstanceOf[GenericRowWithSchema].get(j).asInstanceOf[BigNumeric]

              val bigNumericString = bigNumericValue.getNumber.toPlainString

              val expectedBigNumericString =
                expectedRow(i).asInstanceOf[GenericRowWithSchema].get(j)

              assert(bigNumericString === expectedBigNumericString)
            }
          } else {
            assert(row === expectedRow(i))
          }

          i += 1
        }
      }
    }


    test("known size in bytes. DataSource %s".format(dataSourceFormat)) {
      val allTypesTable = readAllTypesTable(dataSourceFormat)
      val actualTableSize = allTypesTable.queryExecution.analyzed.stats.sizeInBytes
      assert(actualTableSize == TestConstants.ALL_TYPES_TABLE_SIZE)
    }

    test("known schema. DataSource %s".format(dataSourceFormat)) {
      val allTypesTable = readAllTypesTable(dataSourceFormat)
      assert(allTypesTable.schema == TestConstants.ALL_TYPES_TABLE_SCHEMA)
    }

    test("user defined schema. DataSource %s".format(dataSourceFormat)) {
      // TODO(pmkc): consider a schema that wouldn't cause cast errors if read.
      val expectedSchema = StructType(Seq(StructField("whatever", ByteType)))
      val table = spark.read.schema(expectedSchema)
        .format(dataSourceFormat)
        .option("table", SHAKESPEARE_TABLE)
        .load()
      assert(expectedSchema == table.schema)
    }

    test("non-existent schema. DataSource %s".format(dataSourceFormat)) {
      assertThrows[RuntimeException] {
        spark.read.format(dataSourceFormat).option("table", NON_EXISTENT_TABLE).load()
      }
    }

    test("head does not time out and OOM. DataSource %s".format(dataSourceFormat)) {
      failAfter(10 seconds) {
        spark.read.format(dataSourceFormat)
          .option("table", LARGE_TABLE)
          .load()
          .select(LARGE_TABLE_FIELD)
          .head
      }
    }
    test("Unhandle filter on struct. DataSource %s".format(dataSourceFormat)) {
      val df = spark.read.format(dataSourceFormat)
        .option("table", "bigquery-public-data:samples.github_nested")
        .option("filter", "url like '%spark'")
        .load()

      val result = df.select("url")
        .where("repository is not null")
        .collect()

      result.size shouldBe 85
    }
  }

  // Write tests. We have four save modes: Append, ErrorIfExists, Ignore and
  // Overwrite. For each there are two behaviours - the table exists or not.
  // See more at http://spark.apache.org/docs/2.3.2/api/java/org/apache/spark/sql/SaveMode.html

  override def afterAll: Unit = {
    IntegrationTestUtils.deleteDatasetAndTables(testDataset)
  }

  /** Generate a test to verify that the given DataFrame is equal to a known result. */
  def testShakespeare(description: String)(df: => DataFrame): Unit = {
    test(s"read using $description") {
      val youCannotImportVars = spark
      import youCannotImportVars.implicits._
      assert(SHAKESPEARE_TABLE_SCHEMA_WITH_METADATA_COMMENT == df.schema)
      assert(SHAKESPEARE_TABLE_NUM_ROWS == df.count())
      val firstWords = df.select("word")
        .where("word >= 'a' AND word not like '%\\'%'")
        .distinct
        .as[String].sort("word").take(3)
      firstWords should contain theSameElementsInOrderAs Seq("a", "abaissiez", "abandon")
    }
  }

  def extractWords(df: DataFrame): Set[String] = {
    df.select("word")
      .where("corpus_date = 0")
      .collect()
      .map(_.getString(0))
      .toSet
  }
}

