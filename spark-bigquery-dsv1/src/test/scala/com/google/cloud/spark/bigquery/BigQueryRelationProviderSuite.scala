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

import com.google.api.client.util.Base64
import com.google.cloud.bigquery._
import com.google.cloud.bigquery.connector.common.BigQueryClient
import com.google.cloud.spark.bigquery.direct.DirectBigQueryRelation
import com.google.inject._
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.mockito.{Mock, MockitoAnnotations}
import org.scalatest.{BeforeAndAfter, FunSuite}

class BigQueryRelationProviderSuite
  extends FunSuite
    with BeforeAndAfter {

  // TODO(#23) Add test case covering 'credentials' and 'credentialsFile' options

  private val ID = TableId.of("testproject", "test_dataset", "test_table")
  private val TABLE_NAME = "testproject:test_dataset.test_table"
  private val TABLE = TableInfo.of(
    ID,
    StandardTableDefinition
      .newBuilder()
      .setSchema(Schema.of(Field.of("foo", LegacySQLTypeName.STRING)))
      .setNumBytes(42L)
      .build())

  @Mock
  private var sqlCtx: SQLContext = _
  @Mock
  private var sc: SparkContext = _
  private var conf: Configuration = _
  @Mock
  private var config: SparkBigQueryConfig = _
  @Mock
  private var readTableOptions: BigQueryClient.ReadTableOptions = _
  @Mock
  private var bigQueryClient: BigQueryClient = _
  private var provider: BigQueryRelationProvider = _

  @Mock
  private var table: Table = _

  before {
    MockitoAnnotations.initMocks(this)
    conf = new Configuration(false)
    when(config.getTableId).thenReturn(ID)
    when(config.toReadTableOptions).thenReturn(readTableOptions)
    val injector = Guice.createInjector(new Module {
      override def configure(binder: Binder): Unit = {
        binder.bind(classOf[SparkBigQueryConfig]).toInstance(config)
        binder.bind(classOf[BigQueryClient]).toInstance(bigQueryClient)
      }
    })
    val guiceInjectorCreator = new GuiceInjectorCreator {
      override def createGuiceInjector(sqlContext: SQLContext,
                                       parameters: Map[String, String],
                                       schema: scala.Option[StructType] = None): Injector = injector
    }
    provider =
      new BigQueryRelationProvider(() => guiceInjectorCreator)
    table = TestUtils.table(TABLE)

    val master = "local[*]"
    val appName = "MyApp"
    val sparkConf: SparkConf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)
    val ss = SparkSession.builder().config(sparkConf).getOrCreate()

    when(sqlCtx.sparkContext).thenReturn(sc)
    when(sc.hadoopConfiguration).thenReturn(conf)
    when(sc.version).thenReturn("2.4.0")

    when(sqlCtx.sparkSession).thenReturn(ss)
    when(sqlCtx.getAllConfs).thenReturn(Map.empty[String, String])
  }

  after {
    // verifyNoMoreInteractions(bigQuery)
    validateMockitoUsage()
  }

  test("table exists") {
    when(bigQueryClient.getReadTable(any(classOf[BigQueryClient.ReadTableOptions])))
      .thenReturn(table)

    val relation = provider.createRelation(sqlCtx, Map("table" -> TABLE_NAME,
      "parentProject" -> ID.getProject()))
    assert(relation.isInstanceOf[DirectBigQueryRelation])

    verify(bigQueryClient).getReadTable(any(classOf[BigQueryClient.ReadTableOptions]))
  }

  test("table does not exist") {
    when(bigQueryClient.getReadTable(any(classOf[BigQueryClient.ReadTableOptions])))
      .thenReturn(null)

    assertThrows[RuntimeException] {
      provider.createRelation(sqlCtx, Map("table" -> TABLE_NAME,
        "parentProject" -> ID.getProject()))
    }
    verify(bigQueryClient).getReadTable(any(classOf[BigQueryClient.ReadTableOptions]))
  }

  test("Credentials parameter is used to initialize BigQueryOptions") {

    val defaultProvider = new BigQueryRelationProvider()
    val invalidCredentials = Base64.encodeBase64String("{}".getBytes)

    val caught = intercept[ProvisionException] {
      defaultProvider.createRelation(sqlCtx, Map("parentProject" -> ID.getProject,
        "credentials" -> invalidCredentials, "table" -> TABLE_NAME))
    }

    assert(caught.getMessage.contains("Failed to create Credentials from key"))
  }

  /*
  The test is removed as it doesn't work when the GOOGLE_APPLICATION_CREDENTIALS
  environment variable is set. Also we want to check scenarios where the default
  instance is used, such as on Dataproc servers.

  test("default BigQueryOptions instance is used when no credentials provided") {
    val defaultProvider = new BigQueryRelationProvider()
    val caught = intercept[IllegalArgumentException] {
      defaultProvider.createRelation(sqlCtx, Map("project" -> ID.getProject, "table" -> TABLE_NAME))
    }
    assert(caught.getMessage.contains("project ID is required"))
  }
  */


}
