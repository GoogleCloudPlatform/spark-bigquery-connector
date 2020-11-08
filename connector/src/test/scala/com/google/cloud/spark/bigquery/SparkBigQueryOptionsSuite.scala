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

import java.util.{Optional, OptionalInt}

import com.google.cloud.bigquery.JobInfo
import com.google.common.collect.ImmutableMap
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.internal.SQLConf
import org.scalatest.FunSuite

import scala.collection.JavaConverters._

class SparkBigQueryConfigSuite extends FunSuite {

  val hadoopConfiguration = new Configuration
  hadoopConfiguration.set(SparkBigQueryConfig.GCS_CONFIG_CREDENTIALS_FILE_PROPERTY, "hadoop_cfile")
  hadoopConfiguration.set(SparkBigQueryConfig.GCS_CONFIG_PROJECT_ID_PROPERTY, "hadoop_project")

  val parameters = Map("table" -> "dataset.table")
  val emptyMap : ImmutableMap[String, String] = ImmutableMap.of()
  val sparkVersion = "2.4.0"
  
  private def asDataSourceOptionsMap(map: Map[String, String]) = {
    (map ++ map.map { case (key, value) => (key.toLowerCase, value) } .toMap).asJava
  }

  test("taking credentials file from GCS hadoop config") {
    assertResult(Optional.of("hadoop_cfile")) {
      val options = SparkBigQueryConfig.from(
        asDataSourceOptionsMap(parameters),
        emptyMap, // allConf
        hadoopConfiguration,
        1,
        new SQLConf,
        sparkVersion,
        Optional.empty()) // schema
      options.getCredentialsFile
    }
  }

  test("taking credentials file from the properties") {
    assertResult(Optional.of("cfile")) {
      val options = SparkBigQueryConfig.from(
        asDataSourceOptionsMap(parameters + ("credentialsFile" -> "cfile")),
        emptyMap, // allConf
        new Configuration,
        1,
        new SQLConf,
        sparkVersion,
        Optional.empty()) // schema
      options.getCredentialsFile
    }
  }

  test("no credentials file is provided") {
    val options = SparkBigQueryConfig.from(
      asDataSourceOptionsMap(parameters),
      emptyMap, // allConf
      new Configuration,
      1,
      new SQLConf,
      sparkVersion,
      Optional.empty()) // schema
    assert(!options.getCredentialsFile.isPresent)
  }

  test("taking project id from GCS hadoop config") {
    assertResult("hadoop_project") {
      val options = SparkBigQueryConfig.from(
        asDataSourceOptionsMap(parameters),
        emptyMap, // allConf
        hadoopConfiguration,
        1,
        new SQLConf,
        sparkVersion,
        Optional.empty()) // schema
      options.getTableId.getProject
    }
  }

  test("taking project id from the properties") {
    assertResult("pid") {
      val options = SparkBigQueryConfig.from(
        asDataSourceOptionsMap(parameters + ("project" -> "pid")),
        emptyMap, // allConf
        hadoopConfiguration,
        1,
        new SQLConf,
        sparkVersion,
        Optional.empty()) // schema
      options.getTableId.getProject
    }
  }

  test("no project id is provided") {
    assertResult(null) {
      val options = SparkBigQueryConfig.from(
        asDataSourceOptionsMap(parameters),
        emptyMap, // allConf
        new Configuration,
        1,
        new SQLConf,
        sparkVersion,
        Optional.empty()) // schema
      options.getTableId.getProject
    }
  }

  test("Invalid data format") {
      val thrown = intercept[Exception] {
        SparkBigQueryConfig.from(
          asDataSourceOptionsMap(parameters + ("readDataFormat" -> "abc")),
          emptyMap, // allConf
          new Configuration,
          1,
          new SQLConf,
          sparkVersion,
          Optional.empty()) // schema
      }
      assert (thrown.getMessage ==
        "Data read format 'ABC' is not supported. Supported formats are 'ARROW,AVRO'")
  }

  test("data format - no value set") {
    assertResult("ARROW") {
      val options = SparkBigQueryConfig.from(
        asDataSourceOptionsMap(parameters),
        emptyMap, // allConf
        new Configuration,
        1,
        new SQLConf,
        sparkVersion,
        Optional.empty()) // schema
      options.getReadDataFormat.toString
    }
  }

  test("Set Read Data Format as Avro") {
    assertResult("AVRO") {
      val options = SparkBigQueryConfig.from(
        asDataSourceOptionsMap(parameters + ("readDataFormat" -> "Avro")),
        emptyMap, // allConf
        new Configuration,
        1,
        new SQLConf,
        sparkVersion,
        Optional.empty()) // schema
      options.getReadDataFormat.toString
    }
  }

  test("getAnyOptionWithFallback - only new config exist") {
    assertResult(Optional.of("foo")) {
      val options = SparkBigQueryConfig.from(
        asDataSourceOptionsMap(parameters + ("materializationProject" -> "foo")),
        emptyMap, // allConf
        new Configuration,
        1,
        new SQLConf,
        sparkVersion,
        Optional.empty()) // schema
      options.getMaterializationProject
    }
  }

  test("getAnyOptionWithFallback - both configs exist") {
    assertResult(Optional.of("foo")) {
      val options = SparkBigQueryConfig.from(
        asDataSourceOptionsMap(parameters + (
          "materializationProject" -> "foo",
          "viewMaterializationProject" -> "bar")
          ),
        emptyMap, // allConf
        new Configuration,
        1,
        new SQLConf,
        sparkVersion,
        Optional.empty()) // schema
      options.getMaterializationProject
    }
  }

  test("getAnyOptionWithFallback - only old config exist") {
    assertResult(Optional.of("bar")) {
      val options = SparkBigQueryConfig.from(
        asDataSourceOptionsMap(parameters + ("viewMaterializationProject" -> "bar")),
        emptyMap, // allConf
        new Configuration,
        1,
        new SQLConf,
        sparkVersion,
        Optional.empty()) // schema
      options.getMaterializationProject
    }
  }

  test("getAnyOptionWithFallback - no config exist") {
    assertResult(Optional.empty()) {
      val options = SparkBigQueryConfig.from(
        asDataSourceOptionsMap(parameters),
        emptyMap, // allConf
        new Configuration,
        1,
        new SQLConf,
        sparkVersion,
        Optional.empty()) // schema
      options.getMaterializationProject
    }
  }

  test("maxParallelism - only new config exist") {
    assertResult(3) {
      val options = SparkBigQueryConfig.from(
        asDataSourceOptionsMap(parameters + ("maxParallelism" -> "3")),
        emptyMap, // allConf
        new Configuration,
        1,
        new SQLConf,
        sparkVersion,
        Optional.empty()) // schema
      options.getMaxParallelism
    }
  }

  test("maxParallelism - both configs exist") {
    assertResult(3) {
      val options = SparkBigQueryConfig.from(
        asDataSourceOptionsMap(parameters + ("maxParallelism" -> "3", "parallelism" -> "10")),
        emptyMap, // allConf
        new Configuration,
        1,
        new SQLConf,
        sparkVersion,
        Optional.empty()) // schema
      options.getMaxParallelism
    }
  }

  test("maxParallelism - only old config exist") {
    assertResult(10) {
      val options = SparkBigQueryConfig.from(
        asDataSourceOptionsMap(parameters + ("parallelism" -> "10")),
        emptyMap, // allConf
        new Configuration,
        1,
        new SQLConf,
        sparkVersion,
        Optional.empty()) // schema
      options.getMaxParallelism
    }
  }

  test("maxParallelism - no config exist") {
    assertResult(1) {
      val options = SparkBigQueryConfig.from(
        asDataSourceOptionsMap(parameters),
        emptyMap, // allConf
        new Configuration,
        1,
        new SQLConf,
        sparkVersion,
        Optional.empty()) // schema
      options.getMaxParallelism
    }
  }
  
  test("loadSchemaUpdateOption - allowFieldAddition") {
    assertResult(Seq(JobInfo.SchemaUpdateOption.ALLOW_FIELD_ADDITION)) {
      val options = SparkBigQueryConfig.from(
        asDataSourceOptionsMap(parameters + ("allowFieldAddition" -> "true")),
        emptyMap, // allConf
        new Configuration,
        1,
        new SQLConf,
        sparkVersion,
        Optional.empty()) // schema
      options.getLoadSchemaUpdateOptions.asScala.toSeq
    }
  }

  test("loadSchemaUpdateOption - allowFieldRelaxation") {
    assertResult(Seq(JobInfo.SchemaUpdateOption.ALLOW_FIELD_RELAXATION)) {
      val options = SparkBigQueryConfig.from(
        asDataSourceOptionsMap(parameters + ("allowFieldRelaxation" -> "true")),
        emptyMap, // allConf
        new Configuration,
        1,
        new SQLConf,
        sparkVersion,
        Optional.empty()) // schema
      options.getLoadSchemaUpdateOptions.asScala.toSeq
    }
  }

  test("loadSchemaUpdateOption - both") {
    assertResult(Seq(
      JobInfo.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
      JobInfo.SchemaUpdateOption.ALLOW_FIELD_RELAXATION)) {
      val options = SparkBigQueryConfig.from(
        asDataSourceOptionsMap(parameters +
          ("allowFieldAddition" -> "true", "allowFieldRelaxation" -> "true")),
        emptyMap, // allConf
        new Configuration,
        1,
        new SQLConf,
        sparkVersion,
        Optional.empty()) // schema
      options.getLoadSchemaUpdateOptions.asScala.toSeq
    }
  }

  test("loadSchemaUpdateOption - none") {
    assertResult(true) {
      val options = SparkBigQueryConfig.from(
        asDataSourceOptionsMap(parameters),
        emptyMap, // allConf
        new Configuration,
        1,
        new SQLConf,
        sparkVersion,
        Optional.empty()) // schema
      options.getLoadSchemaUpdateOptions.isEmpty
    }
  }

  test("normalize All Conf") {
    val originalConf = Map(
      "key1" -> "val1",
      "spark.datasource.bigquery.key2" -> "val2",
      "key3" -> "val3",
      "spark.datasource.bigquery.key3" -> "external val3")
    val normalizedConf = SparkBigQueryConfig.normalizeConf(originalConf.asJava)

    assert(normalizedConf.get("key1")  == "val1")
    assert(normalizedConf.get("key2")  == "val2")
    assert(normalizedConf.get("key3")  == "external val3")
  }

  test("Set persistentGcsPath") {
    assertResult(Optional.of("/persistent/path")) {
      val options = SparkBigQueryConfig.from(
        asDataSourceOptionsMap(parameters + ("persistentGcsPath" -> "/persistent/path")),
        emptyMap, // allConf
        new Configuration,
        1,
        new SQLConf,
        sparkVersion,
        Optional.empty()) // schema
      options.getPersistentGcsPath
    }
  }

  test("Set persistentGcsBucket") {
    assertResult(Optional.of("gs://persistentGcsBucket")) {
      val options = SparkBigQueryConfig.from(
        asDataSourceOptionsMap(parameters + ("persistentGcsBucket" -> "gs://persistentGcsBucket")),
        emptyMap, // allConf
        new Configuration,
        1,
        new SQLConf,
        sparkVersion,
        Optional.empty()) // schema
      options.getPersistentGcsBucket
    }
  }
}
