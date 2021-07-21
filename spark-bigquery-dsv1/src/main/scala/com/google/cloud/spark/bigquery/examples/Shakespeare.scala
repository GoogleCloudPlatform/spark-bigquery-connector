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
package com.google.cloud.spark.bigquery.examples

import com.google.cloud.spark.bigquery._
import org.apache.spark.sql.SparkSession

object Shakespeare {
  def main(args: Array[String]) {
    val spark = SparkSession.builder()
      .appName("spark-bigquery-demo")
      .getOrCreate()

    // Use the Cloud Storage bucket for temporary BigQuery export data used
    // by the connector. This assumes the Cloud Storage connector for
    // Hadoop is configured.
    val bucket = spark.sparkContext.hadoopConfiguration.get("fs.gs.system.bucket")
    spark.conf.set("temporaryGcsBucket", bucket)

    // Load data in from BigQuery.
    val wordsDF = spark.read.bigquery("bigquery-public-data.samples.shakespeare").cache()
    wordsDF.show()
    wordsDF.printSchema()
    wordsDF.createOrReplaceTempView("words")

    // Perform word count.
     val wordCountDF = spark.sql(
      "SELECT word, SUM(word_count) AS word_count FROM words GROUP BY word")

    // Saving the data to BigQuery
    wordCountDF.write.bigquery("wordcount_dataset.wordcount_output")
  }
}
