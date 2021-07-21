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
package com.google.cloud.spark.bigquery.examples;

import java.io.PrintStream;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class JavaShakespeare {

  public static void main(String[] args) {
    SparkSession spark = SparkSession.builder().appName("spark-bigquery-demo").getOrCreate();

    String outputBigqueryTable = "wordcount_dataset.wordcount_output";
    if (args.length == 1) {
      outputBigqueryTable = args[0];
    } else if (args.length > 1) {
      usage();
    }

    // Use the Cloud Storage bucket for temporary BigQuery export data used
    // by the connector. This assumes the Cloud Storage connector for
    // Hadoop is configured.
    String bucket = spark.sparkContext().hadoopConfiguration().get("fs.gs.system.bucket");
    spark.conf().set("temporaryGcsBucket", bucket);

    // Load data in from BigQuery.
    Dataset<Row> wordsDF =
        spark
            .read()
            .format("bigquery")
            .option("table", "bigquery-public-data.samples.shakespeare")
            .load()
            .cache();

    wordsDF.show();
    wordsDF.printSchema();
    wordsDF.createOrReplaceTempView("words");

    // Perform word count.
    Dataset<Row> wordCountDF =
        spark.sql("SELECT word, SUM(word_count) AS word_count FROM words GROUP BY word");

    wordCountDF.show();
    wordCountDF.printSchema();

    // Saving the data to BigQuery
    wordCountDF.write().format("bigquery").option("table", outputBigqueryTable).save();
  }

  private static void usage() {
    PrintStream out = System.out;
    out.println("usage: spark [OUTPUT_BIGQUERY_TABLE]");
    out.println("[OUTPUT_BIGQUERY_TABLE]       Set the output bigquery table to ");
    out.println("                              OUTPUT_BIGQUERY_TABLE. By default the location");
    out.println("                              is wordcount_dataset.wordcount_output");
    System.exit(1);
  }
}
