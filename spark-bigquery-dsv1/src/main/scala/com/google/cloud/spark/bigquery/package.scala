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
package com.google.cloud.spark

import org.apache.spark.sql._

package object bigquery {

  implicit class BigQueryDataFrameReader(reader: DataFrameReader) {
    def bigquery(table: String): DataFrame = {
      reader.format("bigquery").option("table", table).load()
    }
  }

  implicit class BigQueryDataFrameWriter[T](writer: DataFrameWriter[T]) {
    def bigquery(table: String): Unit = {
      writer.format("bigquery").option("table", table).save()
    }
  }
}
