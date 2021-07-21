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

import com.google.cloud.bigquery._
import com.google.common.base.Preconditions
import org.apache.spark.sql.SparkSession
import org.mockito.Mockito._

object TestUtils {
  def getOrCreateSparkSession(applicationName: String): SparkSession = {
    SparkSession.builder()
      .appName(applicationName)
      .master("local")
      .getOrCreate()
  }

  def table(info: TableInfo): Table = {
    // Visibility hacks
    val table = mock(classOf[Table])
    when(table.getTableId).thenReturn(info.getTableId)
    // TODO(pmkc): Handle other types
    when(table.getDefinition)
      .thenReturn(info.getDefinition[StandardTableDefinition])
    Preconditions.checkNotNull(table.getDefinition[TableDefinition])
    Preconditions.checkNotNull(table.getDefinition[StandardTableDefinition])
    val tableDefinition = table.asInstanceOf[TableInfo].getDefinition[TableDefinition]
    require(table.getTableId != null)
    require(tableDefinition != null)
    table
  }
}
