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
package com.google.cloud.spark.bigquery.v2;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;

import java.io.IOException;

class BigQueryEmptyProjectionInputPartitionReader implements InputPartitionReader<InternalRow> {

  final int partitionSize;
  int currentIndex;

  BigQueryEmptyProjectionInputPartitionReader(int partitionSize) {
    this.partitionSize = partitionSize;
    this.currentIndex = 0;
  }

  @Override
  public boolean next() throws IOException {
    return currentIndex < partitionSize;
  }

  @Override
  public InternalRow get() {
    currentIndex++;
    return InternalRow.empty();
  }

  @Override
  public void close() throws IOException {
    // empty
  }
}
