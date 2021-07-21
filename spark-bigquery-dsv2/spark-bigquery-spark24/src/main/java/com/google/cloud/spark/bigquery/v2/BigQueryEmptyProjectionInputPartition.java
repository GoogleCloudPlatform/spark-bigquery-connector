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
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;

public class BigQueryEmptyProjectionInputPartition implements InputPartition<InternalRow> {

  final int partitionSize;

  public BigQueryEmptyProjectionInputPartition(int partitionSize) {
    this.partitionSize = partitionSize;
  }

  @Override
  public InputPartitionReader<InternalRow> createPartitionReader() {
    return new BigQueryEmptyProjectionInputPartitionReader(partitionSize);
  }
}
