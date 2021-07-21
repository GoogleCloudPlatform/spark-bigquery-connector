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

import com.google.cloud.bigquery.connector.common.ReadRowsHelper;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.cloud.spark.bigquery.ReadRowsResponseToInternalRowIteratorConverter;
import com.google.common.collect.ImmutableList;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;

import java.io.IOException;
import java.util.Iterator;

class BigQueryInputPartitionReader implements InputPartitionReader<InternalRow> {

  private Iterator<ReadRowsResponse> readRowsResponses;
  private ReadRowsResponseToInternalRowIteratorConverter converter;
  private ReadRowsHelper readRowsHelper;
  private Iterator<InternalRow> rows = ImmutableList.<InternalRow>of().iterator();
  private InternalRow currentRow;

  BigQueryInputPartitionReader(
      Iterator<ReadRowsResponse> readRowsResponses,
      ReadRowsResponseToInternalRowIteratorConverter converter,
      ReadRowsHelper readRowsHelper) {
    this.readRowsResponses = readRowsResponses;
    this.converter = converter;
    this.readRowsHelper = readRowsHelper;
  }

  @Override
  public boolean next() throws IOException {
    while (!rows.hasNext()) {
      if (!readRowsResponses.hasNext()) {
        return false;
      }
      ReadRowsResponse readRowsResponse = readRowsResponses.next();
      rows = converter.convert(readRowsResponse);
    }
    currentRow = rows.next();
    return true;
  }

  @Override
  public InternalRow get() {
    return currentRow;
  }

  @Override
  public void close() throws IOException {
    readRowsHelper.close();
  }
}
