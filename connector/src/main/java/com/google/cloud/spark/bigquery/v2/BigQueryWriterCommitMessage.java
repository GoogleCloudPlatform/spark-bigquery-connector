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

import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;

public class BigQueryWriterCommitMessage implements WriterCommitMessage {

  private final String writeStreamName;
  private final int partitionId;
  private final long taskId;
  private final long epochId;
  private final String tablePath;
  private final long rowCount;

  public BigQueryWriterCommitMessage(
      String writeStreamName /*List<String> writeStreamNames*/,
      int partitionId,
      long taskId,
      long epochId,
      String tablePath,
      long rowCount) {
    this.writeStreamName = writeStreamName;
    this.partitionId = partitionId;
    this.taskId = taskId;
    this.epochId = epochId;
    this.tablePath = tablePath;
    this.rowCount = rowCount;
  }

  public String getWriteStreamName() {
    return writeStreamName;
  }

  public int getPartitionId() {
    return partitionId;
  }

  public long getTaskId() {
    return taskId;
  }

  public long getEpochId() {
    return epochId;
  }

  public String getTablePath() {
    return tablePath;
  }

  public long getRowCount() {
    return rowCount;
  }

  @Override
  public String toString() {
    return "BigQueryWriterCommitMessage{"
        + "partitionId="
        + partitionId
        + ", taskId="
        + taskId
        + ", epochId="
        + epochId
        + ", tableId='"
        + tablePath
        + '\''
        + '}';
  }
}
