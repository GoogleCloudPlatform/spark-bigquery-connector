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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Responsible for recursively deleting the intermediate path. Implementing Runnable in order to act
 * as shutdown hook.
 */
class IntermediateDataCleaner extends Thread {
  private static final Logger logger = LoggerFactory.getLogger(IntermediateDataCleaner.class);

  /** the path to delete */
  private final Path path;
  /** the hadoop configuration */
  private final Configuration conf;

  IntermediateDataCleaner(Path path, Configuration conf) {
    this.path = path;
    this.conf = conf;
  }

  @Override
  public void run() {
    deletePath();
  }

  void deletePath() {
    try {
      FileSystem fs = path.getFileSystem(conf);
      if (pathExists(fs, path)) {
        fs.delete(path, true);
      }
    } catch (Exception e) {
      logger.error("Failed to delete path " + path, e);
    }
  }
  // fs.exists can throw exception on missing path
  private boolean pathExists(FileSystem fs, Path path) {
    try {
      return fs.exists(path);
    } catch (Exception e) {
      return false;
    }
  }
}
