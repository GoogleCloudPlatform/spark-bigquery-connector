/*
 * Copyright 2020 Google Inc. All Rights Reserved.
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
package com.google.cloud.spark.bigquery;

import com.google.cloud.bigquery.connector.common.BigQueryUtil;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Properties;

/** Spark related utilities */
public class SparkBigQueryUtil {

  static final Properties BUILD_PROPERTIES = loadBuildProperties();

  static final String CONNECTOR_VERSION = BUILD_PROPERTIES.getProperty("connector.version");

  private static Properties loadBuildProperties() {
    try {
      Properties buildProperties = new Properties();
      buildProperties.load(
          SparkBigQueryUtil.class.getResourceAsStream("/spark-bigquery-connector.properties"));
      return buildProperties;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
  /**
   * Optimizing the URI list for BigQuery load, using the Spark specific file prefix and suffix
   * patterns, based on <code>BigQueryUtil.optimizeLoadUriList()</code>
   *
   * @param uris A list of URIs to be loaded by BigQuery load
   * @return an optimized list of URIs
   */
  public static List<String> optimizeLoadUriListForSpark(List<String> uris) {
    return BigQueryUtil.optimizeLoadUriList(uris, ".*/part-", "-[-\\w\\.]+");
  }
}
