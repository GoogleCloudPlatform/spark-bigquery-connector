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
package com.google.cloud.spark.bigquery;

import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.cloud.bigquery.connector.common.UserAgentProvider;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.CharStreams;
import com.google.common.io.Closeables;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Optional;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import scala.util.Properties;

/** Provides the versions of the client environment in an anonymous way. */
public class SparkBigQueryConnectorUserAgentProvider implements UserAgentProvider {

  @VisibleForTesting
  static String GCP_REGION_PART = getGcpRegion().map(region -> " region/" + region).orElse("");

  @VisibleForTesting
  static String DATAPROC_IMAGE_PART =
      Optional.ofNullable(System.getenv("DATAPROC_IMAGE_VERSION"))
          .map(image -> " dataproc-image/" + image)
          .orElse("");

  // In order to avoid using SparkContext or SparkSession, we are going directly to the source
  private static String SPARK_VERSION = org.apache.spark.package$.MODULE$.SPARK_VERSION();
  private static String JAVA_VERSION = System.getProperty("java.runtime.version");
  private static String SCALA_VERSION = Properties.versionNumberString();
  static final String USER_AGENT =
      format(
          "spark-bigquery-connector/%s spark/%s java/%s scala/%s%s%s",
          SparkBigQueryUtil.CONNECTOR_VERSION,
          SPARK_VERSION,
          JAVA_VERSION,
          SCALA_VERSION,
          GCP_REGION_PART,
          DATAPROC_IMAGE_PART);

  private String dataSourceVersion;

  public SparkBigQueryConnectorUserAgentProvider(String dataSourceVersion) {
    this.dataSourceVersion = dataSourceVersion;
  }

  // Queries the GCE metadata server
  @VisibleForTesting
  static Optional<String> getGcpRegion() {
    RequestConfig config =
        RequestConfig.custom()
            .setConnectTimeout(100)
            .setConnectionRequestTimeout(100)
            .setSocketTimeout(100)
            .build();
    CloseableHttpClient httpClient = HttpClients.custom().setDefaultRequestConfig(config).build();
    HttpGet httpGet =
        new HttpGet("http://metadata.google.internal/computeMetadata/v1/instance/zone");
    httpGet.addHeader("Metadata-Flavor", "Google");
    try (CloseableHttpResponse response = httpClient.execute(httpGet)) {
      if (response.getStatusLine().getStatusCode() == 200) {
        String body =
            CharStreams.toString(new InputStreamReader(response.getEntity().getContent(), UTF_8));
        return Optional.of(body.substring(body.lastIndexOf('/') + 1));
      } else {
        return Optional.empty();
      }
    } catch (Exception e) {
      return Optional.empty();
    } finally {
      try {
        Closeables.close(httpClient, true);
      } catch (IOException e) {
        // nothing to do
      }
    }
  }

  @Override
  public String getUserAgent() {
    return USER_AGENT + " datasource/" + dataSourceVersion;
  }
}
