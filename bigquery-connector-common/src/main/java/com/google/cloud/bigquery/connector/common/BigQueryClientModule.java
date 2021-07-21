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
package com.google.cloud.bigquery.connector.common;

import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.http.HttpTransportOptions;
import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;

public class BigQueryClientModule implements com.google.inject.Module {

  @Provides
  @Singleton
  public static UserAgentHeaderProvider createUserAgentHeaderProvider(
      UserAgentProvider versionProvider) {
    return new UserAgentHeaderProvider(versionProvider.getUserAgent());
  }

  @Override
  public void configure(Binder binder) {
    // BigQuery related
    binder.bind(BigQueryReadClientFactory.class).in(Scopes.SINGLETON);
    binder
        .bind(BigQueryTracerFactory.class)
        .toInstance(new LoggingBigQueryTracerFactory(/*Log every 2^14 batches*/ 14));
  }

  @Provides
  @Singleton
  public BigQueryCredentialsSupplier provideBigQueryCredentialsSupplier(BigQueryConfig config) {
    return new BigQueryCredentialsSupplier(
        config.getAccessToken(), config.getCredentialsKey(), config.getCredentialsFile());
  }

  @Provides
  @Singleton
  public BigQueryClient provideBigQueryClient(
      BigQueryConfig config,
      UserAgentHeaderProvider userAgentHeaderProvider,
      BigQueryCredentialsSupplier bigQueryCredentialsSupplier) {
    BigQueryOptions.Builder options =
        BigQueryOptions.newBuilder()
            .setHeaderProvider(userAgentHeaderProvider)
            .setProjectId(config.getParentProjectId())
            .setCredentials(bigQueryCredentialsSupplier.getCredentials())
            .setRetrySettings(config.getBigQueryClientRetrySettings())
            .setTransportOptions(
                HttpTransportOptions.newBuilder()
                    .setConnectTimeout(config.getBigQueryClientConnectTimeout())
                    .setReadTimeout(config.getBigQueryClientReadTimeout())
                    .build());
    return new BigQueryClient(
        options.build().getService(),
        config.getMaterializationProject(),
        config.getMaterializationDataset());
  }
}
