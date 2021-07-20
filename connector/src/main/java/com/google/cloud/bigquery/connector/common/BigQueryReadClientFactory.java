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

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.auth.Credentials;
import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import com.google.cloud.bigquery.storage.v1.BigQueryReadSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.Optional;

/**
 * Since Guice recommends to avoid injecting closeable resources (see
 * https://github.com/google/guice/wiki/Avoid-Injecting-Closable-Resources), this factory creates
 * short lived clients that can be closed independently.
 */
public class BigQueryReadClientFactory implements Serializable {
  private static final Logger log = LoggerFactory.getLogger(BigQueryReadClientFactory.class);

  private final Credentials credentials;
  // using the user agent as HeaderProvider is not serializable
  private final UserAgentHeaderProvider userAgentHeaderProvider;
  private final BigQueryConfig bqConfig;

  @Inject
  public BigQueryReadClientFactory(
      BigQueryCredentialsSupplier bigQueryCredentialsSupplier,
      UserAgentHeaderProvider userAgentHeaderProvider,
      BigQueryConfig bqConfig) {
    // using Guava's optional as it is serializable
    this.credentials = bigQueryCredentialsSupplier.getCredentials();
    this.userAgentHeaderProvider = userAgentHeaderProvider;
    this.bqConfig = bqConfig;
  }

  public BigQueryReadClient createBigQueryReadClient(Optional<String> endpoint) {
    try {
      InstantiatingGrpcChannelProvider.Builder transportBuilder =
          BigQueryReadSettings.defaultGrpcTransportProviderBuilder()
              .setHeaderProvider(userAgentHeaderProvider);
      BigQueryProxyConfig proxyConfig = bqConfig.getBigQueryProxyConfig();
      if (proxyConfig.getProxyUri().isPresent()) {
        transportBuilder.setChannelConfigurator(
            BigQueryProxyTransporterBuilder.createGrpcChannelConfigurator(
                proxyConfig.getProxyUri(),
                proxyConfig.getProxyUsername(),
                proxyConfig.getProxyPassword()));
      }
      endpoint.ifPresent(
          e -> {
            log.info("Overriding endpoint to: ", e);
            transportBuilder.setEndpoint(e);
          });
      BigQueryReadSettings.Builder clientSettings =
          BigQueryReadSettings.newBuilder()
              .setTransportChannelProvider(transportBuilder.build())
              .setCredentialsProvider(FixedCredentialsProvider.create(credentials));
      return BigQueryReadClient.create(clientSettings.build());
    } catch (IOException e) {
      throw new UncheckedIOException("Error creating BigQueryStorageClient", e);
    }
  }
}
