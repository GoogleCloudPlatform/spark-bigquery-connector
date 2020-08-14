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
import com.google.auth.Credentials;
import com.google.cloud.bigquery.storage.v1alpha2.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1alpha2.BigQueryWriteSettings;

import javax.inject.Inject;
import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;

public class BigQueryWriteClientFactory implements Serializable {
  private final Credentials credentials;
  // using the user agent as HeaderProvider is not serializable
  private final UserAgentHeaderProvider userAgentHeaderProvider;

  @Inject
  public BigQueryWriteClientFactory(
      BigQueryCredentialsSupplier bigQueryCredentialsSupplier,
      UserAgentHeaderProvider userAgentHeaderProvider) {
    // using Guava's optional as it is serializable
    this.credentials = bigQueryCredentialsSupplier.getCredentials();
    this.userAgentHeaderProvider = userAgentHeaderProvider;
  }

  public BigQueryWriteClient createBigQueryWriteClient() {
    try {
      BigQueryWriteSettings.Builder clientSettings =
          BigQueryWriteSettings.newBuilder()
              .setTransportChannelProvider(
                  BigQueryWriteSettings.defaultGrpcTransportProviderBuilder()
                      .setHeaderProvider(userAgentHeaderProvider)
                      .build())
              .setCredentialsProvider(FixedCredentialsProvider.create(credentials));
      return BigQueryWriteClient.create(clientSettings.build());
    } catch (IOException e) {
      throw new BigQueryConnectorException("Error creating BigQueryWriteClient", e);
    }
  }
}
