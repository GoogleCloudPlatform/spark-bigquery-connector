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

import com.google.api.client.util.Base64;
import com.google.auth.Credentials;
import com.google.auth.http.HttpTransportFactory;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.Optional;

public class BigQueryCredentialsSupplier {
  private final Credentials credentials;

  public BigQueryCredentialsSupplier(
      Optional<String> accessToken,
      Optional<String> credentialsKey,
      Optional<String> credentialsFile,
      Optional<URI> proxyUri,
      Optional<String> proxyUsername,
      Optional<String> proxyPassword) {
    if (accessToken.isPresent()) {
      this.credentials = createCredentialsFromAccessToken(accessToken.get());
    } else if (credentialsKey.isPresent()) {
      this.credentials =
          createCredentialsFromKey(credentialsKey.get(), proxyUri, proxyUsername, proxyPassword);
    } else if (credentialsFile.isPresent()) {
      this.credentials =
          createCredentialsFromFile(credentialsFile.get(), proxyUri, proxyUsername, proxyPassword);
    } else {
      this.credentials = createDefaultCredentials();
    }
  }

  private static Credentials createCredentialsFromAccessToken(String accessToken) {
    return GoogleCredentials.create(new AccessToken(accessToken, null));
  }

  private static Credentials createCredentialsFromKey(
      String key,
      Optional<URI> proxyUri,
      Optional<String> proxyUsername,
      Optional<String> proxyPassword) {
    try {
      if (proxyUri.isPresent()) {
        HttpTransportFactory httpTransportFactory =
            BigQueryProxyTransporterBuilder.createHttpTransportFactory(
                proxyUri, proxyUsername, proxyPassword);
        return GoogleCredentials.fromStream(
            new ByteArrayInputStream(Base64.decodeBase64(key)), httpTransportFactory);
      } else {
        return GoogleCredentials.fromStream(new ByteArrayInputStream(Base64.decodeBase64(key)));
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to create Credentials from key", e);
    }
  }

  private static Credentials createCredentialsFromFile(
      String file,
      Optional<URI> proxyUri,
      Optional<String> proxyUsername,
      Optional<String> proxyPassword) {
    try {
      if (proxyUri.isPresent()) {
        HttpTransportFactory httpTransportFactory =
            BigQueryProxyTransporterBuilder.createHttpTransportFactory(
                proxyUri, proxyUsername, proxyPassword);
        return GoogleCredentials.fromStream(new FileInputStream(file), httpTransportFactory);
      } else {
        return GoogleCredentials.fromStream(new FileInputStream(file));
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to create Credentials from file", e);
    }
  }

  public static Credentials createDefaultCredentials() {
    try {
      return GoogleCredentials.getApplicationDefault();
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to create default Credentials", e);
    }
  }

  public Credentials getCredentials() {
    return credentials;
  }
}
