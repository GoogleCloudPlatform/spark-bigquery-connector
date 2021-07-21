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

import com.google.cloud.bigquery.connector.common.ReadRowsHelper.Options;
import com.google.cloud.bigquery.storage.v1.DataFormat;

import java.util.Optional;
import java.util.OptionalInt;

public class ReadSessionCreatorConfig {
  private final boolean viewsEnabled;
  private final Optional<String> materializationProject;
  private final Optional<String> materializationDataset;
  private final String viewEnabledParamName;
  private final int materializationExpirationTimeInMinutes;
  private final DataFormat readDataFormat;
  private final int maxReadRowsRetries;
  private final OptionalInt maxParallelism;
  private final int defaultParallelism;
  private final Optional<String> requestEncodedBase;
  private final Optional<String> endpoint;
  private final int backgroundParsingThreads;
  private final boolean pushAllFilters;
  private final int prebufferResponses;
  private final int streamsPerPartition;

  ReadSessionCreatorConfig(
      boolean viewsEnabled,
      Optional<String> materializationProject,
      Optional<String> materializationDataset,
      int materializationExpirationTimeInMinutes,
      DataFormat readDataFormat,
      int maxReadRowsRetries,
      String viewEnabledParamName,
      OptionalInt maxParallelism,
      int defaultParallelism,
      Optional<String> requestEncodedBase,
      Optional<String> endpoint,
      int backgroundParsingThreads,
      boolean pushAllFilters,
      int prebufferResponses,
      int streamsPerPartition) {
    this.viewsEnabled = viewsEnabled;
    this.materializationProject = materializationProject;
    this.materializationDataset = materializationDataset;
    this.viewEnabledParamName = viewEnabledParamName;
    this.materializationExpirationTimeInMinutes = materializationExpirationTimeInMinutes;
    this.readDataFormat = readDataFormat;
    this.maxReadRowsRetries = maxReadRowsRetries;
    this.maxParallelism = maxParallelism;
    this.defaultParallelism = defaultParallelism;
    this.requestEncodedBase = requestEncodedBase;
    this.endpoint = endpoint;
    this.backgroundParsingThreads = backgroundParsingThreads;
    this.pushAllFilters = pushAllFilters;
    this.prebufferResponses = prebufferResponses;
    this.streamsPerPartition = streamsPerPartition;
  }

  public boolean isViewsEnabled() {
    return viewsEnabled;
  }

  public Optional<String> getMaterializationProject() {
    return materializationProject;
  }

  public Optional<String> getMaterializationDataset() {
    return materializationDataset;
  }

  public String getViewEnabledParamName() {
    return viewEnabledParamName;
  }

  public int getMaterializationExpirationTimeInMinutes() {
    return materializationExpirationTimeInMinutes;
  }

  public DataFormat getReadDataFormat() {
    return readDataFormat;
  }

  public int getMaxReadRowsRetries() {
    return maxReadRowsRetries;
  }

  public OptionalInt getMaxParallelism() {
    return maxParallelism;
  }

  public int getDefaultParallelism() {
    return defaultParallelism;
  }

  public Optional<String> getRequestEncodedBase() {
    return this.requestEncodedBase;
  }

  public Optional<String> endpoint() {
    return this.endpoint;
  }

  public int backgroundParsingThreads() {
    return this.backgroundParsingThreads;
  }

  public boolean getPushAllFilters() {
    return this.pushAllFilters;
  }

  public ReadRowsHelper.Options toReadRowsHelperOptions() {
    return new ReadRowsHelper.Options(
        getMaxReadRowsRetries(), endpoint(), backgroundParsingThreads(), getPrebufferResponses());
  }

  public int streamsPerPartition() {
    return streamsPerPartition;
  }

  public int getPrebufferResponses() {
    return prebufferResponses;
  }
}
