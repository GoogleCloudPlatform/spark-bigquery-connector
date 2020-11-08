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

import com.google.cloud.bigquery.storage.v1.DataFormat;

import java.util.Optional;
import java.util.OptionalInt;

public class ReadSessionCreatorConfig {
  final boolean viewsEnabled;
  final Optional<String> materializationProject;
  final Optional<String> materializationDataset;
  final String viewEnabledParamName;
  final int viewExpirationTimeInHours;
  final DataFormat readDataFormat;
  final int maxReadRowsRetries;
  final int maxParallelism;

  public ReadSessionCreatorConfig(
      boolean viewsEnabled,
      Optional<String> materializationProject,
      Optional<String> materializationDataset,
      int viewExpirationTimeInHours,
      DataFormat readDataFormat,
      int maxReadRowsRetries,
      String viewEnabledParamName,
      int maxParallelism) {
    this.viewsEnabled = viewsEnabled;
    this.materializationProject = materializationProject;
    this.materializationDataset = materializationDataset;
    this.viewEnabledParamName = viewEnabledParamName;
    this.viewExpirationTimeInHours = viewExpirationTimeInHours;
    this.readDataFormat = readDataFormat;
    this.maxReadRowsRetries = maxReadRowsRetries;
    this.maxParallelism = maxParallelism;
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

  public int getViewExpirationTimeInHours() {
    return viewExpirationTimeInHours;
  }

  public DataFormat getReadDataFormat() {
    return readDataFormat;
  }

  public int getMaxReadRowsRetries() {
    return maxReadRowsRetries;
  }

  public int getMaxParallelism() {
    return maxParallelism;
  }
}
