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

public enum BigQueryErrorCode {
  BIGQUERY_VIEW_DESTINATION_TABLE_CREATION_FAILED(0),
  BIGQUERY_DATETIME_PARSING_ERROR(1),
  BIGQUERY_FAILED_TO_EXECUTE_QUERY(2),
  // Should be last
  UNSUPPORTED(9998),
  UNKNOWN(9999);

  final int code;

  BigQueryErrorCode(int code) {
    this.code = code;
  }

  public int getCode() {
    return code;
  }
}
