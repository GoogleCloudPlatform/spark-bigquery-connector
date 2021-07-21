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

import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.TableId;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.cloud.http.BaseHttpServiceException.UNKNOWN_CODE;
import static com.google.common.base.Throwables.getCausalChain;
import static java.lang.String.format;

public class BigQueryUtil {
  static final ImmutableSet<String> INTERNAL_ERROR_MESSAGES =
      ImmutableSet.of(
          "HTTP/2 error code: INTERNAL_ERROR",
          "Connection closed with unknown cause",
          "Received unexpected EOS on DATA frame from server");
  private static final String PROJECT_PATTERN = "\\S+";
  private static final String DATASET_PATTERN = "\\w+";
  // Allow all non-whitespace beside ':' and '.'.
  // These confuse the qualified table parsing.
  private static final String TABLE_PATTERN = "[\\S&&[^.:]]+";
  /**
   * Regex for an optionally fully qualified table.
   *
   * <p>Must match 'project.dataset.table' OR the legacy 'project:dataset.table' OR 'dataset.table'
   * OR 'table'.
   */
  private static final Pattern QUALIFIED_TABLE_REGEX =
      Pattern.compile(
          format("^(((%s)[:.])?(%s)\\.)?(%s)$$", PROJECT_PATTERN, DATASET_PATTERN, TABLE_PATTERN));

  private BigQueryUtil() {}

  public static boolean isRetryable(Throwable cause) {
    return getCausalChain(cause).stream().anyMatch(BigQueryUtil::isRetryableInternalError);
  }

  static boolean isRetryableInternalError(Throwable t) {
    if (t instanceof StatusRuntimeException) {
      StatusRuntimeException statusRuntimeException = (StatusRuntimeException) t;
      return statusRuntimeException.getStatus().getCode() == Status.Code.INTERNAL
          && INTERNAL_ERROR_MESSAGES.stream()
              .anyMatch(message -> statusRuntimeException.getMessage().contains(message));
    }
    return false;
  }

  static BigQueryException convertToBigQueryException(BigQueryError error) {
    return new BigQueryException(UNKNOWN_CODE, error.getMessage(), error);
  }

  // returns the first present optional, empty if all parameters are empty
  public static <T> Optional<T> firstPresent(Optional<T>... optionals) {
    for (Optional<T> o : optionals) {
      if (o.isPresent()) {
        return o;
      }
    }
    return Optional.empty();
  }

  public static TableId parseTableId(String rawTable) {
    return parseTableId(rawTable, Optional.empty(), Optional.empty(), Optional.empty());
  }

  public static TableId parseTableId(
      String rawTable, Optional<String> dataset, Optional<String> project) {
    return parseTableId(rawTable, dataset, project, Optional.empty());
  }

  public static TableId parseTableId(
      String rawTable,
      Optional<String> dataset,
      Optional<String> project,
      Optional<String> datePartition) {
    Matcher matcher = QUALIFIED_TABLE_REGEX.matcher(rawTable);
    if (!matcher.matches()) {
      throw new IllegalArgumentException(
          format("Invalid Table ID '%s'. Must match '%s'", rawTable, QUALIFIED_TABLE_REGEX));
    }
    String table = matcher.group(5);
    Optional<String> parsedDataset = Optional.ofNullable(matcher.group(4));
    Optional<String> parsedProject = Optional.ofNullable(matcher.group(3));
    String tableAndPartition =
        datePartition.map(date -> String.format("%s$%s", table, date)).orElse(table);
    String actualDataset =
        firstPresent(parsedDataset, dataset)
            .orElseThrow(() -> new IllegalArgumentException("'dataset' not parsed or provided."));
    return firstPresent(parsedProject, project)
        .map(p -> TableId.of(p, actualDataset, tableAndPartition))
        .orElse(TableId.of(actualDataset, tableAndPartition));
  }

  public static String friendlyTableName(TableId tableId) {
    return tableId.getProject() != null
        ? String.format("%s.%s.%s", tableId.getProject(), tableId.getDataset(), tableId.getTable())
        : String.format("%s.%s", tableId.getDataset(), tableId.getTable());
  }

  public static void convertAndThrow(BigQueryError error) {
    throw new BigQueryException(UNKNOWN_CODE, error.getMessage(), error);
  }

  /**
   * Solving Issue #248. As the bigquery load API can handle wildcard characters, we replace (a) 10
   * URIs ending in XXX[0-9].suffix with XXX*.suffix; (b) 10 URIs ending in XXX[0-9]*.suffix with
   * XXX*.suffix; This reduces the total number of URIs sent to the BigQuery API
   *
   * @param uris the list of URIs where the data was written to
   * @param prefixRegex the regex to catch the part before the file index.
   * @param suffixRegex the regex to catch the part after the file index.
   */
  public static List<String> optimizeLoadUriList(
      List<String> uris, String prefixRegex, String suffixRegex) {
    Pattern pattern = Pattern.compile("(" + prefixRegex + "\\d*)\\d\\**(" + suffixRegex + ")");
    ImmutableList.Builder<String> result = ImmutableList.builder();
    List<String> workingList = uris;
    while (!workingList.isEmpty()) {
      Multimap<String, String> trimmedUriMap = trimUris(pattern, workingList);
      List<String> nextList = new ArrayList<>();
      for (String trimmedUri : trimmedUriMap.keySet()) {
        Collection<String> mappedUris = trimmedUriMap.get(trimmedUri);
        if (mappedUris.size() == 10) {
          // All URIs exists, we can replace the last character with '*' in the next iteration
          nextList.add(trimmedUri);
        } else {
          // not all suffixes exist, so we can't optimize further.
          result.addAll(mappedUris);
        }
      }
      // cleanup and prepare next iteration
      trimmedUriMap.clear();
      workingList = nextList;
    }
    return result.build();
  }

  private static Multimap<String, String> trimUris(Pattern pattern, List<String> workingList) {
    Multimap<String, String> trimmedUriMap = HashMultimap.create();
    // converting
    // prefix-1234.suffix to prefix-123*.suffix
    // prefix-123*.suffix to prefix-12*.suffix
    for (String uri : workingList) {
      String trimmedUri = pattern.matcher(uri).replaceFirst("$1*$2");
      trimmedUriMap.put(trimmedUri, uri);
    }
    return trimmedUriMap;
  }
}
