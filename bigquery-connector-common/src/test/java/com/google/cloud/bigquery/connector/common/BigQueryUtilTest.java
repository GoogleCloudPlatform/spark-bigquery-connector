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
import org.junit.Test;

import java.util.Optional;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

public class BigQueryUtilTest {

  private static final TableId TABLE_ID =
      TableId.of("test.org:test-project", "test_dataset", "test_table");
  private static final String FULLY_QUALIFIED_TABLE =
      "test.org:test-project.test_dataset.test_table";

  @Test
  public void testParseFullyQualifiedTable() {
    TableId tableId = BigQueryUtil.parseTableId(FULLY_QUALIFIED_TABLE);
    assertThat(tableId).isEqualTo(TABLE_ID);
  }

  @Test
  public void testParseFullyQualifiedLegacyTable() {
    TableId tableId = BigQueryUtil.parseTableId("test.org:test-project.test_dataset.test_table");
    assertThat(tableId).isEqualTo(TABLE_ID);
  }

  @Test
  public void testParseInvalidTable() {
    assertThrows(
        IllegalArgumentException.class,
        () -> BigQueryUtil.parseTableId("test-org:test-project.table"));
  }

  @Test
  public void testParseFullyQualifiedTableWithDefaults() {
    TableId tableId =
        BigQueryUtil.parseTableId(
            FULLY_QUALIFIED_TABLE, Optional.of("other_dataset"), Optional.of("other-project"));
    assertThat(tableId).isEqualTo(TABLE_ID);
  }

  @Test
  public void testParsePartiallyQualifiedTable() {
    TableId tableId = BigQueryUtil.parseTableId("test_dataset.test_table");
    assertThat(tableId).isEqualTo(TableId.of("test_dataset", "test_table"));
  }

  @Test
  public void testParsePartiallyQualifiedTableWithDefaults() {
    TableId tableId =
        BigQueryUtil.parseTableId(
            "test_dataset.test_table",
            Optional.of("other_dataset"),
            Optional.of("default-project"));
    assertThat(tableId).isEqualTo(TableId.of("default-project", "test_dataset", "test_table"));
  }

  @Test
  public void testParseUnqualifiedTableWithDefaults() {
    TableId tableId =
        BigQueryUtil.parseTableId(
            "test_table", Optional.of("default_dataset"), Optional.of("default-project"));
    assertThat(tableId).isEqualTo(TableId.of("default-project", "default_dataset", "test_table"));
  }

  @Test
  public void testParseFullyQualifiedPartitionedTable() {
    TableId tableId = BigQueryUtil.parseTableId(FULLY_QUALIFIED_TABLE + "$12345");
    assertThat(tableId)
        .isEqualTo(TableId.of("test.org:test-project", "test_dataset", "test_table$12345"));
  }

  @Test
  public void testParseUnqualifiedPartitionedTable() {
    TableId tableId =
        BigQueryUtil.parseTableId(
            "test_table$12345", Optional.of("default_dataset"), Optional.empty());
    assertThat(tableId).isEqualTo(TableId.of("default_dataset", "test_table$12345"));
  }

  @Test
  public void testParseTableWithDatePartition() {
    TableId tableId =
        BigQueryUtil.parseTableId(
            "test_table",
            Optional.of("default_dataset"),
            Optional.empty(),
            Optional.of("20200101"));
    assertThat(tableId).isEqualTo(TableId.of("default_dataset", "test_table$20200101"));
  }

  @Test
  public void testUnparsableTable() {
    assertThrows(IllegalArgumentException.class, () -> BigQueryUtil.parseTableId("foo:bar:baz"));
  }

  @Test
  public void testFriendlyName() {
    String name = BigQueryUtil.friendlyTableName(TABLE_ID);
    assertThat(name).isEqualTo(FULLY_QUALIFIED_TABLE);
  }

  @Test
  public void testShortFriendlyName() {
    String name = BigQueryUtil.friendlyTableName(TableId.of("test_dataset", "test_table"));
    assertThat(name).isEqualTo("test_dataset.test_table");
  }

  @Test
  public void testConvertAndThrows() {
    final BigQueryError bigQueryError = new BigQueryError("reason", "location", "message");
    BigQueryException bigQueryException =
        assertThrows(BigQueryException.class, () -> BigQueryUtil.convertAndThrow(bigQueryError));
    assertThat(bigQueryException).hasMessageThat().isEqualTo("message");
    assertThat(bigQueryException.getError()).isEqualTo(bigQueryError);
  }

  @Test
  public void testFirstPresent() {
    assertThat(BigQueryUtil.firstPresent(Optional.empty(), Optional.of("a")))
        .isEqualTo(Optional.of("a"));
    assertThat(BigQueryUtil.firstPresent(Optional.empty(), Optional.of("a"), Optional.of("b")))
        .isEqualTo(Optional.of("a"));
    assertThat(BigQueryUtil.firstPresent(Optional.of("a"), Optional.empty()))
        .isEqualTo(Optional.of("a"));
    assertThat(BigQueryUtil.firstPresent(Optional.empty())).isEqualTo(Optional.empty());
  }
}
