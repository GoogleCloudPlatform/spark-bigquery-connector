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
package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.bigquery.*;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.cloud.bigquery.storage.v1.StreamStats;
import com.google.cloud.spark.bigquery.ReadRowsResponseToInternalRowIteratorConverter;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.TextFormat;
import java.util.Optional;
import org.apache.log4j.Logger;
import org.apache.spark.sql.catalyst.InternalRow;
import org.junit.Test;

import java.util.Iterator;

import static com.google.common.truth.Truth.*;

public class BigQueryInputPartitionReaderTest {

  private static final String ALL_TYPES_TABLE_AVRO_RAW_SCHEMA =
      "{\"type\":\"record\",\"name\":\"__root__\",\"fields\":[{\"name\":\"string_f\",\"type\":[\"null\",\"string\"]},{\"name\":\"bytes_f\",\"type\":[\"null\",\"bytes\"]},{\"name\":\"int_f\",\"type\":[\"null\",\"long\"]},{\"name\":\"float_f\",\"type\":[\"null\",\"double\"]},{\"name\":\"numeric_f\",\"type\":[\"null\",{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":38,\"scale\":9}]},{\"name\":\"boolean_f\",\"type\":[\"null\",\"boolean\"]},{\"name\":\"timestamp_f\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}]},{\"name\":\"date_f\",\"type\":[\"null\",{\"type\":\"int\",\"logicalType\":\"date\"}]},{\"name\":\"time_f\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"time-micros\"}]},{\"name\":\"datetime_f\",\"type\":[\"null\",{\"type\":\"string\",\"logicalType\":\"datetime\"}]},{\"name\":\"geo_f\",\"type\":[\"null\",{\"type\":\"string\",\"sqlType\":\"GEOGRAPHY\"}]},{\"name\":\"record_f\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"__s_0\",\"fields\":[{\"name\":\"s\",\"type\":[\"null\",\"string\"]},{\"name\":\"i\",\"type\":[\"null\",\"long\"]}]}]},{\"name\":\"null_f\",\"type\":[\"null\",\"string\"]},{\"name\":\"sarray_f\",\"type\":{\"type\":\"array\",\"items\":\"string\"}},{\"name\":\"iarray_f\",\"type\":[\"null\",\"long\"]}]}";
  private static final Schema ALL_TYPES_TABLE_BIGQUERY_SCHEMA =
      Schema.of(
          Field.of("string_f", LegacySQLTypeName.STRING),
          Field.of("bytes_f", LegacySQLTypeName.BYTES),
          Field.of("int_f", LegacySQLTypeName.INTEGER),
          Field.of("float_f", LegacySQLTypeName.FLOAT),
          Field.of("numeric_f", LegacySQLTypeName.NUMERIC),
          Field.of("boolean_f", LegacySQLTypeName.BOOLEAN),
          Field.of("timestamp_f", LegacySQLTypeName.TIMESTAMP),
          Field.of("date_f", LegacySQLTypeName.DATE),
          Field.of("time_f", LegacySQLTypeName.TIME),
          Field.of("datetime_f", LegacySQLTypeName.DATETIME),
          Field.of("geo_f", LegacySQLTypeName.GEOGRAPHY),
          Field.of(
              "record_f",
              LegacySQLTypeName.RECORD,
              Field.of("s", LegacySQLTypeName.STRING),
              Field.of("i", LegacySQLTypeName.INTEGER)),
          Field.of("null_f", LegacySQLTypeName.STRING),
          Field.newBuilder("sarray_f", LegacySQLTypeName.STRING)
              .setMode(Field.Mode.REPEATED)
              .build(),
          Field.newBuilder("iarray_f", LegacySQLTypeName.INTEGER)
              .setMode(Field.Mode.REPEATED)
              .build());
  private static final ImmutableList<String> ALL_TYPES_TABLE_FIELDS =
      ImmutableList.of(
          "string_f",
          "bytes_f",
          "int_f",
          "float_f",
          "numeric_f",
          "boolean_f",
          "timestamp_f",
          "date_f",
          "time_f",
          "datetime_f",
          "geo_f",
          "record_f",
          "null_f",
          "sarray_f",
          "iarray_f");

  private static final String ALL_TYPES_TABLE_READ_ROWS_RESPONSE_STR =
      "stats {\n"
          + "  progress {\n"
          + "    at_response_end: 0.5\n"
          + "  }\n"
          + "}\n"
          + "avro_rows {\n"
          + "  serialized_binary_rows: \"\\002\\nhello\\002\\b\\001\\002\\003\\004\\002\\310\\001\\0023333336@\\002 \\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\nX\\324\\226\\000\\002\\001\\002\\244\\234\\205\\342\\275\\242\\312\\005\\002\\204\\235\\002\\002\\340\\232\\206\\213\\330\\004\\00222019-11-11T11:11:11.11111\\002 POINT(31.2 44.5)\\002\\002\\fin_rec\\002\\\"\\000\\004\\006big\\nquery\\000\\000\"\n"
          + "  row_count: 1\n"
          + "}\n"
          + "row_count: 1\n";

  @Test
  public void testReadAvro() throws Exception {
    TableInfo allTypesTableInfo = allTypesTableInfo();
    ReadRowsResponse.Builder readRowsResponse = ReadRowsResponse.newBuilder();
    TextFormat.merge(ALL_TYPES_TABLE_READ_ROWS_RESPONSE_STR, readRowsResponse);
    Iterator<ReadRowsResponse> readRowsResponses =
        ImmutableList.of(readRowsResponse.build()).iterator();

    ReadRowsResponseToInternalRowIteratorConverter converter =
        ReadRowsResponseToInternalRowIteratorConverter.avro(
            ALL_TYPES_TABLE_BIGQUERY_SCHEMA,
            ALL_TYPES_TABLE_FIELDS,
            ALL_TYPES_TABLE_AVRO_RAW_SCHEMA,
            Optional.empty());

    BigQueryInputPartitionReader reader =
        new BigQueryInputPartitionReader(readRowsResponses, converter, null);

    assertThat(reader.next()).isTrue();
    InternalRow row = reader.get();
    assertThat(reader.next()).isFalse();
    assertThat(row.numFields()).isEqualTo(15);
    assertThat(row.getString(0)).isEqualTo("hello");
  }

  private TableInfo allTypesTableInfo() {
    return TableInfo.newBuilder(
            TableId.of("test", "alltypes"),
            StandardTableDefinition.newBuilder()
                .setType(TableDefinition.Type.TABLE)
                .setSchema(ALL_TYPES_TABLE_BIGQUERY_SCHEMA)
                .setNumBytes(146L)
                .setNumRows(1L)
                .build())
        .build();
  }
}
