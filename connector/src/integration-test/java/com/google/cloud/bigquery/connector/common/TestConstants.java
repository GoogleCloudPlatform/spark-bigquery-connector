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

import com.google.common.collect.ImmutableList;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.types.*;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.Arrays;
import java.util.List;
import java.util.TimeZone;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.spark.sql.functions.*;

public class TestConstants {
    private static DataType BQ_NUMERIC = DataTypes.createDecimalType(38, 9);
    public static int BIG_NUMERIC_COLUMN_POSITION = 11;

    public static StructType ALL_TYPES_TABLE_SCHEMA = new StructType(copy(
            new StructField("int_req", DataTypes.LongType, false, Metadata.empty()),
            new StructField("int_null", DataTypes.LongType, true, Metadata.empty()),
            new StructField("bl", DataTypes.BooleanType, true, Metadata.empty()),
            new StructField("str", DataTypes.StringType, true, Metadata.empty()),
            new StructField("day", DataTypes.DateType, true, Metadata.empty()),
            new StructField("ts", DataTypes.TimestampType, true, Metadata.empty()),
            new StructField("dt", DataTypes.StringType, true, Metadata.empty()),
            new StructField("tm", DataTypes.LongType, true, Metadata.empty()),
            new StructField("binary", DataTypes.BinaryType, true, Metadata.empty()),
            new StructField("float", DataTypes.DoubleType, true, Metadata.empty()),
            new StructField("nums",
                    new StructType(copy(
                            new StructField("min", BQ_NUMERIC, true, Metadata.empty()),
                            new StructField("max", BQ_NUMERIC, true, Metadata.empty()),
                            new StructField("pi", BQ_NUMERIC, true, Metadata.empty()),
                            new StructField("big_pi", BQ_NUMERIC, true, Metadata.empty()))),
                    true, Metadata.empty()),
            new StructField("big_numeric_nums",
                    new StructType(copy(
                            new StructField("min", BigQueryDataTypes.BigNumericType, nullable = true),
                            new StructField("max", BigQueryDataTypes.BigNumericType, nullable = true))),
                    nullable = true),
            new StructField("int_arr", new ArrayType(DataTypes.LongType, true), true, Metadata.empty()),
            new StructField("int_struct_arr", new ArrayType(
                    new StructType(copy(new StructField("i", DataTypes.LongType, true, Metadata.empty()))), true),
                    true, Metadata.empty())
    ));

    public static String ALL_TYPES_TABLE_QUERY_TEMPLATE =
            // Use DDL to support required fields
            Stream.of(
                    "create table %s.%s (",
                    "int_req int64 not null,",
                    "int_null int64,",
                    "bl bool,",
                    "str string,",
                    "day date,",
                    "ts timestamp,",
                    "dt datetime,",
                    "tm time,",
                    "binary bytes,",
                    "float float64,",
                    "nums struct<min numeric, max numeric, pi numeric, big_pi numeric>,",
                    "big_numeric_nums struct<min bignumeric, max bignumeric>,",
                    "int_arr array<int64>,",
                    "int_struct_arr array<struct<i int64>>",
                    ") as",
                    "",
                    "select",
                    "42 as int_req,",
                    "null as int_null,",
                    "true as bl,",
                    "\"string\" as str,",
                    "cast(\"2019-03-18\" as date) as day,",
                    "cast(\"2019-03-18T01:23:45.678901\" as timestamp) as ts,",
                    "cast(\"2019-03-18T01:23:45.678901\"  as datetime) as dt,",
                    "cast(\"01:23:45.678901\" as time) as tm,",
                    "cast(\"bytes\" as bytes) as binary,",
                    "4.2 as float,",
                    "struct(",
                    "  cast(\"-99999999999999999999999999999.999999999\" as numeric) as min,",
                    "  cast(\"99999999999999999999999999999.999999999\" as numeric) as max,",
                    "  cast(3.14 as numeric) as pi,",
                    "  cast(\"31415926535897932384626433832.795028841\" as numeric) as big_pi",
                    ") as nums,",
                    "[1, 2, 3] as int_arr,",
                    "[(select as struct 1)] as int_struct_arr"
            ).collect(Collectors.joining("\n"));

    public static int ALL_TYPES_TABLE_SIZE = 224;

    public static List<Column> ALL_TYPES_TABLE_COLS = Arrays.asList(
            lit(42L),
            lit(null),
            lit(true),
            lit("string"),
            to_date(lit("2019-03-18")),
            from_utc_timestamp(lit("2019-03-18T01:23:45.678901"), TimeZone.getDefault().getID()),
            lit("2019-03-18T01:23:45.678901"),
            lit(5025678901L),
            lit("bytes").cast("BINARY"),
            lit(4.2),
            struct(seq(
                    lit("-99999999999999999999999999999.999999999").cast(BQ_NUMERIC),
                    lit("99999999999999999999999999999.999999999").cast(BQ_NUMERIC),
                    lit(3.14).cast(BQ_NUMERIC),
                    lit("31415926535897932384626433832.795028841").cast(BQ_NUMERIC))),
            struct(
                    lit("-578960446186580977117854925043439539266.34992332820282019728792003956564819968"),
                    lit("578960446186580977117854925043439539266.34992332820282019728792003956564819967")
            ),
            array(seq(lit(1), lit(2), lit(3))),
            array(seq(struct(seq(lit(1)))))
    );

    private static <T> T[] copy(T... elements) {
        return elements;
    }

    private static <T> Seq<T> seq(T... elements) {
        return JavaConverters.asScalaBufferConverter(ImmutableList.copyOf(elements)).asScala().toSeq();
    }
}
