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
package com.google.cloud.spark.bigquery;

import static org.apache.spark.sql.functions.array;
import static org.apache.spark.sql.functions.from_utc_timestamp;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.struct;
import static org.apache.spark.sql.functions.to_date;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.TimeZone;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.spark.bigquery.BigQueryDataTypes;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class TestConstants {

  public static int BIG_NUMERIC_COLUMN_POSITION = 11;
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
              "struct(",
              "  cast(\"-578960446186580977117854925043439539266.34992332820282019728792003956564819968\" as bignumeric) as min,",
              "  cast(\"578960446186580977117854925043439539266.34992332820282019728792003956564819967\" as bignumeric) as max",
              ") as big_numeric_nums,",
              "[1, 2, 3] as int_arr,",
              "[(select as struct 1)] as int_struct_arr")
          .collect(Collectors.joining("\n"));
  public static int ALL_TYPES_TABLE_SIZE = 224;
  public static String STRUCT_COLUMN_ORDER_TEST_TABLE_QUERY_TEMPLATE =
      // Use DDL to support required fields
      Stream.of(
              "create table %s (",
              "TypeDebugging.typeDebug.str string,",
              "nums struct <",
              "  num1 int64,",
              "  num2 int64,",
              "  num3 int64,",
              "  stringStructArr array <struct<str1 string, str2 string, str3 string>>",
              " >",
              ")",
              "as",
              "select",
              "\"outer_string\" as str,",
              "struct(",
              " 1 as num1,",
              " 2 as num2,",
              " 3 as num3,",
              " [",
              "   struct(\"0:str1\" as str1, \"0:str2\" as str2, \"0:str3\" as str3),",
              "   struct(\"1:str1\" as str1, \"1:str2\" as str2, \"1:str3\" as str3)",
              " ] as stringStructArr",
              ") as nums")
          .collect(Collectors.joining("\n"));
  public static ColumnOrderTestClass STRUCT_COLUMN_ORDER_TEST_TABLE_COLS =
      new ColumnOrderTestClass(
          new NumStruct(
              3L,
              2L,
              1L,
              ImmutableList.of(
                  new StringStruct("0:str3", "0:str1", "0:str2"),
                  new StringStruct("1:str3", "1:str1", "1:str2"))),
          "outer_string");
  private static DecimalType BQ_NUMERIC = DataTypes.createDecimalType(38, 9);
  public static StructType ALL_TYPES_TABLE_SCHEMA =
      new StructType(
          copy(
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
              new StructField(
                  "nums",
                  new StructType(
                      copy(
                          new StructField("min", BQ_NUMERIC, true, Metadata.empty()),
                          new StructField("max", BQ_NUMERIC, true, Metadata.empty()),
                          new StructField("pi", BQ_NUMERIC, true, Metadata.empty()),
                          new StructField("big_pi", BQ_NUMERIC, true, Metadata.empty()))),
                  true,
                  Metadata.empty()),
              new StructField(
                  "big_numeric_nums",
                  new StructType(
                      copy(
                          new StructField(
                              "min", BigQueryDataTypes.BigNumericType, true, Metadata.empty()),
                          new StructField(
                              "max", BigQueryDataTypes.BigNumericType, true, Metadata.empty()))),
                  true,
                  Metadata.empty()),
              new StructField(
                  "int_arr", new ArrayType(DataTypes.LongType, true), true, Metadata.empty()),
              new StructField(
                  "int_struct_arr",
                  new ArrayType(
                      new StructType(
                          copy(new StructField("i", DataTypes.LongType, true, Metadata.empty()))),
                      true),
                  true,
                  Metadata.empty())));
  public static Column[] ALL_TYPES_TABLE_COLS =
      new Column[] {
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
        struct(
            lit("-99999999999999999999999999999.999999999").cast(BQ_NUMERIC),
            lit("99999999999999999999999999999.999999999").cast(BQ_NUMERIC),
            lit(3.14).cast(BQ_NUMERIC),
            lit("31415926535897932384626433832.795028841").cast(BQ_NUMERIC)),
        struct(
            lit("-578960446186580977117854925043439539266.34992332820282019728792003956564819968"),
            lit("578960446186580977117854925043439539266.34992332820282019728792003956564819967")),
        array(lit(1), lit(2), lit(3)),
        array(struct(lit(1)))
      };

  private TestConstants() {}

  private static <T> T[] copy(T... elements) {
    return elements;
  }

  public static class StringStruct {
    private String str3;
    private String str1;
    private String str2;

    public StringStruct(String str3, String str1, String str2) {
      this.str3 = str3;
      this.str1 = str1;
      this.str2 = str2;
    }

    public String getStr3() {
      return str3;
    }

    public void setStr3(String str3) {
      this.str3 = str3;
    }

    public String getStr1() {
      return str1;
    }

    public void setStr1(String str1) {
      this.str1 = str1;
    }

    public String getStr2() {
      return str2;
    }

    public void setStr2(String str2) {
      this.str2 = str2;
    }
  }

  public static class NumStruct {
    private Long num3;
    private Long num2;
    private Long num1;
    private List<StringStruct> stringStructArr;

    public NumStruct(Long num3, Long num2, Long num1, List<StringStruct> stringStructArr) {
      this.num3 = num3;
      this.num2 = num2;
      this.num1 = num1;
      this.stringStructArr = stringStructArr;
    }

    public Long getNum3() {
      return num3;
    }

    public void setNum3(Long num3) {
      this.num3 = num3;
    }

    public Long getNum2() {
      return num2;
    }

    public void setNum2(Long num2) {
      this.num2 = num2;
    }

    public Long getNum1() {
      return num1;
    }

    public void setNum1(Long num1) {
      this.num1 = num1;
    }

    public List<StringStruct> getStringStructArr() {
      return stringStructArr;
    }

    public void setStringStructArr(List<StringStruct> stringStructArr) {
      this.stringStructArr = stringStructArr;
    }
  }

  public static class ColumnOrderTestClass {
    private NumStruct nums;
    private String str;

    public ColumnOrderTestClass(NumStruct nums, String str) {
      this.nums = nums;
      this.str = str;
    }

    public NumStruct getNums() {
      return nums;
    }

    public void setNums(NumStruct nums) {
      this.nums = nums;
    }

    public String getStr() {
      return str;
    }

    public void setStr(String str) {
      this.str = str;
    }
  }
}
