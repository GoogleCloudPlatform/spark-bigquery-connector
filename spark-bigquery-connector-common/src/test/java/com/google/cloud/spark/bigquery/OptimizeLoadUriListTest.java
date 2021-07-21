/*
 * Copyright 2020 Google Inc. All Rights Reserved.
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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.truth.Truth.assertThat;

@RunWith(Parameterized.class)
public class OptimizeLoadUriListTest {

  private List<String> input;
  private List<String> expected;

  public OptimizeLoadUriListTest(List<String> input, List<String> expected) {
    this.input = input;
    this.expected = expected;
  }

  @Parameterized.Parameters(name = "{index}: Should get {1}")
  public static Iterable<Object[]> data() {
    return Arrays.asList(
        new Object[] {
          // input 1
          Arrays.asList(
              "gs://bucket/path/to/part-00000-bc3a92eb-414e-4110-9fd4-70cc41e6c43b-c000.csv",
              "gs://bucket/path/to/part-00001-bc3a92eb-414e-4110-9fd4-70cc41e6c43b-c000.csv"),
          // output 1
          Arrays.asList(
              "gs://bucket/path/to/part-00000-bc3a92eb-414e-4110-9fd4-70cc41e6c43b-c000.csv",
              "gs://bucket/path/to/part-00001-bc3a92eb-414e-4110-9fd4-70cc41e6c43b-c000.csv")
        },
        new Object[] {
          // input 2
          Arrays.asList(
              "gs://bucket/path/to/part-00000-2a70634c-f292-46f4-ba71-36a4af58f777-c000.avro",
              "gs://bucket/path/to/part-00001-2a70634c-f292-46f4-ba71-36a4af58f777-c000.avro",
              "gs://bucket/path/to/part-00002-2a70634c-f292-46f4-ba71-36a4af58f777-c000.avro",
              "gs://bucket/path/to/part-00003-2a70634c-f292-46f4-ba71-36a4af58f777-c000.avro",
              "gs://bucket/path/to/part-00004-2a70634c-f292-46f4-ba71-36a4af58f777-c000.avro",
              "gs://bucket/path/to/part-00005-2a70634c-f292-46f4-ba71-36a4af58f777-c000.avro",
              "gs://bucket/path/to/part-00006-2a70634c-f292-46f4-ba71-36a4af58f777-c000.avro",
              "gs://bucket/path/to/part-00007-2a70634c-f292-46f4-ba71-36a4af58f777-c000.avro",
              "gs://bucket/path/to/part-00008-2a70634c-f292-46f4-ba71-36a4af58f777-c000.avro",
              "gs://bucket/path/to/part-00009-2a70634c-f292-46f4-ba71-36a4af58f777-c000.avro",
              "gs://bucket/path/to/part-00010-2a70634c-f292-46f4-ba71-36a4af58f777-c000.avro"),
          // output 2
          Arrays.asList(
              "gs://bucket/path/to/part-0000*-2a70634c-f292-46f4-ba71-36a4af58f777-c000.avro",
              "gs://bucket/path/to/part-00010-2a70634c-f292-46f4-ba71-36a4af58f777-c000.avro")
        },
        new Object[] {
          // input 3
          IntStream.range(100, 200)
              .mapToObj(
                  i ->
                      String.format(
                          "gs://bucket/path/to/part-00%d-5ef57800-1b61-4b15-af3c-f766f84d179c-c000.snappy.parquet",
                          i))
              .collect(Collectors.toList()),
          // output 3
          Arrays.asList(
              "gs://bucket/path/to/part-001*-5ef57800-1b61-4b15-af3c-f766f84d179c-c000.snappy.parquet")
        },
        new Object[] {
          // input 4
          IntStream.range(100, 211)
              .mapToObj(
                  i ->
                      String.format(
                          "gs://bucket/path/to/part-00%s-2a70634c-f292-46f4-ba71-36a4af58f777-c000.json",
                          i))
              .collect(Collectors.toList()),
          // output 4
          Arrays.asList(
              "gs://bucket/path/to/part-001*-2a70634c-f292-46f4-ba71-36a4af58f777-c000.json",
              "gs://bucket/path/to/part-0020*-2a70634c-f292-46f4-ba71-36a4af58f777-c000.json",
              "gs://bucket/path/to/part-00210-2a70634c-f292-46f4-ba71-36a4af58f777-c000.json")
        },
        new Object[] { // size of 1
          // input 5
          Arrays.asList(
              "gs://bucket/path/to/part-00000-2a70634c-f292-46f4-ba71-36a4af58f777-c000.avro"),
          // output 5
          Arrays.asList(
              "gs://bucket/path/to/part-00000-2a70634c-f292-46f4-ba71-36a4af58f777-c000.avro")
        },
        new Object[] { // empty
          // input 6
          Arrays.asList(),
          // output 6
          Arrays.asList()
        });
  }

  @Test
  public void test() {
    List<String> actual = SparkBigQueryUtil.optimizeLoadUriListForSpark(input);
    assertThat(actual).containsExactlyElementsIn(expected);
  }
}
