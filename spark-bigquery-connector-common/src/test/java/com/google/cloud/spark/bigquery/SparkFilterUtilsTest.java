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

import com.google.cloud.bigquery.storage.v1.DataFormat;
import com.google.common.collect.ImmutableList;
import org.apache.spark.sql.sources.*;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.Date;
import java.sql.Timestamp;
import java.text.ParseException;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Optional;

import static com.google.common.truth.Truth.assertThat;

@RunWith(Parameterized.class)
public class SparkFilterUtilsTest {

  private static final DataFormat ARROW = DataFormat.ARROW;
  private static final DataFormat AVRO = DataFormat.AVRO;

  @Parameterized.Parameters(name = "{index}: dataFormat={0}, pushAllFilters={1}")
  public static Iterable<Object[]> data() {
    return Arrays.asList(
        new Object[] {AVRO, false},
        new Object[] {AVRO, true},
        new Object[] {ARROW, false},
        new Object[] {ARROW, true});
  }

  private DataFormat dataFormat;
  private boolean pushAllFilters;

  public SparkFilterUtilsTest(DataFormat dataFormat, boolean pushAllFilters) {
    this.dataFormat = dataFormat;
    this.pushAllFilters = pushAllFilters;
  }

  @Test
  public void testValidFilters() {
    ArrayList<Filter> validFilters =
        new ArrayList(
            Arrays.asList(
                EqualTo.apply("foo", "manatee"),
                GreaterThan.apply("foo", "aardvark"),
                GreaterThanOrEqual.apply("bar", 2),
                LessThan.apply("foo", "zebra"),
                LessThanOrEqual.apply("bar", 1),
                In.apply("foo", new Object[] {1, 2, 3}),
                IsNull.apply("foo"),
                IsNotNull.apply("foo"),
                And.apply(IsNull.apply("foo"), IsNotNull.apply("bar")),
                Not.apply(IsNull.apply("foo")),
                StringStartsWith.apply("foo", "abc"),
                StringEndsWith.apply("foo", "def"),
                StringContains.apply("foo", "abcdef")));
    if (dataFormat != ARROW || pushAllFilters) {
      // If pushAllFilters isn't true, the following filter can't be handled for ARROW.
      validFilters.add(Or.apply(IsNull.apply("foo"), IsNotNull.apply("foo")));
    }
    validFilters.forEach(
        f ->
            assertThat(SparkFilterUtils.unhandledFilters(pushAllFilters, dataFormat, f)).isEmpty());
  }

  @Test
  public void testMultipleValidFiltersAreHandled() {
    Filter valid1 = EqualTo.apply("foo", "bar");
    Filter valid2 = EqualTo.apply("bar", 1);
    assertThat(SparkFilterUtils.unhandledFilters(pushAllFilters, dataFormat, valid1, valid2))
        .isEmpty();
  }

  @Test
  public void testInvalidFilters() {
    Filter valid1 = EqualTo.apply("foo", "bar");
    Filter valid2 = EqualTo.apply("bar", 1);
    Filter invalid1 = EqualNullSafe.apply("foo", "bar");
    Filter invalid2 =
        And.apply(EqualTo.apply("foo", "bar"), Not.apply(EqualNullSafe.apply("bar", 1)));
    Filter avroValid = Or.apply(IsNull.apply("foo"), IsNotNull.apply("foo"));
    Iterable<Filter> unhandled =
        SparkFilterUtils.unhandledFilters(
            pushAllFilters, dataFormat, valid1, valid2, invalid1, invalid2, avroValid);
    if (pushAllFilters) {
      assertThat(unhandled).isEmpty();
    } else if (dataFormat == AVRO) {
      assertThat(unhandled).containsExactly(invalid1, invalid2);
    } else {
      assertThat(unhandled).containsExactly(invalid1, invalid2, avroValid);
    }
  }

  @Test
  public void testNewFilterBehaviourWithFilterOption() {
    if (dataFormat == ARROW && !pushAllFilters) {
      // If pushAllFilters isn't true, the following test won't pass for ARROW.
      return;
    }
    checkFilters(
        pushAllFilters,
        dataFormat,
        "(f>1)",
        "(f>1) AND (`a` > 2)",
        Optional.of("f>1"),
        GreaterThan.apply("a", 2));
  }

  @Test
  public void testNewFilterBehaviourNoFilterOption() {
    if (dataFormat == ARROW && !pushAllFilters) {
      // If pushAllFilters isn't true, the following test won't pass for ARROW.
      return;
    }
    checkFilters(
        pushAllFilters, dataFormat, "", "(`a` > 2)", Optional.empty(), GreaterThan.apply("a", 2));
  }

  private void checkFilters(
      boolean pushAllFilters,
      DataFormat readDateFormat,
      String resultWithoutFilters,
      String resultWithFilters,
      Optional<String> configFilter,
      Filter... filters) {
    String result1 =
        SparkFilterUtils.getCompiledFilter(pushAllFilters, readDateFormat, configFilter);
    assertThat(result1).isEqualTo(resultWithoutFilters);
    String result2 =
        SparkFilterUtils.getCompiledFilter(pushAllFilters, readDateFormat, configFilter, filters);
    assertThat(result2).isEqualTo(resultWithFilters);
  }

  @Test
  public void testStringFilters() {
    assertThat(SparkFilterUtils.compileFilter(StringStartsWith.apply("foo", "bar")))
        .isEqualTo("`foo` LIKE 'bar%'");
    assertThat(SparkFilterUtils.compileFilter(StringEndsWith.apply("foo", "bar")))
        .isEqualTo("`foo` LIKE '%bar'");
    assertThat(SparkFilterUtils.compileFilter(StringContains.apply("foo", "bar")))
        .isEqualTo("`foo` LIKE '%bar%'");

    assertThat(SparkFilterUtils.compileFilter(StringStartsWith.apply("foo", "b'ar")))
        .isEqualTo("`foo` LIKE 'b\\'ar%'");
    assertThat(SparkFilterUtils.compileFilter(StringEndsWith.apply("foo", "b'ar")))
        .isEqualTo("`foo` LIKE '%b\\'ar'");
    assertThat(SparkFilterUtils.compileFilter(StringContains.apply("foo", "b'ar")))
        .isEqualTo("`foo` LIKE '%b\\'ar%'");
  }

  @Test
  public void testNumericAndNullFilters() {

    assertThat(SparkFilterUtils.compileFilter(EqualTo.apply("foo", 1))).isEqualTo("`foo` = 1");
    assertThat(SparkFilterUtils.compileFilter(GreaterThan.apply("foo", 2))).isEqualTo("`foo` > 2");
    assertThat(SparkFilterUtils.compileFilter(GreaterThanOrEqual.apply("foo", 3)))
        .isEqualTo("`foo` >= 3");
    assertThat(SparkFilterUtils.compileFilter(LessThan.apply("foo", 4))).isEqualTo("`foo` < 4");
    assertThat(SparkFilterUtils.compileFilter(LessThanOrEqual.apply("foo", 5)))
        .isEqualTo("`foo` <= 5");
    assertThat(SparkFilterUtils.compileFilter(In.apply("foo", new Object[] {6, 7, 8})))
        .isEqualTo("`foo` IN (6, 7, 8)");
    assertThat(SparkFilterUtils.compileFilter(IsNull.apply("foo"))).isEqualTo("`foo` IS NULL");
    assertThat(SparkFilterUtils.compileFilter(IsNotNull.apply("foo")))
        .isEqualTo("`foo` IS NOT NULL");
    assertThat(
            SparkFilterUtils.compileFilter(And.apply(IsNull.apply("foo"), IsNotNull.apply("bar"))))
        .isEqualTo("((`foo` IS NULL) AND (`bar` IS NOT NULL))");
    assertThat(
            SparkFilterUtils.compileFilter(Or.apply(IsNull.apply("foo"), IsNotNull.apply("bar"))))
        .isEqualTo("((`foo` IS NULL) OR (`bar` IS NOT NULL))");
    assertThat(SparkFilterUtils.compileFilter(Not.apply(IsNull.apply("foo"))))
        .isEqualTo("(NOT (`foo` IS NULL))");
  }

  @Test
  public void testDateFilters() throws ParseException {
    assertThat(
            SparkFilterUtils.compileFilter(
                In.apply(
                    "datefield",
                    new Object[] {Date.valueOf("2020-09-01"), Date.valueOf("2020-11-03")})))
        .isEqualTo("`datefield` IN (DATE '2020-09-01', DATE '2020-11-03')");
  }

  @Test
  public void testTimestampFilters() throws ParseException {
    Timestamp ts1 = Timestamp.valueOf("2008-12-25 15:30:00");
    Timestamp ts2 = Timestamp.valueOf("2020-01-25 02:10:10");
    assertThat(SparkFilterUtils.compileFilter(In.apply("tsfield", new Object[] {ts1, ts2})))
        .isEqualTo(
            "`tsfield` IN (TIMESTAMP '2008-12-25 15:30:00.0', TIMESTAMP '2020-01-25 02:10:10.0')");
  }

  @Test
  public void testFiltersWithNestedOrAnd_1() {
    if (dataFormat == ARROW && !pushAllFilters) {
      // If pushAllFilters isn't true, the following test won't pass for ARROW.
      return;
    }

    // original query
    // (c1 >= 500 or c1 <= 70 or c1 >= 900 or c3 <= 50) and
    // (c1 >= 100 or c1 <= 700  or c2 <= 900) and
    // (c1 >= 5000 or c1 <= 701)

    Filter part1 =
        Or.apply(
            Or.apply(GreaterThanOrEqual.apply("c1", 500), LessThanOrEqual.apply("c1", 70)),
            Or.apply(GreaterThanOrEqual.apply("c1", 900), LessThanOrEqual.apply("c3", 50)));

    Filter part2 =
        Or.apply(
            Or.apply(GreaterThanOrEqual.apply("c1", 100), LessThanOrEqual.apply("c1", 700)),
            LessThanOrEqual.apply("c2", 900));

    Filter part3 = Or.apply(GreaterThanOrEqual.apply("c1", 5000), LessThanOrEqual.apply("c1", 701));

    checkFilters(
        pushAllFilters,
        dataFormat,
        "",
        "(((((`c1` >= 100) OR (`c1` <= 700))) OR (`c2` <= 900)) "
            + "AND ((((`c1` >= 500) OR (`c1` <= 70))) OR (((`c1` >= 900) OR "
            + "(`c3` <= 50)))) AND ((`c1` >= 5000) OR (`c1` <= 701)))",
        Optional.empty(),
        part1,
        part2,
        part3);
  }

  @Test
  public void testFiltersWithNestedOrAnd_2() {
    if (dataFormat == ARROW && !pushAllFilters) {
      // If pushAllFilters isn't true, the following test won't pass for ARROW.
      return;
    }

    // original query
    // (c1 >= 500 and c2 <= 300) or (c1 <= 800 and c3 >= 230)

    Filter filter =
        Or.apply(
            And.apply(GreaterThanOrEqual.apply("c1", 500), LessThanOrEqual.apply("c2", 300)),
            And.apply(LessThanOrEqual.apply("c1", 800), GreaterThanOrEqual.apply("c3", 230)));

    checkFilters(
        pushAllFilters,
        dataFormat,
        "",
        "(((((`c1` >= 500) AND (`c2` <= 300))) OR (((`c1` <= 800) AND (`c3` >= 230)))))",
        Optional.empty(),
        filter);
  }

  @Test
  public void testFiltersWithNestedOrAnd_3() {
    if (dataFormat == ARROW && !pushAllFilters) {
      // If pushAllFilters isn't true, the following test won't pass for ARROW.
      return;
    }

    // original query
    // (((c1 >= 500 or c1 <= 70) and
    // (c1 >= 900 or (c3 <= 50 and (c2 >= 20 or c3 > 200))))) and
    // (((c1 >= 5000 or c1 <= 701) and (c2 >= 150 or c3 >= 100)) or
    // ((c1 >= 50 or c1 <= 71) and (c2 >= 15 or c3 >= 10)))

    Filter part1 = Or.apply(GreaterThanOrEqual.apply("c1", 500), LessThanOrEqual.apply("c1", 70));

    Filter part2 =
        Or.apply(
            GreaterThanOrEqual.apply("c1", 900),
            And.apply(
                LessThanOrEqual.apply("c3", 50),
                Or.apply(GreaterThanOrEqual.apply("c2", 20), GreaterThan.apply("c3", 200))));

    Filter part3 =
        Or.apply(
            And.apply(
                Or.apply(GreaterThanOrEqual.apply("c1", 5000), LessThanOrEqual.apply("c1", 701)),
                Or.apply(GreaterThanOrEqual.apply("c2", 150), GreaterThanOrEqual.apply("c3", 100))),
            And.apply(
                Or.apply(GreaterThanOrEqual.apply("c1", 50), LessThanOrEqual.apply("c1", 71)),
                Or.apply(GreaterThanOrEqual.apply("c2", 15), GreaterThanOrEqual.apply("c3", 10))));

    checkFilters(
        pushAllFilters,
        dataFormat,
        "",
        "(((((((`c1` >= 5000) OR (`c1` <= 701))) AND "
            + "(((`c2` >= 150) OR (`c3` >= 100))))) OR (((((`c1` >= 50) OR "
            + "(`c1` <= 71))) AND (((`c2` >= 15) OR (`c3` >= 10)))))) AND "
            + "((`c1` >= 500) OR (`c1` <= 70)) AND ((`c1` >= 900) OR "
            + "(((`c3` <= 50) AND (((`c2` >= 20) OR (`c3` > 200)))))))",
        Optional.empty(),
        part1,
        part2,
        part3);
  }
}
