/*
 * Copyright (C) 2023 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.flink.bigquery.common.utils;

import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.flink.bigquery.fakes.StorageClientFaker;
import com.google.cloud.flink.bigquery.services.PartitionIdWithInfo;
import com.google.cloud.flink.bigquery.services.PartitionIdWithInfoAndStatus;
import com.google.cloud.flink.bigquery.services.TablePartitionInfo;
import org.assertj.core.api.Assertions;
import org.junit.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/** */
public class BigQueryPartitionUtilsTest {

    @Test
    public void testPartitionHour() {
        List<String> partitionIds = Arrays.asList("2023062822", "2023062823");
        // ISO formatted dates as single quote string literals at the beginning of the hour.
        List<String> expectedValues = Arrays.asList("2023-06-28 22:00:00", "2023-06-28 23:00:00");
        List<String> values =
                BigQueryPartitionUtils.partitionValuesFromIdAndDataType(
                        partitionIds, StandardSQLTypeName.TIMESTAMP);

        Assertions.assertThat(values).isEqualTo(expectedValues);
    }

    @Test
    public void testPartitionDay() {
        List<String> partitionIds = Arrays.asList("20230628", "20230628");
        // ISO formatted dates as single quote string literals.
        List<String> expectedValues = Arrays.asList("2023-06-28", "2023-06-28");
        List<String> values =
                BigQueryPartitionUtils.partitionValuesFromIdAndDataType(
                        partitionIds, StandardSQLTypeName.DATETIME);

        Assertions.assertThat(values).isEqualTo(expectedValues);
    }

    @Test
    public void testPartitionMonth() {
        List<String> partitionIds = Arrays.asList("202306", "202307");
        // ISO formatted dates as single quote string literals
        List<String> expectedValues = Arrays.asList("2023-06-01", "2023-07-01");
        List<String> values =
                BigQueryPartitionUtils.partitionValuesFromIdAndDataType(
                        partitionIds, StandardSQLTypeName.DATE);

        Assertions.assertThat(values).isEqualTo(expectedValues);
    }

    @Test
    public void testPartitionYear() {
        List<String> partitionIds = Arrays.asList("2023", "2022");
        // ISO formatted dates as single quote string literals
        List<String> expectedValues = Arrays.asList("2023-01-01", "2022-01-01");
        List<String> values =
                BigQueryPartitionUtils.partitionValuesFromIdAndDataType(
                        partitionIds, StandardSQLTypeName.TIMESTAMP);

        Assertions.assertThat(values).isEqualTo(expectedValues);
    }

    @Test
    public void testPartitionInteger() {
        List<String> partitionIds = Arrays.asList("2023", "2022");
        // ISO formatted dates as single quote string literals
        List<String> expectedValues = Arrays.asList("2023", "2022");
        List<String> values =
                BigQueryPartitionUtils.partitionValuesFromIdAndDataType(
                        partitionIds, StandardSQLTypeName.INT64);

        Assertions.assertThat(values).isEqualTo(expectedValues);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWrongTemporalPartition() {
        List<String> partitionIds = Arrays.asList("202308101112");
        BigQueryPartitionUtils.partitionValuesFromIdAndDataType(
                partitionIds, StandardSQLTypeName.TIMESTAMP);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWrongArrayPartition() {
        List<String> partitionIds = Arrays.asList("2023", "2022");
        BigQueryPartitionUtils.partitionValuesFromIdAndDataType(
                partitionIds, StandardSQLTypeName.ARRAY);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWrongStructPartition() {
        List<String> partitionIds = Arrays.asList("2023", "2022");
        BigQueryPartitionUtils.partitionValuesFromIdAndDataType(
                partitionIds, StandardSQLTypeName.STRUCT);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWrongJsonPartition() {
        List<String> partitionIds = Arrays.asList("2023", "2022");
        BigQueryPartitionUtils.partitionValuesFromIdAndDataType(
                partitionIds, StandardSQLTypeName.JSON);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWrongGeoPartition() {
        List<String> partitionIds = Arrays.asList("2023", "2022");
        BigQueryPartitionUtils.partitionValuesFromIdAndDataType(
                partitionIds, StandardSQLTypeName.GEOGRAPHY);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWrongBigNumPartition() {
        List<String> partitionIds = Arrays.asList("2023", "2022");
        BigQueryPartitionUtils.partitionValuesFromIdAndDataType(
                partitionIds, StandardSQLTypeName.BIGNUMERIC);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWrongBoolPartition() {
        List<String> partitionIds = Arrays.asList("2023", "2022");
        BigQueryPartitionUtils.partitionValuesFromIdAndDataType(
                partitionIds, StandardSQLTypeName.BOOL);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWrongBytesPartition() {
        List<String> partitionIds = Arrays.asList("2023", "2022");
        BigQueryPartitionUtils.partitionValuesFromIdAndDataType(
                partitionIds, StandardSQLTypeName.BYTES);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWrongFloatPartition() {
        List<String> partitionIds = Arrays.asList("2023", "2022");
        BigQueryPartitionUtils.partitionValuesFromIdAndDataType(
                partitionIds, StandardSQLTypeName.FLOAT64);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWrongStringPartition() {
        List<String> partitionIds = Arrays.asList("2023", "2022");
        BigQueryPartitionUtils.partitionValuesFromIdAndDataType(
                partitionIds, StandardSQLTypeName.STRING);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWrongTimePartition() {
        List<String> partitionIds = Arrays.asList("2023", "2022");
        BigQueryPartitionUtils.partitionValuesFromIdAndDataType(
                partitionIds, StandardSQLTypeName.TIME);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWrongIntervalPartition() {
        List<String> partitionIds = Arrays.asList("2023", "2022");
        BigQueryPartitionUtils.partitionValuesFromIdAndDataType(
                partitionIds, StandardSQLTypeName.INTERVAL);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWrongNumericPartition() {
        List<String> partitionIds = Arrays.asList("2023", "2022");
        BigQueryPartitionUtils.partitionValuesFromIdAndDataType(
                partitionIds, StandardSQLTypeName.NUMERIC);
    }

    @Test
    public void testPartitionValueInteger() {
        Assertions.assertThat(
                        BigQueryPartitionUtils.partitionValueToValueGivenType(
                                "2023", StandardSQLTypeName.INT64))
                .isEqualTo("2023");
    }

    @Test
    public void testPartitionValueDate() {
        Assertions.assertThat(
                        BigQueryPartitionUtils.partitionValueToValueGivenType(
                                "2023", StandardSQLTypeName.DATE))
                .isEqualTo("'2023'");
    }

    @Test
    public void testPartitionValueDateTime() {
        Assertions.assertThat(
                        BigQueryPartitionUtils.partitionValueToValueGivenType(
                                "2023", StandardSQLTypeName.DATETIME))
                .isEqualTo("'2023'");
    }

    @Test
    public void testPartitionValueTimestamp() {
        Assertions.assertThat(
                        BigQueryPartitionUtils.partitionValueToValueGivenType(
                                "2023", StandardSQLTypeName.TIMESTAMP))
                .isEqualTo("'2023'");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPartitionValueFailsWithOtherType() {
        Assertions.assertThat(
                        BigQueryPartitionUtils.partitionValueToValueGivenType(
                                "2023", StandardSQLTypeName.NUMERIC))
                .isEqualTo("'2023'");
    }

    @Test
    public void testRetrievePartitionColumnType() {
        StandardSQLTypeName retrieved =
                BigQueryPartitionUtils.retrievePartitionColumnType(
                        StorageClientFaker.SIMPLE_BQ_TABLE_SCHEMA, "ts");

        Assertions.assertThat(retrieved).isEqualTo(StandardSQLTypeName.TIMESTAMP);
    }

    @Test(expected = IllegalStateException.class)
    public void testRetrieveTypeOfNonExistentColumn() {
        BigQueryPartitionUtils.retrievePartitionColumnType(
                StorageClientFaker.SIMPLE_BQ_TABLE_SCHEMA, "address");
    }

    @Test
    public void testCheckPartitionCompletedHour() {
        PartitionIdWithInfo partitionWithInfo =
                new PartitionIdWithInfo(
                        "2023072804",
                        new TablePartitionInfo(
                                "temporal",
                                BigQueryPartitionUtils.PartitionType.HOUR,
                                StandardSQLTypeName.TIMESTAMP,
                                tsStringToInstant("2023-07-28 05:00:01 UTC")));

        PartitionIdWithInfoAndStatus partitionWithInfoAndStatus =
                BigQueryPartitionUtils.checkPartitionCompleted(partitionWithInfo);

        Assertions.assertThat(partitionWithInfoAndStatus.getStatus())
                .isEqualTo(BigQueryPartitionUtils.PartitionStatus.COMPLETED);
    }

    @Test
    public void testCheckPartitionNotCompletedHour() {
        PartitionIdWithInfo partitionWithInfo =
                new PartitionIdWithInfo(
                        "2023072804",
                        new TablePartitionInfo(
                                "temporal",
                                BigQueryPartitionUtils.PartitionType.HOUR,
                                StandardSQLTypeName.TIMESTAMP,
                                tsStringToInstant("2023-07-28 04:59:59 UTC")));

        PartitionIdWithInfoAndStatus partitionWithInfoAndStatus =
                BigQueryPartitionUtils.checkPartitionCompleted(partitionWithInfo);

        Assertions.assertThat(partitionWithInfoAndStatus.getStatus())
                .isEqualTo(BigQueryPartitionUtils.PartitionStatus.IN_PROGRESS);
    }

    @Test
    public void testCheckPartitionCompletedDay() {
        PartitionIdWithInfo partitionWithInfo =
                new PartitionIdWithInfo(
                        "20230728",
                        new TablePartitionInfo(
                                "temporal",
                                BigQueryPartitionUtils.PartitionType.DAY,
                                StandardSQLTypeName.DATE,
                                tsStringToInstant("2023-07-29 00:00:01 UTC")));

        PartitionIdWithInfoAndStatus partitionWithInfoAndStatus =
                BigQueryPartitionUtils.checkPartitionCompleted(partitionWithInfo);

        Assertions.assertThat(partitionWithInfoAndStatus.getStatus())
                .isEqualTo(BigQueryPartitionUtils.PartitionStatus.COMPLETED);
    }

    @Test
    public void testCheckPartitionNotCompletedDay() {
        PartitionIdWithInfo partitionWithInfo =
                new PartitionIdWithInfo(
                        "20230728",
                        new TablePartitionInfo(
                                "temporal",
                                BigQueryPartitionUtils.PartitionType.DAY,
                                StandardSQLTypeName.DATE,
                                tsStringToInstant("2023-07-28 23:59:59 UTC")));

        PartitionIdWithInfoAndStatus partitionWithInfoAndStatus =
                BigQueryPartitionUtils.checkPartitionCompleted(partitionWithInfo);

        Assertions.assertThat(partitionWithInfoAndStatus.getStatus())
                .isEqualTo(BigQueryPartitionUtils.PartitionStatus.IN_PROGRESS);
    }

    @Test
    public void testCheckPartitionCompletedMonth() {
        PartitionIdWithInfo partitionWithInfo =
                new PartitionIdWithInfo(
                        "202307",
                        new TablePartitionInfo(
                                "temporal",
                                BigQueryPartitionUtils.PartitionType.MONTH,
                                StandardSQLTypeName.TIMESTAMP,
                                tsStringToInstant("2023-08-01 00:00:01 UTC")));

        PartitionIdWithInfoAndStatus partitionWithInfoAndStatus =
                BigQueryPartitionUtils.checkPartitionCompleted(partitionWithInfo);

        Assertions.assertThat(partitionWithInfoAndStatus.getStatus())
                .isEqualTo(BigQueryPartitionUtils.PartitionStatus.COMPLETED);
    }

    @Test
    public void testCheckPartitionNotCompletedMonth() {
        PartitionIdWithInfo partitionWithInfo =
                new PartitionIdWithInfo(
                        "202303",
                        new TablePartitionInfo(
                                "temporal",
                                BigQueryPartitionUtils.PartitionType.MONTH,
                                StandardSQLTypeName.TIMESTAMP,
                                tsStringToInstant("2023-03-31 23:59:59 UTC")));

        PartitionIdWithInfoAndStatus partitionWithInfoAndStatus =
                BigQueryPartitionUtils.checkPartitionCompleted(partitionWithInfo);

        Assertions.assertThat(partitionWithInfoAndStatus.getStatus())
                .isEqualTo(BigQueryPartitionUtils.PartitionStatus.IN_PROGRESS);
    }

    @Test
    public void testCheckPartitionNotCompletedMonthFebLeapYear() {
        PartitionIdWithInfo partitionWithInfo =
                new PartitionIdWithInfo(
                        "202002",
                        new TablePartitionInfo(
                                "temporal",
                                BigQueryPartitionUtils.PartitionType.MONTH,
                                StandardSQLTypeName.TIMESTAMP,
                                tsStringToInstant("2020-02-29 23:59:59 UTC")));

        PartitionIdWithInfoAndStatus partitionWithInfoAndStatus =
                BigQueryPartitionUtils.checkPartitionCompleted(partitionWithInfo);

        Assertions.assertThat(partitionWithInfoAndStatus.getStatus())
                .isEqualTo(BigQueryPartitionUtils.PartitionStatus.IN_PROGRESS);
    }

    @Test
    public void testCheckPartitionCompletedYear() {
        PartitionIdWithInfo partitionWithInfo =
                new PartitionIdWithInfo(
                        "2022",
                        new TablePartitionInfo(
                                "temporal",
                                BigQueryPartitionUtils.PartitionType.YEAR,
                                StandardSQLTypeName.DATE,
                                tsStringToInstant("2023-01-02 00:00:01 UTC")));

        PartitionIdWithInfoAndStatus partitionWithInfoAndStatus =
                BigQueryPartitionUtils.checkPartitionCompleted(partitionWithInfo);

        Assertions.assertThat(partitionWithInfoAndStatus.getStatus())
                .isEqualTo(BigQueryPartitionUtils.PartitionStatus.COMPLETED);
    }

    @Test
    public void testCheckPartitionNotCompletedLeapYear() {
        PartitionIdWithInfo partitionWithInfo =
                new PartitionIdWithInfo(
                        "2020",
                        new TablePartitionInfo(
                                "temporal",
                                BigQueryPartitionUtils.PartitionType.YEAR,
                                StandardSQLTypeName.DATE,
                                tsStringToInstant("2020-12-31 23:59:59 UTC")));

        PartitionIdWithInfoAndStatus partitionWithInfoAndStatus =
                BigQueryPartitionUtils.checkPartitionCompleted(partitionWithInfo);

        Assertions.assertThat(partitionWithInfoAndStatus.getStatus())
                .isEqualTo(BigQueryPartitionUtils.PartitionStatus.IN_PROGRESS);
    }

    @Test
    public void testCheckPartitionCompletedIntRange() {
        PartitionIdWithInfo partitionWithInfo =
                new PartitionIdWithInfo(
                        "2023",
                        new TablePartitionInfo(
                                "intvalue",
                                BigQueryPartitionUtils.PartitionType.INT_RANGE,
                                StandardSQLTypeName.INT64,
                                Instant.now()));

        PartitionIdWithInfoAndStatus partitionWithInfoAndStatus =
                BigQueryPartitionUtils.checkPartitionCompleted(partitionWithInfo);

        Assertions.assertThat(partitionWithInfoAndStatus.getStatus())
                .isEqualTo(BigQueryPartitionUtils.PartitionStatus.COMPLETED);
    }

    @Test
    public void testFormatPartitionRestrictionBasedOnInfoEmpty() {
        String expected = "dragon = verde";
        String actual =
                BigQueryPartitionUtils.formatPartitionRestrictionBasedOnInfo(
                        Optional.empty(), "dragon", "verde");

        Assertions.assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void testFormatPartitionRestrictionBasedOnInfoInteger() {
        String expected = "dragon = 5";
        String actual =
                BigQueryPartitionUtils.formatPartitionRestrictionBasedOnInfo(
                        Optional.of(
                                new TablePartitionInfo(
                                        "dragon",
                                        BigQueryPartitionUtils.PartitionType.INT_RANGE,
                                        StandardSQLTypeName.INT64,
                                        Instant.now())),
                        "dragon",
                        "5");

        Assertions.assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void testFormatPartitionRestrictionBasedOnInfoDateDay() {
        String expected = "dragon >= '2023-01-02' AND dragon < '2023-01-03'";
        String actual =
                BigQueryPartitionUtils.formatPartitionRestrictionBasedOnInfo(
                        Optional.of(
                                new TablePartitionInfo(
                                        "dragon",
                                        BigQueryPartitionUtils.PartitionType.DAY,
                                        StandardSQLTypeName.DATE,
                                        Instant.now())),
                        "dragon",
                        "2023-01-02");

        Assertions.assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void testFormatPartitionRestrictionBasedOnInfoDateMonth() {
        String expected = "dragon >= '2023-01-01' AND dragon < '2023-02-01'";
        String actual =
                BigQueryPartitionUtils.formatPartitionRestrictionBasedOnInfo(
                        Optional.of(
                                new TablePartitionInfo(
                                        "dragon",
                                        BigQueryPartitionUtils.PartitionType.MONTH,
                                        StandardSQLTypeName.DATE,
                                        Instant.now())),
                        "dragon",
                        "2023-01-01");

        Assertions.assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void testFormatPartitionRestrictionBasedOnInfoDateYear() {
        String expected = "dragon >= '2023-01-01' AND dragon < '2024-01-01'";
        String actual =
                BigQueryPartitionUtils.formatPartitionRestrictionBasedOnInfo(
                        Optional.of(
                                new TablePartitionInfo(
                                        "dragon",
                                        BigQueryPartitionUtils.PartitionType.YEAR,
                                        StandardSQLTypeName.DATE,
                                        Instant.now())),
                        "dragon",
                        "2023-01-01");

        Assertions.assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void testFormatPartitionRestrictionBasedOnInfoTimestampHour() {
        String expected = "dragon >= '2023-01-01 03:00:00' AND dragon < '2023-01-01 04:00:00'";
        String actual =
                BigQueryPartitionUtils.formatPartitionRestrictionBasedOnInfo(
                        Optional.of(
                                new TablePartitionInfo(
                                        "dragon",
                                        BigQueryPartitionUtils.PartitionType.HOUR,
                                        StandardSQLTypeName.TIMESTAMP,
                                        Instant.now())),
                        "dragon",
                        "2023-01-01 03:00:00");

        Assertions.assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void testFormatPartitionRestrictionBasedOnInfoTimestampDay() {
        String expected = "dragon >= '2023-01-01 00:00:00' AND dragon < '2023-01-02 00:00:00'";
        String actual =
                BigQueryPartitionUtils.formatPartitionRestrictionBasedOnInfo(
                        Optional.of(
                                new TablePartitionInfo(
                                        "dragon",
                                        BigQueryPartitionUtils.PartitionType.DAY,
                                        StandardSQLTypeName.TIMESTAMP,
                                        Instant.now())),
                        "dragon",
                        "2023-01-01 03:00:00");

        Assertions.assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void testFormatPartitionRestrictionBasedOnInfoTimestampMonth() {
        String expected = "dragon >= '2023-01-01 00:00:00' AND dragon < '2023-02-01 00:00:00'";
        String actual =
                BigQueryPartitionUtils.formatPartitionRestrictionBasedOnInfo(
                        Optional.of(
                                new TablePartitionInfo(
                                        "dragon",
                                        BigQueryPartitionUtils.PartitionType.MONTH,
                                        StandardSQLTypeName.TIMESTAMP,
                                        Instant.now())),
                        "dragon",
                        "2023-01-01 03:00:00");

        Assertions.assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void testFormatPartitionRestrictionBasedOnInfoTimestampYear() {
        String expected = "dragon >= '2023-01-01 00:00:00' AND dragon < '2024-01-01 00:00:00'";
        String actual =
                BigQueryPartitionUtils.formatPartitionRestrictionBasedOnInfo(
                        Optional.of(
                                new TablePartitionInfo(
                                        "dragon",
                                        BigQueryPartitionUtils.PartitionType.YEAR,
                                        StandardSQLTypeName.TIMESTAMP,
                                        Instant.now())),
                        "dragon",
                        "2023-01-01 03:00:00");

        Assertions.assertThat(actual).isEqualTo(expected);
    }

    private Instant tsStringToInstant(String ts) {
        try {
            return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss zzz").parse(ts).toInstant();
        } catch (ParseException e) {
            throw new IllegalStateException(
                    "Invalid date format in test. This should never happen!");
        }
    }
}
