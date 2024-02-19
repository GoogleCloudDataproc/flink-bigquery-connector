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

import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.flink.bigquery.common.utils.flink.annotations.Internal;
import com.google.cloud.flink.bigquery.common.utils.flink.core.Preconditions;
import com.google.cloud.flink.bigquery.services.PartitionIdWithInfo;
import com.google.cloud.flink.bigquery.services.PartitionIdWithInfoAndStatus;
import com.google.cloud.flink.bigquery.services.TablePartitionInfo;
import org.apache.commons.lang3.StringUtils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.TimeZone;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Utility class to handle the BigQuery partition conversions to Flink types and structures. */
@Internal
public class BigQueryPartitionUtils {

    /**
     * Below durations are added to current partitionId to obtain the next tentative partitionId,
     * which serves as a non-inclusive upper limit for observing records until current partition is
     * considered complete. For month and year, maximum values are used (31 days for month, 366 days
     * for year) so no records are dropped, even though the current window remains open for a longer
     * time. For instance, given month based partitioning, the partition for February 2023 will not
     * be considered complete till 4th March 2023.
     */
    static final Integer HOUR_SECONDS = 3600;

    static final Integer DAY_SECONDS = 86400;
    static final Integer MONTH_SECONDS = 2678400;
    static final Integer YEAR_SECONDS = 31622400;

    private static final ZoneId UTC_ZONE = ZoneId.of("UTC");
    private static final TimeZone UTC_TIME_ZONE = TimeZone.getTimeZone(UTC_ZONE);

    private static final String BQPARTITION_HOUR_FORMAT_STRING = "yyyyMMddHH";
    private static final String BQPARTITION_DAY_FORMAT_STRING = "yyyyMMdd";
    private static final String BQPARTITION_MONTH_FORMAT_STRING = "yyyyMM";
    private static final String BQPARTITION_YEAR_FORMAT_STRING = "yyyy";

    private static final String SQL_HOUR_FORMAT_STRING = "yyyy-MM-dd HH:00:00";
    private static final String SQL_DAY_FORMAT_STRING = "yyyy-MM-dd";
    private static final String SQL_MONTH_FORMAT_STRING = "yyyy-MM-01";
    private static final String SQL_YEAR_FORMAT_STRING = "yyyy-01-01";

    private static final SimpleDateFormat BQPARTITION_HOUR_FORMAT =
            new SimpleDateFormat(BQPARTITION_HOUR_FORMAT_STRING);
    private static final SimpleDateFormat BQPARTITION_DAY_FORMAT =
            new SimpleDateFormat(BQPARTITION_DAY_FORMAT_STRING);
    private static final SimpleDateFormat BQPARTITION_MONTH_FORMAT =
            new SimpleDateFormat(BQPARTITION_MONTH_FORMAT_STRING);
    private static final SimpleDateFormat BQPARTITION_YEAR_FORMAT =
            new SimpleDateFormat(BQPARTITION_YEAR_FORMAT_STRING);

    private static final SimpleDateFormat SQL_HOUR_FORMAT =
            new SimpleDateFormat(SQL_HOUR_FORMAT_STRING);
    private static final SimpleDateFormat SQL_DAY_FORMAT =
            new SimpleDateFormat(SQL_DAY_FORMAT_STRING);
    private static final SimpleDateFormat SQL_MONTH_FORMAT =
            new SimpleDateFormat(SQL_MONTH_FORMAT_STRING);
    private static final SimpleDateFormat SQL_YEAR_FORMAT =
            new SimpleDateFormat(SQL_YEAR_FORMAT_STRING);

    static {
        BQPARTITION_HOUR_FORMAT.setTimeZone(UTC_TIME_ZONE);
        BQPARTITION_DAY_FORMAT.setTimeZone(UTC_TIME_ZONE);
        BQPARTITION_MONTH_FORMAT.setTimeZone(UTC_TIME_ZONE);
        BQPARTITION_YEAR_FORMAT.setTimeZone(UTC_TIME_ZONE);
        SQL_HOUR_FORMAT.setTimeZone(UTC_TIME_ZONE);
        SQL_DAY_FORMAT.setTimeZone(UTC_TIME_ZONE);
        SQL_MONTH_FORMAT.setTimeZone(UTC_TIME_ZONE);
        SQL_YEAR_FORMAT.setTimeZone(UTC_TIME_ZONE);
    }

    private BigQueryPartitionUtils() {}

    /** Represents the partition types the BigQuery can use in partitioned tables. */
    public enum PartitionType {
        HOUR,
        DAY,
        MONTH,
        YEAR,
        INT_RANGE
    }

    /** Represents the completion status of a BigQuery partition. */
    public enum PartitionStatus {
        IN_PROGRESS,
        COMPLETED
    }

    public static StandardSQLTypeName retrievePartitionColumnType(
            TableSchema schema, String partitionColumn) {
        return SchemaTransform.bigQueryTableFieldSchemaTypeToSQLType(
                schema.getFields().stream()
                        .filter(tfs -> tfs.getName().equals(partitionColumn))
                        .map(tfs -> tfs.getType())
                        .findAny()
                        .orElseThrow(
                                () ->
                                        new IllegalStateException(
                                                String.format(
                                                        "The retrieved partition column"
                                                                + " provided %s does not"
                                                                + " correlate with a first"
                                                                + " level column in the schema"
                                                                + " %s.",
                                                        partitionColumn, schema.toString()))));
    }

    static List<String> partitionIdToDateFormat(
            List<String> partitions, SimpleDateFormat parseFormat, SimpleDateFormat printFormat) {
        return partitions.stream()
                .map(
                        id -> {
                            try {
                                return parseFormat.parse(id);
                            } catch (ParseException ex) {
                                throw new RuntimeException(
                                        "Problems parsing the temporal value: " + id, ex);
                            }
                        })
                .map(date -> printFormat.format(date))
                .collect(Collectors.toList());
    }

    public static String partitionValueToValueGivenType(
            String partitionValue, StandardSQLTypeName dataType) {

        switch (dataType) {
                // integer range partition
            case INT64:
                return partitionValue;
                // time based partitioning (hour, date, month, year)
            case DATE:
            case DATETIME:
            case TIMESTAMP:
                return String.format("'%s'", partitionValue);
                // non supported data types for partitions
            default:
                throw new IllegalArgumentException(
                        String.format(
                                "The provided SQL type name (%s) is not supported"
                                        + " as a partition column.",
                                dataType.name()));
        }
    }

    static String dateRestrictionFromPartitionType(
            PartitionType partitionType, String columnName, String valueFromSQL) {
        String temporalFormat = "%s BETWEEN '%s' AND '%s'";
        try {
            switch (partitionType) {
                case DAY:
                    {
                        // extract a date from the value and restrict
                        // between previous and next day
                        Date day = SQL_DAY_FORMAT.parse(valueFromSQL);
                        return String.format(
                                temporalFormat,
                                columnName,
                                SQL_DAY_FORMAT.format(day),
                                SQL_DAY_FORMAT.format(
                                        Date.from(day.toInstant().plusSeconds(DAY_SECONDS))));
                    }
                case MONTH:
                    {
                        // extract a date from the value and restrict
                        // between previous and next month
                        Date day = SQL_MONTH_FORMAT.parse(valueFromSQL);
                        return String.format(
                                temporalFormat,
                                columnName,
                                SQL_MONTH_FORMAT.format(day),
                                DateTimeFormatter.ofPattern(SQL_DAY_FORMAT_STRING)
                                        .withZone(UTC_ZONE)
                                        .format(
                                                day.toInstant()
                                                        .atZone(UTC_ZONE)
                                                        .toLocalDate()
                                                        .plusMonths(1)
                                                        .atTime(LocalTime.MIDNIGHT)
                                                        .toInstant(ZoneOffset.UTC)));
                    }
                case YEAR:
                    {
                        // extract a date from the value and restrict
                        // between previous and next year
                        Date day = SQL_YEAR_FORMAT.parse(valueFromSQL);
                        return String.format(
                                temporalFormat,
                                columnName,
                                SQL_YEAR_FORMAT.format(day),
                                DateTimeFormatter.ofPattern(SQL_YEAR_FORMAT_STRING)
                                        .withZone(UTC_ZONE)
                                        .format(
                                                day.toInstant()
                                                        .atZone(UTC_ZONE)
                                                        .toLocalDate()
                                                        .plusYears(1)
                                                        .atTime(LocalTime.MIDNIGHT)
                                                        .toInstant(ZoneOffset.UTC)));
                    }
                default:
                    throw new IllegalArgumentException(
                            String.format(
                                    "The provided partition type %s is not supported as a"
                                            + " temporal based partition for the column %s.",
                                    partitionType, columnName));
            }
        } catch (ParseException ex) {
            throw new IllegalArgumentException(
                    "Problems while manipulating the temporal argument: " + valueFromSQL, ex);
        }
    }

    static String timestampRestrictionFromPartitionType(
            PartitionType partitionType, String columnName, String valueFromSQL) {
        ZonedDateTime parsedDateTime =
                LocalDateTime.parse(
                                valueFromSQL,
                                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
                                        .withZone(UTC_ZONE))
                        .atZone(UTC_ZONE);
        String temporalFormat = "%s BETWEEN '%s' AND '%s'";
        switch (partitionType) {
            case HOUR:
                {
                    // extract a datetime from the value and restrict
                    // between previous and next hour
                    DateTimeFormatter hourFormatter =
                            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:00:00").withZone(UTC_ZONE);
                    return String.format(
                            temporalFormat,
                            columnName,
                            parsedDateTime.format(hourFormatter),
                            parsedDateTime.plusHours(1).format(hourFormatter));
                }
            case DAY:
                {
                    // extract a date from the value and restrict
                    // between previous and next day
                    DateTimeFormatter dayFormatter =
                            DateTimeFormatter.ofPattern("yyyy-MM-dd 00:00:00").withZone(UTC_ZONE);
                    return String.format(
                            temporalFormat,
                            columnName,
                            parsedDateTime.format(dayFormatter),
                            parsedDateTime.plusDays(1).format(dayFormatter));
                }
            case MONTH:
                {
                    // extract a date from the value and restrict
                    // between previous and next month
                    DateTimeFormatter monthFormatter =
                            DateTimeFormatter.ofPattern("yyyy-MM-01 00:00:00").withZone(UTC_ZONE);
                    return String.format(
                            temporalFormat,
                            columnName,
                            parsedDateTime.format(monthFormatter),
                            parsedDateTime.plusMonths(1).format(monthFormatter));
                }
            case YEAR:
                {
                    // extract a date from the value and restrict
                    // between previous and next year
                    DateTimeFormatter yearFormatter =
                            DateTimeFormatter.ofPattern("yyyy-01-01 00:00:00").withZone(UTC_ZONE);
                    return String.format(
                            temporalFormat,
                            columnName,
                            parsedDateTime.format(yearFormatter),
                            parsedDateTime.plusYears(1).format(yearFormatter));
                }
            default:
                throw new IllegalArgumentException(
                        String.format(
                                "The provided partition type %s is not supported as a"
                                        + " temporal based partition for the column %s.",
                                partitionType, columnName));
        }
    }

    public static String formatPartitionRestrictionBasedOnInfo(
            Optional<TablePartitionInfo> tablePartitionInfo,
            String columnNameFromSQL,
            String valueFromSQL) {
        return tablePartitionInfo
                .map(
                        info -> {
                            switch (info.getColumnType()) {
                                    // integer range partition
                                case INT64:
                                    return String.format(
                                            "%s = %s", info.getColumnName(), valueFromSQL);
                                    // date based partitioning (hour, date, month, year)
                                case DATE:
                                    return dateRestrictionFromPartitionType(
                                            info.getPartitionType(),
                                            columnNameFromSQL,
                                            valueFromSQL);
                                    // date based partitioning (hour, date, month, year)
                                case DATETIME:
                                case TIMESTAMP:
                                    return timestampRestrictionFromPartitionType(
                                            info.getPartitionType(),
                                            columnNameFromSQL,
                                            valueFromSQL);
                                    // non supported data types for partitions
                                default:
                                    throw new IllegalArgumentException(
                                            String.format(
                                                    "The provided SQL type name (%s) is not supported"
                                                            + " as a partition column in BigQuery.",
                                                    info.getColumnType()));
                            }
                        })
                .orElse(String.format("%s = %s", columnNameFromSQL, valueFromSQL));
    }

    static PartitionIdWithInfoAndStatus retrievePartitionInfoWithStatus(
            PartitionIdWithInfo partition, Function<String, Long> parseAndManipulatePartitionTS) {
        return partitionValuesFromIdAndDataType(
                        Arrays.asList(partition.getPartitionId()),
                        partition.getInfo().getColumnType())
                .stream()
                .map(parseAndManipulatePartitionTS)
                .filter(
                        nextPartitionTs ->
                                partition
                                        .getInfo()
                                        .getStreamingBufferOldestEntryTime()
                                        .isAfter(Instant.ofEpochSecond(nextPartitionTs)))
                .map(
                        i ->
                                new PartitionIdWithInfoAndStatus(
                                        partition.getPartitionId(),
                                        partition.getInfo(),
                                        BigQueryPartitionUtils.PartitionStatus.COMPLETED))
                .findFirst()
                .orElse(
                        new PartitionIdWithInfoAndStatus(
                                partition.getPartitionId(),
                                partition.getInfo(),
                                BigQueryPartitionUtils.PartitionStatus.IN_PROGRESS));
    }

    static Instant retrieveEpochSecondsFromParsedTemporal(SimpleDateFormat sdf, String tsString) {
        try {
            return sdf.parse(tsString).toInstant();
        } catch (ParseException ex) {
            throw new RuntimeException(
                    "Problems while parsing temporal info from: " + tsString, ex);
        }
    }

    // TODO: currently, time based partitioning attribute is checked against the ingestion time of
    // BigQuery table's buffer stream's oldest entry. Hence, there is a logical mismatch where
    // "event time" is being compared with "ingestion time". In subsequent versions of the
    // connector,
    // replace this implementation of partition completeness with a configurable lateness duration
    // where a partition would be considered complete after a certain amount of time has elapsed
    // after the partition's end instant.
    public static PartitionIdWithInfoAndStatus checkPartitionCompleted(
            PartitionIdWithInfo partition) {
        switch (partition.getInfo().getPartitionType()) {
            case HOUR:
                {
                    return retrievePartitionInfoWithStatus(
                            partition,
                            tsString ->
                                    retrieveEpochSecondsFromParsedTemporal(
                                                    SQL_HOUR_FORMAT, tsString)
                                            // an hour
                                            .plusSeconds(HOUR_SECONDS)
                                            .getEpochSecond());
                }
            case DAY:
                {
                    return retrievePartitionInfoWithStatus(
                            partition,
                            tsString ->
                                    retrieveEpochSecondsFromParsedTemporal(SQL_DAY_FORMAT, tsString)
                                            // a day
                                            .plusSeconds(DAY_SECONDS)
                                            .getEpochSecond());
                }
            case MONTH:
                {
                    return retrievePartitionInfoWithStatus(
                            partition,
                            tsString ->
                                    retrieveEpochSecondsFromParsedTemporal(
                                                    SQL_MONTH_FORMAT, tsString)
                                            // a month
                                            .plusSeconds(MONTH_SECONDS)
                                            .getEpochSecond());
                }
            case YEAR:
                {
                    return retrievePartitionInfoWithStatus(
                            partition,
                            tsString ->
                                    retrieveEpochSecondsFromParsedTemporal(
                                                    SQL_YEAR_FORMAT, tsString)
                                            .plusSeconds(YEAR_SECONDS)
                                            .getEpochSecond());
                }
            case INT_RANGE:
                return new PartitionIdWithInfoAndStatus(
                        partition.getPartitionId(),
                        partition.getInfo(),
                        BigQueryPartitionUtils.PartitionStatus.COMPLETED);
            default:
                throw new IllegalArgumentException(
                        "Partition type not supported: " + partition.getInfo().getPartitionType());
        }
    }

    public static List<String> partitionValuesFromIdAndDataType(
            List<String> partitionIds, StandardSQLTypeName dataType) {
        List<String> partitionValues = new ArrayList<>();
        switch (dataType) {
                // integer range partition
            case INT64:
                // we add them as they are
                partitionValues.addAll(partitionIds);
                break;
                // time based partitioning (hour, date, month, year)
            case DATE:
            case DATETIME:
            case TIMESTAMP:
                // lets first check that all the partition ids have the same length
                String firstId = partitionIds.get(0);
                Preconditions.checkState(
                        partitionIds.stream()
                                .allMatch(
                                        pid ->
                                                (pid.length() == firstId.length())
                                                        && StringUtils.isNumeric(pid)),
                        "Some elements in the partition id list have a different length: "
                                + partitionIds.toString());
                switch (firstId.length()) {
                    case 4:
                        // we have yearly partitions
                        partitionValues.addAll(
                                partitionIdToDateFormat(
                                        partitionIds, BQPARTITION_YEAR_FORMAT, SQL_YEAR_FORMAT));
                        break;
                    case 6:
                        // we have monthly partitions
                        partitionValues.addAll(
                                partitionIdToDateFormat(
                                        partitionIds, BQPARTITION_MONTH_FORMAT, SQL_MONTH_FORMAT));
                        break;
                    case 8:
                        // we have daily partitions
                        partitionValues.addAll(
                                partitionIdToDateFormat(
                                        partitionIds, BQPARTITION_DAY_FORMAT, SQL_DAY_FORMAT));
                        break;
                    case 10:
                        // we have hourly partitions
                        partitionValues.addAll(
                                partitionIdToDateFormat(
                                        partitionIds, BQPARTITION_HOUR_FORMAT, SQL_HOUR_FORMAT));
                        break;
                    default:
                        throw new IllegalArgumentException(
                                "The lenght of the partition id is not one of the expected ones: "
                                        + firstId);
                }
                break;
                // non supported data types for partitions
            default:
                throw new IllegalArgumentException(
                        String.format(
                                "The provided SQL type name (%s) is not supported"
                                        + " as a partition column.",
                                dataType.name()));
        }
        return partitionValues;
    }
}
