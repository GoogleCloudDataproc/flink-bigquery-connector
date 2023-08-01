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

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.flink.bigquery.services.PartitionIdWithInfo;
import com.google.cloud.flink.bigquery.services.PartitionIdWithInfoAndStatus;
import com.google.cloud.flink.bigquery.services.TablePartitionInfo;
import org.apache.commons.lang3.StringUtils;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/** Utility class to handle the BigQuery partition conversions to Flink types and structures. */
@Internal
public class BigQueryPartition {

    private static final String BQPARTITION_HOUR_FORMAT_STRING = "yyyyMMddHH";
    private static final String BQPARTITION_DAY_FORMAT_STRING = "yyyyMMdd";
    private static final String BQPARTITION_MONTH_FORMAT_STRING = "yyyyMM";

    private static final String SQL_HOUR_FORMAT_STRING = "yyyy-MM-dd HH:00:00";
    private static final String SQL_DAY_FORMAT_STRING = "yyyy-MM-dd";
    private static final String SQL_MONTH_FORMAT_STRING = "yyyy-MM";

    private static final DateTimeFormatter BQPARTITION_HOUR_FORMAT =
            DateTimeFormatter.ofPattern(BQPARTITION_HOUR_FORMAT_STRING);
    private static final DateTimeFormatter BQPARTITION_DAY_FORMAT =
            DateTimeFormatter.ofPattern(BQPARTITION_DAY_FORMAT_STRING);
    private static final DateTimeFormatter BQPARTITION_MONTH_FORMAT =
            DateTimeFormatter.ofPattern(BQPARTITION_MONTH_FORMAT_STRING);

    private static final DateTimeFormatter SQL_HOUR_FORMAT =
            DateTimeFormatter.ofPattern(SQL_HOUR_FORMAT_STRING);
    private static final DateTimeFormatter SQL_DAY_FORMAT =
            DateTimeFormatter.ofPattern(SQL_DAY_FORMAT_STRING);
    private static final DateTimeFormatter SQL_MONTH_FORMAT =
            DateTimeFormatter.ofPattern(SQL_MONTH_FORMAT_STRING);

    private BigQueryPartition() {}

    /** Represents the partition types the BigQuery can use in partitioned tables. */
    public enum PartitionType {
        HOUR,
        DAY,
        MONTH,
        YEAR,
        INT_RANGE
    }

    /** represents the completion status of a BigQuery partition */
    public enum PartitionStatus {
        IN_PROGRESS,
        COMPLETED
    }

    public static StandardSQLTypeName retrievePartitionColumnType(
            TableSchema schema, String partitionColumn) {
        return StandardSQLTypeName.valueOf(
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
            List<String> partitions, DateTimeFormatter parseFormat, DateTimeFormatter printFormat) {
        return partitions.stream()
                .map(
                        id -> {
                            try {
                                return parseFormat.parse(id);
                            } catch (DateTimeParseException ex) {
                                throw new RuntimeException(
                                        "Problems parsing the temporal value: " + id);
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
        ZonedDateTime parsedDateTime =
                LocalDateTime.parse(valueFromSQL, DateTimeFormatter.ofPattern("yyyy-MM-dd"))
                        .atZone(ZoneId.of("UTC"));
        String temporalFormat = "%s BETWEEN '%s' AND '%s'";
        switch (partitionType) {
            case DAY:
                {
                    // extract a date from the value and restrict
                    // between previous and next day
                    DateTimeFormatter dayFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
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
                    DateTimeFormatter monthFormatter = DateTimeFormatter.ofPattern("yyyy-MM");
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
                    DateTimeFormatter yearFormatter = DateTimeFormatter.ofPattern("yyyy");
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

    static String timestampRestrictionFromPartitionType(
            PartitionType partitionType, String columnName, String valueFromSQL) {
        ZonedDateTime parsedDateTime =
                LocalDateTime.parse(
                                valueFromSQL, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
                        .atZone(ZoneId.of("UTC"));
        String temporalFormat = "%s BETWEEN '%s' AND '%s'";
        switch (partitionType) {
            case HOUR:
                {
                    // extract a datetime from the value and restrict
                    // between previous and next hour
                    DateTimeFormatter hourFormatter =
                            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:00:00");
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
                    DateTimeFormatter dayFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
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
                    DateTimeFormatter monthFormatter = DateTimeFormatter.ofPattern("yyyy-MM");
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
                    DateTimeFormatter yearFormatter = DateTimeFormatter.ofPattern("yyyy");
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

    public static PartitionIdWithInfoAndStatus checkPartitionCompleted(
            PartitionIdWithInfo partition) {
        switch (partition.getInfo().getPartitionType()) {
            case HOUR:
                {
                    return null;
                }
            case DAY:
                {
                    return null;
                }
            case MONTH:
                {
                    return null;
                }
            case YEAR:
                {
                    return null;
                }
            case INT_RANGE:
                return new PartitionIdWithInfoAndStatus(
                        partition.getPartitionId(),
                        partition.getInfo(),
                        BigQueryPartition.PartitionStatus.COMPLETED);
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
                        partitionValues.addAll(partitionIds);
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
            case ARRAY:
            case STRUCT:
            case JSON:
            case GEOGRAPHY:
            case BIGNUMERIC:
            case BOOL:
            case BYTES:
            case FLOAT64:
            case STRING:
            case TIME:
            case INTERVAL:
            case NUMERIC:
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
