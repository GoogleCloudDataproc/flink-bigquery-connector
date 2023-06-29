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

package com.google.cloud.flink.bigquery.table.restrictions;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import com.google.cloud.bigquery.StandardSQLTypeName;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/** Utility class to handle the BigQuery partition conversions to Flink types and structures. */
@Internal
public class BigQueryPartition {

    private BigQueryPartition() {}

    static List<String> partitionIdToDateFormat(
            List<String> partitions, String fromFormat, String toFormat) {
        SimpleDateFormat parseFormat = new SimpleDateFormat(fromFormat);
        SimpleDateFormat printFormat = new SimpleDateFormat(toFormat);

        return partitions.stream()
                .map(
                        id -> {
                            try {
                                return parseFormat.parse(id);
                            } catch (ParseException ex) {
                                throw new RuntimeException(
                                        "Problems parsing the temporal value: " + id);
                            }
                        })
                .map(date -> printFormat.format(date))
                .map(strDate -> String.format("'%s'", strDate))
                .collect(Collectors.toList());
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
                        partitionIds.stream().allMatch(pid -> pid.length() == firstId.length()),
                        "Some elements in the partition id list have a different length: "
                                + partitionIds.toString());
                switch (firstId.length()) {
                    case 4:
                        // we have yearly partitions
                        partitionValues.addAll(
                                partitionIds.stream()
                                        .map(id -> String.format("'%s'", id))
                                        .collect(Collectors.toList()));
                        break;
                    case 6:
                        // we have monthly partitions
                        partitionValues.addAll(
                                partitionIdToDateFormat(partitionIds, "yyyyMM", "yyyy-MM"));
                        break;
                    case 8:
                        // we have daily partitions
                        partitionValues.addAll(
                                partitionIdToDateFormat(partitionIds, "yyyyMMdd", "yyyy-MM-dd"));
                        break;
                    case 10:
                        // we have hourly partitions
                        partitionValues.addAll(
                                partitionIdToDateFormat(
                                        partitionIds, "yyyyMMddHH", "yyyy-MM-dd HH:mm:ss"));
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
