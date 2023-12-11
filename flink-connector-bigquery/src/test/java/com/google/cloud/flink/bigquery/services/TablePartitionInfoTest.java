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

package com.google.cloud.flink.bigquery.services;

import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.flink.bigquery.common.utils.BigQueryPartitionUtils;
import org.junit.Test;

import java.time.Instant;

import static com.google.common.truth.Truth.assertThat;

/** */
public class TablePartitionInfoTest {

    @Test
    public void testEquality() {
        String columnName = "someName";
        Instant now = Instant.now();
        TablePartitionInfo info1 =
                new TablePartitionInfo(
                        columnName,
                        BigQueryPartitionUtils.PartitionType.MONTH,
                        StandardSQLTypeName.TIMESTAMP,
                        now);
        TablePartitionInfo info2 =
                new TablePartitionInfo(
                        columnName,
                        BigQueryPartitionUtils.PartitionType.INT_RANGE,
                        StandardSQLTypeName.INT64,
                        Instant.now());

        assertThat(info1).isNotEqualTo(info2);

        TablePartitionInfo info3 =
                new TablePartitionInfo(
                        columnName,
                        BigQueryPartitionUtils.PartitionType.MONTH,
                        StandardSQLTypeName.TIMESTAMP,
                        now);

        assertThat(info1).isEqualTo(info3);
    }
}
