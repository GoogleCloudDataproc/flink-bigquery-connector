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

package com.google.cloud.flink.bigquery.table;

import org.apache.flink.table.api.DataTypes;

import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;
import com.google.cloud.flink.bigquery.services.BigQueryServices;
import com.google.cloud.flink.bigquery.services.BigQueryServicesFactory;
import com.google.cloud.flink.bigquery.services.TablePartitionInfo;
import com.google.cloud.flink.bigquery.source.config.BigQueryReadOptions;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests partition discovery and pruning without a BigQuery connection. */
public class BigQueryDynamicTableSourcePartitionTest {
    @Test
    public void testDayTimestampPartitionDiscoveryAndPruning() {
        BigQueryServices services = mock(BigQueryServices.class);
        BigQueryServices.QueryDataClient client = mock(BigQueryServices.QueryDataClient.class);
        when(services.createQueryDataClient(any())).thenReturn(client);
        BigQueryConnectOptions options =
                BigQueryConnectOptions.builder()
                        .setProjectId("project")
                        .setDataset("dataset")
                        .setTable("table")
                        .setTestingBigQueryServices(() -> services)
                        .build();
        when(client.retrievePartitionColumnInfo("project", "dataset", "table"))
                .thenReturn(
                        Optional.of(
                                new TablePartitionInfo(
                                        "event_time",
                                        TablePartitionInfo.PartitionType.DAY,
                                        StandardSQLTypeName.TIMESTAMP,
                                        null)));
        when(client.retrieveTablePartitions("project", "dataset", "table"))
                .thenReturn(Collections.singletonList("20240229"));
        try {
            BigQueryDynamicTableSource source =
                    new BigQueryDynamicTableSource(
                            BigQueryReadOptions.builder()
                                    .setBigQueryConnectOptions(options)
                                    .build(),
                            DataTypes.ROW(
                                    DataTypes.FIELD("event_time", DataTypes.TIMESTAMP_LTZ(6))));
            List<Map<String, String>> partitions = source.listPartitions().get();
            assertThat(partitions)
                    .containsExactly(Collections.singletonMap("event_time", "2024-02-29"));
            source.applyPartitions(partitions);
            assertThat(source.getReadOptions().getRowRestriction())
                    .isEqualTo("(event_time >= '2024-02-29' AND event_time < '2024-03-01')");
        } finally {
            BigQueryServicesFactory.instance(options).defaultImplementation();
        }
    }
}
