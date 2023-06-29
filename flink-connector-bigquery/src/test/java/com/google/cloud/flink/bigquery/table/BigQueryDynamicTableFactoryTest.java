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
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.utils.FactoryMocks;

import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;
import com.google.cloud.flink.bigquery.common.config.CredentialsOptions;
import com.google.cloud.flink.bigquery.source.config.BigQueryReadOptions;
import com.google.cloud.flink.bigquery.table.config.BigQueryConnectorOptions;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/** Tests for the {@link BigQueryDynamicTableSource} factory class. */
public class BigQueryDynamicTableFactoryTest {

    private static final ResolvedSchema SCHEMA =
            new ResolvedSchema(
                    Arrays.asList(
                            Column.physical("aaa", DataTypes.INT().notNull()),
                            Column.physical("bbb", DataTypes.STRING().notNull()),
                            Column.physical("ccc", DataTypes.DOUBLE()),
                            Column.physical("ddd", DataTypes.DECIMAL(31, 18)),
                            Column.physical("eee", DataTypes.TIMESTAMP(3))),
                    Collections.emptyList(),
                    UniqueConstraint.primaryKey("name", Arrays.asList("bbb", "aaa")));

    @Test
    public void testBigQuerySourceCommonProperties() throws IOException {
        DynamicTableSource actualSource =
                FactoryMocks.createTableSource(SCHEMA, getRequiredOptions());

        BigQueryDynamicTableSource expectedSource =
                new BigQueryDynamicTableSource(
                        getConnectorOptions(), SCHEMA.toPhysicalRowDataType());
        Assert.assertEquals("The sources are not equals.", actualSource, expectedSource);
    }

    @Test
    public void testBigQueryReadProperties() throws IOException {
        Map<String, String> properties = getRequiredOptions();
        properties.put(BigQueryConnectorOptions.COLUMNS_PROJECTION.key(), "aaa,bbb");
        properties.put(BigQueryConnectorOptions.MAX_STREAM_COUNT.key(), "100");
        properties.put(
                BigQueryConnectorOptions.ROW_RESTRICTION.key(), "aaa > 10 AND NOT bbb IS NULL");
        properties.put(
                BigQueryConnectorOptions.SNAPSHOT_TIMESTAMP.key(),
                "" + Instant.EPOCH.toEpochMilli());

        DynamicTableSource actual = FactoryMocks.createTableSource(SCHEMA, properties);

        BigQueryReadOptions connectorOptions = getConnectorOptions();
        BigQueryReadOptions readOptions =
                BigQueryReadOptions.builder()
                        .setColumnNames(Arrays.asList("aaa", "bbb"))
                        .setMaxStreamCount(100)
                        .setRowRestriction("aaa > 10 AND NOT bbb IS NULL")
                        .setSnapshotTimestampInMillis(Instant.EPOCH.toEpochMilli())
                        .setBigQueryConnectOptions(connectorOptions.getBigQueryConnectOptions())
                        .build();

        BigQueryDynamicTableSource expected =
                new BigQueryDynamicTableSource(readOptions, SCHEMA.toPhysicalRowDataType());

        Assertions.assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void testBigQuerySourceValidation() {
        // max num of streams should be positive
        assertSourceValidationRejects(
                BigQueryConnectorOptions.MAX_STREAM_COUNT.key(),
                "-5",
                "The max number of streams should be zero or positive.");
        // the snapshot timestamp in millis should at least be equal to epoch
        assertSourceValidationRejects(
                BigQueryConnectorOptions.SNAPSHOT_TIMESTAMP.key(),
                "-1000",
                "The oldest timestamp should be equal or bigger than epoch.");
    }

    private void assertSourceValidationRejects(String key, String value, String errorMessage) {
        Assertions.assertThatThrownBy(
                        () ->
                                FactoryMocks.createTableSource(
                                        SCHEMA, getRequiredOptionsWithSetting(key, value)))
                .hasStackTraceContaining(errorMessage);
    }

    private static Map<String, String> getRequiredOptionsWithSetting(String key, String value) {
        Map<String, String> requiredOptions = getRequiredOptions();
        requiredOptions.put(key, value);
        return requiredOptions;
    }

    private static Map<String, String> getRequiredOptions() {
        Map<String, String> options = new HashMap<>();
        options.put(FactoryUtil.CONNECTOR.key(), "bigquery");
        options.put(BigQueryConnectorOptions.PROJECT.key(), "project");
        options.put(BigQueryConnectorOptions.DATASET.key(), "dataset");
        options.put(BigQueryConnectorOptions.TABLE.key(), "table");
        return options;
    }

    private static BigQueryReadOptions getConnectorOptions() throws IOException {
        return BigQueryReadOptions.builder()
                .setBigQueryConnectOptions(
                        BigQueryConnectOptions.builder()
                                .setDataset("dataset")
                                .setProjectId("project")
                                .setTable("table")
                                .setCredentialsOptions(CredentialsOptions.builder().build())
                                .build())
                .build();
    }
}
