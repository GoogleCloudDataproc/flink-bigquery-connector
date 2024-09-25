/*
 * Copyright (C) 2024 Google Inc.
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

package com.google.cloud.flink.bigquery.table.config;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.table.api.TableDescriptor;

/**
 * Configurations for a BigQuery Table API Read.
 *
 * <p>Inherits {@link BigQueryTableConfig} for general options and defines read specific options.
 *
 * <p>Uses static inner builder to initialize new instances.
 */
public class BigQueryReadTableConfig extends BigQueryTableConfig {

    private final Integer limit;
    private final String rowRestriction;
    private final String columnProjection;
    private final Integer maxStreamCount;
    private final Long snapshotTimestamp;
    private final Boundedness boundedness;
    private final Integer partitionDiscoveryInterval;

    BigQueryReadTableConfig(
            String project,
            String dataset,
            String table,
            String credentialAccessToken,
            String credentialFile,
            String credentialKey,
            Boolean testMode,
            String columnProjection,
            Integer maxStreamCount,
            String rowRestriction,
            Integer limit,
            Long snapshotTimestamp,
            Boundedness boundedness,
            Integer partitionDiscoveryInterval) {
        super(
                project,
                dataset,
                table,
                credentialAccessToken,
                credentialFile,
                credentialKey,
                testMode);

        this.columnProjection = columnProjection;
        this.rowRestriction = rowRestriction;
        this.limit = limit;
        this.maxStreamCount = maxStreamCount;
        this.snapshotTimestamp = snapshotTimestamp;
        this.boundedness = boundedness;
        this.partitionDiscoveryInterval = partitionDiscoveryInterval;
    }

    public static BigQueryReadTableConfig.Builder newBuilder() {
        return new BigQueryReadTableConfig.Builder();
    }

    @Override
    public TableDescriptor updateTableDescriptor(TableDescriptor tableDescriptor) {
        tableDescriptor = super.updateTableDescriptor(tableDescriptor);
        TableDescriptor.Builder tableDescriptorBuilder = tableDescriptor.toBuilder();
        if (this.limit != null) {
            tableDescriptorBuilder.option(BigQueryConnectorOptions.LIMIT, this.limit);
        }
        if (this.maxStreamCount != null) {
            tableDescriptorBuilder.option(
                    BigQueryConnectorOptions.MAX_STREAM_COUNT, this.maxStreamCount);
        }
        if (this.columnProjection != null) {
            tableDescriptorBuilder.option(
                    BigQueryConnectorOptions.COLUMNS_PROJECTION, this.columnProjection);
        }
        if (this.rowRestriction != null) {
            tableDescriptorBuilder.option(
                    BigQueryConnectorOptions.ROW_RESTRICTION, this.rowRestriction);
        }
        if (this.snapshotTimestamp != null) {
            tableDescriptorBuilder.option(
                    BigQueryConnectorOptions.SNAPSHOT_TIMESTAMP, this.snapshotTimestamp);
        }
        if (this.boundedness != null) {
            tableDescriptorBuilder.option(BigQueryConnectorOptions.MODE, this.boundedness);
        }
        if (this.partitionDiscoveryInterval != null) {
            tableDescriptorBuilder.option(
                    BigQueryConnectorOptions.PARTITION_DISCOVERY_INTERVAL,
                    this.partitionDiscoveryInterval);
        }
        return tableDescriptorBuilder.build();
    }

    /** Builder for BigQueryReadTableConfig. */
    public static class Builder extends BigQueryTableConfig.Builder {

        private Integer limit;
        private String rowRestriction;

        private String columnProjection;
        private Integer maxStreamCount;
        private Long snapshotTimestamp;

        private Boundedness boundedness;

        private Integer partitionDiscoveryInterval;

        @Override
        public BigQueryReadTableConfig.Builder project(String project) {
            super.project = project;
            return this;
        }

        @Override
        public BigQueryReadTableConfig.Builder dataset(String dataset) {
            super.dataset = dataset;
            return this;
        }

        @Override
        public BigQueryReadTableConfig.Builder table(String table) {
            super.table = table;
            return this;
        }

        @Override
        public BigQueryReadTableConfig.Builder credentialAccessToken(String credentialAccessToken) {
            super.credentialAccessToken = credentialAccessToken;
            return this;
        }

        @Override
        public BigQueryReadTableConfig.Builder credentialKey(String credentialKey) {
            super.credentialKey = credentialKey;
            return this;
        }

        @Override
        public BigQueryReadTableConfig.Builder credentialFile(String credentialFile) {
            super.credentialFile = credentialFile;
            return this;
        }

        @Override
        public BigQueryReadTableConfig.Builder testMode(Boolean testMode) {
            super.testMode = testMode;
            return this;
        }

        /**
         * [OPTIONAL, Read Configuration] Integer value indicating the maximum number of
         * rows/records to be read from source. <br>
         * Default: -1 - Reads all rows from the source table.
         */
        public BigQueryReadTableConfig.Builder limit(Integer limit) {
            this.limit = limit;
            return this;
        }

        /**
         * [OPTIONAL, Read Configuration] String value indicating any filter or restriction on the
         * rows to be read from the source. <br>
         * Default: None - No filter/restriction on the rows read.
         */
        public BigQueryReadTableConfig.Builder rowRestriction(String rowRestriction) {
            this.rowRestriction = rowRestriction;
            return this;
        }

        /**
         * [OPTIONAL, Read Configuration] String value indicating any the columns to be included as
         * part of the data retrieved from the source. <br>
         * Default: None - All columns are included.
         */
        public BigQueryReadTableConfig.Builder columnProjection(String columnProjection) {
            this.columnProjection = columnProjection;
            return this;
        }

        /**
         * [OPTIONAL, Read Configuration] Integer value indicating the maximum number of streams
         * used to read from the underlying source table.<br>
         * Default: 0 - BigQuery decides the optimal amount.
         */
        public BigQueryReadTableConfig.Builder maxStreamCount(Integer maxStreamCount) {
            this.maxStreamCount = maxStreamCount;
            return this;
        }

        /**
         * [OPTIONAL, Read Configuration] Long value indicating the millis since epoch for the
         * underlying table snapshot. Connector would read records from this snapshot instance
         * table. <br>
         * Default: latest snapshot is read.
         */
        public BigQueryReadTableConfig.Builder snapshotTimestamp(Long snapshotTimestamp) {
            this.snapshotTimestamp = snapshotTimestamp;
            return this;
        }

        /**
         * [OPTIONAL, Read Configuration] Enum value indicating the "BOUNDEDNESS" of the read job.
         * Can be <code>Boundedness.BOUNDED </code> or <code>Boundedness.CONTINUOUS_UNBOUNDED</code>
         * <br>
         * Default: <code>Boundedness.BOUNDED </code> - Bounded mode.
         */
        public BigQueryReadTableConfig.Builder boundedness(Boundedness boundedness) {
            this.boundedness = boundedness;
            return this;
        }

        /**
         * [OPTIONAL, Read Configuration] Integer value indicating periodicity (in minutes) of
         * partition discovery in table. This config is used in unbounded source.<br>
         * Default: 10 minutes
         */
        public BigQueryReadTableConfig.Builder partitionDiscoveryInterval(
                Integer partitionDiscoveryInterval) {
            this.partitionDiscoveryInterval = partitionDiscoveryInterval;
            return this;
        }

        public BigQueryReadTableConfig build() {
            return new BigQueryReadTableConfig(
                    project,
                    dataset,
                    table,
                    credentialAccessToken,
                    credentialFile,
                    credentialKey,
                    testMode,
                    columnProjection,
                    maxStreamCount,
                    rowRestriction,
                    limit,
                    snapshotTimestamp,
                    boundedness,
                    partitionDiscoveryInterval);
        }
    }
}
