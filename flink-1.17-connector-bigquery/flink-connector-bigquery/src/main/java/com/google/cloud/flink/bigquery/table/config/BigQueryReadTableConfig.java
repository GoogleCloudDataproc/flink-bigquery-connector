package com.google.cloud.flink.bigquery.table.config;

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

    BigQueryReadTableConfig(
            String project,
            String dataset,
            String table,
            String credentialAccessToken,
            String credentialFile,
            String credentialKey,
            boolean testMode,
            String columnProjection,
            Integer maxStreamCount,
            String rowRestriction,
            Integer limit,
            Long snapshotTimestamp) {
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

        return tableDescriptorBuilder.build();
    }

    /** Builder for BigQueryTableConfig. */
    public static class Builder extends BigQueryTableConfig.Builder {

        private Integer limit;
        private String rowRestriction;

        private String columnProjection;
        private Integer maxStreamCount;
        private Long snapshotTimestamp;

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
         * Read Configuration: Long value indicating the millis since epoch for the underlying table
         * snapshot. Connector would read records from this snapshot instance table. <br>
         * Default: latest snapshot is read.
         */
        public BigQueryTableConfig.Builder snapshotTimestamp(Long snapshotTimestamp) {
            this.snapshotTimestamp = snapshotTimestamp;
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
                    snapshotTimestamp);
        }
    }
}
