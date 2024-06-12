package com.google.cloud.flink.bigquery.table.config;

import org.apache.flink.table.api.TableDescriptor;

/**
 * Configurations for a BigQuery Table API Read and Write.
 *
 * <p>Uses static inner builder to initialize new instances.
 */
public class BigQueryTableConfig {

    private final String project;
    private final String dataset;
    private final String table;
    private final String credentialAccessToken;
    private final String credentialFile;
    private final String credentialKey;
    private final Boolean testMode;

    public static BigQueryTableConfig.Builder newBuilder() {
        return new BigQueryTableConfig.Builder();
    }

    BigQueryTableConfig(
            String project,
            String dataset,
            String table,
            String credentialAccessToken,
            String credentialFile,
            String credentialKey,
            Boolean testMode) {
        this.project = project;
        this.dataset = dataset;
        this.table = table;
        this.credentialAccessToken = credentialAccessToken;
        this.credentialFile = credentialFile;
        this.credentialKey = credentialKey;
        this.testMode = testMode;
    }

    public TableDescriptor updateTableDescriptor(TableDescriptor tableDescriptor) {
        TableDescriptor.Builder tableDescriptorBuilder = tableDescriptor.toBuilder();
        if (this.table != null) {
            tableDescriptorBuilder.option(BigQueryConnectorOptions.TABLE, this.table);
        }
        if (this.project != null) {
            tableDescriptorBuilder.option(BigQueryConnectorOptions.PROJECT, this.project);
        }
        if (this.dataset != null) {
            tableDescriptorBuilder.option(BigQueryConnectorOptions.DATASET, this.dataset);
        }
        if (this.credentialKey != null) {
            tableDescriptorBuilder.option(
                    BigQueryConnectorOptions.CREDENTIALS_KEY, this.credentialKey);
        }
        if (this.credentialFile != null) {
            tableDescriptorBuilder.option(
                    BigQueryConnectorOptions.CREDENTIALS_FILE, this.credentialFile);
        }
        if (this.credentialAccessToken != null) {
            tableDescriptorBuilder.option(
                    BigQueryConnectorOptions.CREDENTIALS_ACCESS_TOKEN, this.credentialAccessToken);
        }
        if (this.testMode != null) {
            tableDescriptorBuilder.option(BigQueryConnectorOptions.TEST_MODE, this.testMode);
        }
        return tableDescriptorBuilder.build();
    }

    public String getDataset() {
        return dataset;
    }

    public String getTable() {
        return table;
    }

    public String getProject() {
        return project;
    }

    /** Builder for BigQueryTableConfig. */
    public static class Builder {

        String project;
        String dataset;
        String table;
        String credentialAccessToken;
        String credentialFile;
        String credentialKey;
        boolean testMode;

        /**
         * [REQUIRED] The GCP BigQuery Project ID which contains the desired connector (source or
         * sink) table.
         */
        public BigQueryTableConfig.Builder project(String project) {
            this.project = project;
            return this;
        }

        /**
         * [REQUIRED] The GCP BigQuery Dataset Name which contains the desired connector(source or
         * sink) table.
         */
        public BigQueryTableConfig.Builder dataset(String dataset) {
            this.dataset = dataset;
            return this;
        }

        /** [REQUIRED] Name of the table to connect to in BigQuery. */
        public BigQueryTableConfig.Builder table(String table) {
            this.table = table;
            return this;
        }

        /** [OPTIONAL] Specifies the GCP access token to use as credentials. */
        public BigQueryTableConfig.Builder credentialAccessToken(String credentialAccessToken) {
            this.credentialAccessToken = credentialAccessToken;
            return this;
        }

        /** [OPTIONAL] Specifies the GCP credentials file to use as credentials. */
        public BigQueryTableConfig.Builder credentialKey(String credentialKey) {
            this.credentialKey = credentialKey;
            return this;
        }

        /** [OPTIONAL] Specifies the GCP credentials file to use as credentials. */
        public BigQueryTableConfig.Builder credentialFile(String credentialFile) {
            this.credentialFile = credentialFile;
            return this;
        }

        /**
         * [OPTIONAL] Boolean value indicating if the connector is run in test mode. In Test Mode,
         * BigQuery Tables are not modified, mock sources and sinks are used instead. <br>
         * Default: false
         */
        public BigQueryTableConfig.Builder testMode(Boolean testMode) {
            this.testMode = testMode;
            return this;
        }

        public BigQueryTableConfig build() {
            return new BigQueryTableConfig(
                    project,
                    dataset,
                    table,
                    credentialAccessToken,
                    credentialFile,
                    credentialKey,
                    testMode);
        }
    }
}