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

import org.apache.flink.table.api.TableDescriptor;

/**
 * Configurations for a BigQuery Table API Read and Write.
 *
 * <p>Uses static inner builder to initialize new instances.
 */
public abstract class BigQueryTableConfig {

    private final String project;
    private final String dataset;
    private final String table;
    private final String credentialAccessToken;
    private final String credentialFile;
    private final String credentialKey;
    private final Boolean testMode;

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

    public String getCredentialAccessToken() {
        return credentialAccessToken;
    }

    public String getCredentialFile() {
        return credentialFile;
    }

    public String getCredentialKey() {
        return credentialKey;
    }

    /** Builder for BigQueryTableConfig. */
    public abstract static class Builder {

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
        public abstract BigQueryTableConfig.Builder project(String project);

        /**
         * [REQUIRED] The GCP BigQuery Dataset Name which contains the desired connector(source or
         * sink) table.
         */
        public abstract BigQueryTableConfig.Builder dataset(String dataset);

        /** [REQUIRED] Name of the table to connect to in BigQuery. */
        public abstract BigQueryTableConfig.Builder table(String table);

        /** [OPTIONAL] Specifies the GCP access token to use as credentials. */
        public abstract BigQueryTableConfig.Builder credentialAccessToken(
                String credentialAccessToken);

        /** [OPTIONAL] Specifies the GCP credentials file to use as credentials. */
        public abstract BigQueryTableConfig.Builder credentialKey(String credentialKey);

        /** [OPTIONAL] Specifies the GCP credentials file to use as credentials. */
        public abstract BigQueryTableConfig.Builder credentialFile(String credentialFile);

        /**
         * [OPTIONAL] Boolean value indicating if the connector is run in test mode. In Test Mode,
         * BigQuery Tables are not modified, mock sources and sinks are used instead. <br>
         * Default: false
         */
        public abstract BigQueryTableConfig.Builder testMode(Boolean testMode);
    }
}
