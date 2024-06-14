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

/**
 * Configurations for a BigQuery Table API Write.
 *
 * <p>Inherits {@link BigQueryTableConfig} for general options and defines sink specific options.
 *
 * <p>Uses static inner builder to initialize new instances.
 */
public class BigQuerySinkTableConfig extends BigQueryTableConfig {

    BigQuerySinkTableConfig(
            String project,
            String dataset,
            String table,
            String credentialAccessToken,
            String credentialFile,
            String credentialKey,
            boolean testMode) {
        super(
                project,
                dataset,
                table,
                credentialAccessToken,
                credentialFile,
                credentialKey,
                testMode);
    }
}
