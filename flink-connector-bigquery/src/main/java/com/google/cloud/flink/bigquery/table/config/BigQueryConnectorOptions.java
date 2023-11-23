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

package com.google.cloud.flink.bigquery.table.config;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/**
 * Base options for the BigQuery connector. Needs to be public so that the {@link
 * org.apache.flink.table.api.TableDescriptor} can access it.
 */
@PublicEvolving
public class BigQueryConnectorOptions {

    private BigQueryConnectorOptions() {}

    public static final ConfigOption<String> PROJECT =
            ConfigOptions.key("project")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Specifies the GCP project for BigQuery.");
    public static final ConfigOption<String> DATASET =
            ConfigOptions.key("dataset")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Specifies the BigQuery dataset name.");
    public static final ConfigOption<String> TABLE =
            ConfigOptions.key("table")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Specifies the BigQuery table name.");
    public static final ConfigOption<Integer> LIMIT =
            ConfigOptions.key("read.limit")
                    .intType()
                    .defaultValue(-1)
                    .withDescription("Specifies the limit number of rows retrieved.");
    public static final ConfigOption<String> ROW_RESTRICTION =
            ConfigOptions.key("read.row.restriction")
                    .stringType()
                    .defaultValue("")
                    .withDescription("Specifies the row restriction for data retrieval.");
    public static final ConfigOption<String> COLUMNS_PROJECTION =
            ConfigOptions.key("read.columns.projection")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Specifies, as a comma separated list of values, "
                                    + "the columns to be included as part of the data retrieved.");
    public static final ConfigOption<Integer> MAX_STREAM_COUNT =
            ConfigOptions.key("read.streams.maxcount")
                    .intType()
                    .defaultValue(0)
                    .withDescription(
                            "The max number of streams used to read from the underlying table,"
                                    + " BigQuery can decide for less than this number.");
    public static final ConfigOption<Long> SNAPSHOT_TIMESTAMP =
            ConfigOptions.key("read.snapshot.timestamp")
                    .longType()
                    .noDefaultValue()
                    .withDescription("The millis since epoch for the underlying table snapshot.");
    public static final ConfigOption<String> CREDENTIALS_ACCESS_TOKEN =
            ConfigOptions.key("credentials.accesstoken")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Specifies the GCP access token to use as credentials.");
    public static final ConfigOption<String> CREDENTIALS_FILE =
            ConfigOptions.key("credentials.file")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Specifies the GCP credentials file to use.");
    public static final ConfigOption<String> CREDENTIALS_KEY =
            ConfigOptions.key("credentials.key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Specifies the GCP credentials key to use.");
    public static final ConfigOption<Boolean> TEST_MODE =
            ConfigOptions.key("test.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Specifies if the connector should run in test mode.");
}
