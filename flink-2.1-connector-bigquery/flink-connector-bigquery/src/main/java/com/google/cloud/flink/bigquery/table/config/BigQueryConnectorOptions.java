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
import org.apache.flink.connector.base.DeliveryGuarantee;

import com.google.cloud.bigquery.TimePartitioning;

/**
 * Base options for the BigQuery connector. Needs to be public so that the {@link
 * org.apache.flink.table.api.TableDescriptor} can access it.
 */
@PublicEvolving
public class BigQueryConnectorOptions {

    private BigQueryConnectorOptions() {}

    /**
     * [REQUIRED] The GCP BigQuery Project ID which contains the desired connector (source or sink)
     * table.
     */
    public static final ConfigOption<String> PROJECT =
            ConfigOptions.key("project")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Specifies the GCP project for BigQuery.");

    /**
     * [OPTIONAL] The quota project ID to use when connecting.
     */
    public static final ConfigOption<String> QUOTA_PROJECT_ID =
            ConfigOptions.key("quota-project-id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Specifies the BigQuery quota project to use.");

    /**
     * [REQUIRED] The GCP BigQuery Dataset Name which contains the desired connector(source or sink)
     * table.
     */
    public static final ConfigOption<String> DATASET =
            ConfigOptions.key("dataset")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Specifies the BigQuery dataset name.");

    /** [REQUIRED] Name of the table to connect to in BigQuery. */
    public static final ConfigOption<String> TABLE =
            ConfigOptions.key("table")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Specifies the BigQuery table name.");

    /**
     * [OPTIONAL, Read Configuration] Integer value indicating the maximum number of rows/records to
     * be read from source. <br>
     * Default: -1 - Reads all rows from the source table.
     */
    public static final ConfigOption<Integer> LIMIT =
            ConfigOptions.key("read.limit")
                    .intType()
                    .defaultValue(-1)
                    .withDescription("Specifies the limit number of rows retrieved.");

    /**
     * [OPTIONAL, Read Configuration] String value indicating any filter or restriction on the rows
     * to be read from the source. <br>
     * Default: None - No filter/restriction on the rows read.
     */
    public static final ConfigOption<String> ROW_RESTRICTION =
            ConfigOptions.key("read.row.restriction")
                    .stringType()
                    .defaultValue("")
                    .withDescription("Specifies the row restriction for data retrieval.");

    /**
     * [OPTIONAL, Read Configuration] String value indicating any the columns to be included as part
     * of the data retrieved from the source. <br>
     * Default: None - All columns are included.
     */
    public static final ConfigOption<String> COLUMNS_PROJECTION =
            ConfigOptions.key("read.columns.projection")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Specifies, as a comma separated list of values, "
                                    + "the columns to be included as part of the data retrieved.");

    /**
     * [OPTIONAL, Read Configuration] Integer value indicating the maximum number of streams used to
     * read from the underlying source table.<br>
     * Default: 0 - BigQuery decides the optimal amount.
     */
    public static final ConfigOption<Integer> MAX_STREAM_COUNT =
            ConfigOptions.key("read.streams.max-count")
                    .intType()
                    .defaultValue(0)
                    .withDescription(
                            "The max number of streams used to read from the underlying table,"
                                    + " BigQuery can decide for less than this number.");

    /**
     * Read Configuration: Long value indicating the millis since epoch for the underlying table
     * snapshot. Connector would read records from this snapshot instance table. <br>
     * Default: latest snapshot is read.
     */
    public static final ConfigOption<Long> SNAPSHOT_TIMESTAMP =
            ConfigOptions.key("read.snapshot.timestamp")
                    .longType()
                    .noDefaultValue()
                    .withDescription("The millis since epoch for the underlying table snapshot.");

    /** [OPTIONAL] Specifies the GCP access token to use as credentials. */
    public static final ConfigOption<String> CREDENTIALS_ACCESS_TOKEN =
            ConfigOptions.key("credentials.access-token")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Specifies the GCP access token to use as credentials.");

    /** [OPTIONAL] Specifies the GCP credentials file to use as credentials. */
    public static final ConfigOption<String> CREDENTIALS_FILE =
            ConfigOptions.key("credentials.file")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Specifies the GCP credentials file to use.");

    /** [OPTIONAL] Specifies the GCP credentials file to use as credentials. */
    public static final ConfigOption<String> CREDENTIALS_KEY =
            ConfigOptions.key("credentials.key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Specifies the GCP credentials key to use.");

    /**
     * [OPTIONAL] Boolean value indicating if the connector is run in test mode. In Test Mode,
     * BigQuery Tables are not modified, mock sources and sinks are used instead. <br>
     * Default: false
     */
    public static final ConfigOption<Boolean> TEST_MODE =
            ConfigOptions.key("test.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Specifies if the connector should run in test mode.");

    /**
     * [OPTIONAL, Sink Configuration] Enum value indicating the delivery guarantee of the sink. Can
     * be <code>DeliveryGuarantee.AT_LEAST_ONCE</code> or <code>DeliveryGuarantee.EXACTLY_ONCE
     * </code><br>
     * Default: <code>DeliveryGuarantee.AT_LEAST_ONCE</code> - At-least Once Mode.
     */
    public static final ConfigOption<DeliveryGuarantee> DELIVERY_GUARANTEE =
            ConfigOptions.key("write.delivery-guarantee")
                    .enumType(DeliveryGuarantee.class)
                    .defaultValue(DeliveryGuarantee.AT_LEAST_ONCE)
                    .withDescription("Delivery Guarantee (AT_LEAST_ONCE or EXACTLY_ONCE");

    /** [OPTIONAL, Sink Configuration] Int value indicating the parallelism of the sink. */
    public static final ConfigOption<Integer> SINK_PARALLELISM =
            ConfigOptions.key("write.parallelism")
                    .intType()
                    .noDefaultValue()
                    .withDescription("Sink parallelism");

    /** [OPTIONAL, Sink Configuration] Boolean flag controlling table creation in the sink. */
    public static final ConfigOption<Boolean> ENABLE_TABLE_CREATION =
            ConfigOptions.key("write.enable-table-creation")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Enable table creation in sink");

    /** [OPTIONAL, Sink Configuration] Name of partitioning field. */
    public static final ConfigOption<String> PARTITION_FIELD =
            ConfigOptions.key("write.partition-field")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Field for partitioning destination BigQuery table (used if new table is created)");

    /** [OPTIONAL, Sink Configuration] Enum value indicating the partitioning frequency. */
    public static final ConfigOption<TimePartitioning.Type> PARTITION_TYPE =
            ConfigOptions.key("write.partition-type")
                    .enumType(TimePartitioning.Type.class)
                    .noDefaultValue()
                    .withDescription(
                            "Time based partitioning frequency in destination BigQuery table (used if new table is created)");

    /** [OPTIONAL, Sink Configuration] Long value indicating the partition expiration. */
    public static final ConfigOption<Long> PARTITION_EXPIRATION_MILLIS =
            ConfigOptions.key("write.partition-expiration-millis")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "Partition expiration in destination BigQuery table (used if new table is created)");

    /** [OPTIONAL, Sink Configuration] Names of clustered fields, comma separated. */
    public static final ConfigOption<String> CLUSTERED_FIELDS =
            ConfigOptions.key("write.clustered-fields")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Comma separated fields for clustering destination BigQuery table (used if new table is created)");

    /** [OPTIONAL, Sink Configuration] GCP region of destination BigQuery table. */
    public static final ConfigOption<String> REGION =
            ConfigOptions.key("write.region")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "GCP region of destination BigQuery table (used if new table is created)");

    /** [OPTIONAL, Sink Configuration] Fail if serializer cannot convert record to proto. */
    public static final ConfigOption<Boolean> FATALIZE_SERIALIZER =
            ConfigOptions.key("write.fatalize-serializer")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Fail if serializer cannot convert record to proto");
}
