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

package com.google.cloud.flink.bigquery.sink;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.util.StringUtils;

import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.TimePartitioning;
import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;
import com.google.cloud.flink.bigquery.services.BigQueryServicesImpl;
import com.google.cloud.flink.bigquery.sink.client.BigQueryClientWithErrorHandling;
import com.google.cloud.flink.bigquery.sink.serializer.BigQueryProtoSerializer;
import com.google.cloud.flink.bigquery.sink.serializer.BigQuerySchemaProvider;
import com.google.cloud.flink.bigquery.sink.serializer.BigQuerySchemaProviderImpl;
import com.google.cloud.flink.bigquery.sink.writer.CreateTableOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

/** Base class for developing a BigQuery sink. */
abstract class BigQueryBaseSink<IN> implements Sink<IN> {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    // Generally, a single write connection supports at least 1 MBps of throughput.
    // Check https://cloud.google.com/bigquery/docs/write-api#connections
    // Project-level BQ storage write API throughout in multi-regions (us and eu) is capped at
    // 3 GBps. Other regions allow 300 MBps.
    // Check https://cloud.google.com/bigquery/quotas#write-api-limits
    // Keeping these factors in mind, the BigQuery sink's parallelism is capped according to the
    // destination table's region.
    public static final int DEFAULT_MAX_SINK_PARALLELISM = 128;
    public static final int MULTI_REGION_MAX_SINK_PARALLELISM = 512;
    public static final List<String> BQ_MULTI_REGIONS = Arrays.asList("us", "eu");
    private static final String DEFAULT_REGION = "us";

    final BigQueryConnectOptions connectOptions;
    final BigQuerySchemaProvider schemaProvider;
    final BigQueryProtoSerializer serializer;
    final String tablePath;
    final boolean enableTableCreation;
    final String partitionField;
    final TimePartitioning.Type partitionType;
    final Long partitionExpirationMillis;
    final List<String> clusteredFields;
    final String region;
    final int maxParallelism;
    final String traceId;

    BigQueryBaseSink(BigQuerySinkConfig sinkConfig) {
        validateSinkConfig(sinkConfig);
        connectOptions = sinkConfig.getConnectOptions();
        if (sinkConfig.getSchemaProvider() == null) {
            schemaProvider = new BigQuerySchemaProviderImpl(connectOptions);
        } else {
            schemaProvider = sinkConfig.getSchemaProvider();
        }
        serializer = sinkConfig.getSerializer();
        tablePath =
                String.format(
                        "projects/%s/datasets/%s/tables/%s",
                        connectOptions.getProjectId(),
                        connectOptions.getDataset(),
                        connectOptions.getTable());
        enableTableCreation = sinkConfig.enableTableCreation();
        partitionField = sinkConfig.getPartitionField();
        partitionType = sinkConfig.getPartitionType();
        partitionExpirationMillis = sinkConfig.getPartitionExpirationMillis();
        clusteredFields = sinkConfig.getClusteredFields();
        region = getRegion(sinkConfig.getRegion());
        maxParallelism = getMaxParallelism();
        traceId = BigQueryServicesImpl.generateTraceId();
    }

    private void validateSinkConfig(BigQuerySinkConfig sinkConfig) {
        // Do not use class attribute!
        // This method is invoked before any assignments.
        BigQueryConnectOptions options = sinkConfig.getConnectOptions();
        if (options == null) {
            throw new IllegalArgumentException(
                    "BigQuery connect options in sink config cannot be null");
        }
        if (sinkConfig.getSerializer() == null) {
            throw new IllegalArgumentException("BigQuery serializer in sink config cannot be null");
        }
        if (!BigQueryClientWithErrorHandling.tableExists(options)
                && !sinkConfig.enableTableCreation()) {
            throw new IllegalStateException(
                    "Destination BigQuery table does not exist and table creation is not enabled in sink.");
        }
    }

    /** Ensures Sink's parallelism does not exceed the allowed maximum when scaling Flink job. */
    void checkParallelism(int numberOfParallelSubtasks) {
        if (numberOfParallelSubtasks > maxParallelism) {
            logger.error(
                    "Maximum allowed parallelism for Sink is {}, but attempting to create Writer number {}",
                    maxParallelism,
                    numberOfParallelSubtasks);
            throw new IllegalStateException("Attempting to create more Sink Writers than allowed");
        }
    }

    CreateTableOptions createTableOptions() {
        return new CreateTableOptions(
                enableTableCreation,
                partitionField,
                partitionType,
                partitionExpirationMillis,
                clusteredFields,
                region);
    }

    private String getRegion(String userProvidedRegion) {
        Dataset dataset = BigQueryClientWithErrorHandling.getDataset(connectOptions);
        if (dataset == null || !dataset.exists()) {
            if (StringUtils.isNullOrWhitespaceOnly(userProvidedRegion)) {
                logger.info(
                        "No destination BQ region provided by user. Using default us multi-region.");
                return DEFAULT_REGION;
            }
            return userProvidedRegion;
        }
        String datasetLocation = dataset.getLocation();
        if (!datasetLocation.equalsIgnoreCase(userProvidedRegion)) {
            logger.warn(
                    "Provided sink dataset region {} will be overridden by dataset's existing location {}",
                    userProvidedRegion,
                    datasetLocation);
        }
        return dataset.getLocation();
    }

    // Max sink parallelism is deduced using destination dataset's region.
    // Ensure instance variable 'region' is assigned before invoking this method.
    private int getMaxParallelism() {
        if (BQ_MULTI_REGIONS.contains(region)) {
            return MULTI_REGION_MAX_SINK_PARALLELISM;
        }
        return DEFAULT_MAX_SINK_PARALLELISM;
    }
}
