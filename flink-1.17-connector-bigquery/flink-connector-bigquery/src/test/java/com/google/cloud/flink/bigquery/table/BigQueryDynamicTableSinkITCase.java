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

import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.util.function.SerializableFunction;
import org.apache.flink.util.function.SerializableSupplier;

import com.google.cloud.flink.bigquery.fakes.StorageClientFaker;
import com.google.cloud.flink.bigquery.services.BigQueryServices;
import com.google.cloud.flink.bigquery.sink.serializer.BigQueryTableSchemaProvider;
import com.google.cloud.flink.bigquery.table.config.BigQuerySinkTableConfig;
import com.google.cloud.flink.bigquery.table.config.BigQueryTableConfig;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/** An integration test for the SQL interface of the BigQuery connector. */
public class BigQueryDynamicTableSinkITCase {

    private static final int PARALLELISM = 1;
    private static final Integer TOTAL_ROW_COUNT_PER_STREAM = 10000;
    private static final Integer STREAM_COUNT = 2;
    private static final Schema AVRO_SCHEMA = StorageClientFaker.SIMPLE_AVRO_SCHEMA;

    @RegisterExtension
    static final MiniClusterExtension MINI_CLUSTER_RESOURCE =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(PARALLELISM)
                            .build());

    @BeforeAll
    public static void beforeTest() throws Exception {
        // create a data generator based on the test schema
        SerializableFunction<StorageClientFaker.RecordGenerationParams, List<GenericRecord>>
                dataGenerator =
                        params -> {
                            Schema schema = new Schema.Parser().parse(params.getAvroSchemaString());
                            return IntStream.range(0, params.getRecordCount())
                                    .mapToObj(
                                            i -> {
                                                GenericRecord record =
                                                        new GenericData.Record(schema);
                                                record.put("name", "record_name_" + i);
                                                record.put("numer", (long) i * 2);
                                                record.put("ts", Instant.now().toEpochMilli());
                                                return record;
                                            })
                                    .collect(Collectors.toList());
                        };

        SerializableSupplier<BigQueryServices> testingServices =
                StorageClientFaker.createReadOptions(
                                TOTAL_ROW_COUNT_PER_STREAM,
                                STREAM_COUNT,
                                AVRO_SCHEMA.toString(),
                                dataGenerator)
                        .getBigQueryConnectOptions()
                        .getTestingBigQueryServices();

        // init the testing services and inject them into the table factory
        BigQueryDynamicTableFactory.setTestingServices(testingServices);
        BigQueryTableSchemaProvider.setTestingServices(testingServices);
    }

    public static StreamExecutionEnvironment env;
    public static StreamTableEnvironment tEnv;

    @BeforeEach
    public void before() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        tEnv = StreamTableEnvironment.create(env);
    }

    @Test
    public void testSchemaResolution() throws IOException {
        tEnv.createTable("bigquery_sink", createTestDDl(null));
        // Resolved Schema is obtained after resolution and validation.
        ResolvedSchema resolvedSchema = tEnv.from("bigquery_sink").getResolvedSchema();
        ResolvedSchema expectedResolvedSchema =
                ResolvedSchema.of(
                        Column.physical("name", DataTypes.STRING().notNull()),
                        Column.physical("number", DataTypes.BIGINT().notNull()),
                        Column.physical("ts", DataTypes.TIMESTAMP(6).notNull()));
        Assertions.assertEquals(expectedResolvedSchema, resolvedSchema);
    }

    private static TableDescriptor createTestDDl(Map<String, String> extraOptions)
            throws IOException {
        BigQueryTableConfig tableConfig =
                BigQuerySinkTableConfig.newBuilder()
                        .project("project")
                        .dataset("dataset")
                        .table("table")
                        .testMode(true)
                        .deliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                        .build();
        return BigQueryTableSchemaProvider.getTableDescriptor(tableConfig);
    }
}
