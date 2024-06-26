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

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.function.SerializableFunction;
import org.apache.flink.util.function.SerializableSupplier;

import com.google.cloud.flink.bigquery.fakes.StorageClientFaker;
import com.google.cloud.flink.bigquery.services.BigQueryServices;
import com.google.cloud.flink.bigquery.sink.BigQuerySink;
import com.google.cloud.flink.bigquery.sink.serializer.BigQueryTableSchemaProvider;
import com.google.cloud.flink.bigquery.table.config.BigQueryReadTableConfig;
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
import org.mockito.Mockito;

import java.io.IOException;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.flink.table.api.Expressions.$;
import static org.junit.Assert.assertThrows;

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
    public void testActualTable() throws IOException {
        BigQueryDynamicTableFactory.setTestingServices(null);
        BigQueryTableSchemaProvider.setTestingServices(null);

        String project = "bqrampupprashasti";
        String dataset = "testing_dataset";
        String table = "time";

        BigQueryTableConfig readTableConfig =
                BigQueryReadTableConfig.newBuilder()
                        .project(project)
                        .dataset(dataset)
                        .table(table)
                        .build();
        TableDescriptor readTableDescriptor =
                BigQueryTableSchemaProvider.getTableDescriptor(readTableConfig);
        System.out.println("readTableDescriptor: " + readTableDescriptor);
        tEnv.createTable("bigquery_source", readTableDescriptor);
        Table readTable = tEnv.from("bigquery_source");
        readTable = readTable.select($("*"));
        Iterator<Row> readRows = readTable.execute().collect();

        List<String> result =
                CollectionUtil.iteratorToList(readRows).stream()
                        .map(Row::toString)
                        .sorted()
                        .collect(Collectors.toList());

        System.out.println("Number of Rows read: " + result.size());
        for (String row : result) {
            System.out.println(row);
        }

        // ------------ Sink Start  -------------
        BigQueryTableConfig sinkTableConfig =
                BigQuerySinkTableConfig.newBuilder()
                        .project(project)
                        .dataset(dataset + "_copy")
                        .table(table)
                        .deliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                        .build();
        TableDescriptor sinkTableDescriptor =
                BigQueryTableSchemaProvider.getTableDescriptor(sinkTableConfig);
        System.out.println("sinkTableDescriptor: " + sinkTableDescriptor);
        tEnv.createTable("bigquery_sink", sinkTableDescriptor);
        Iterator<Row> sinkRows = readTable.executeInsert("bigquery_sink").collect();

        List<String> sinkResult =
                CollectionUtil.iteratorToList(sinkRows).stream()
                        .map(Row::toString)
                        .sorted()
                        .collect(Collectors.toList());

        System.out.println("Number of Rows Sink: " + sinkResult.size());
    }

    @Test
    public void testSchemaResolution() throws IOException {
        tEnv.createTable("bigquery_sink", createTestDDl(DeliveryGuarantee.AT_LEAST_ONCE));
        // Resolved Schema is obtained after resolution and validation.
        ResolvedSchema resolvedSchema = tEnv.from("bigquery_sink").getResolvedSchema();
        ResolvedSchema expectedResolvedSchema =
                ResolvedSchema.of(
                        Column.physical("name", DataTypes.STRING().notNull()),
                        Column.physical("number", DataTypes.BIGINT().notNull()),
                        Column.physical("ts", DataTypes.TIMESTAMP(6).notNull()));
        Assertions.assertEquals(expectedResolvedSchema, resolvedSchema);
    }

    @Test
    public void testSink() throws IOException {
        tEnv.createTable("bigquery_sink", createTestDDl(DeliveryGuarantee.AT_LEAST_ONCE));
        Iterator<Row> tableResult =
                tEnv.executeSql(
                                "Insert into bigquery_sink (name, number, ts) "
                                        + "values ('test_name', 12345, TIMESTAMP '2023-01-01 00:00:00.000000');")
                        .collect();

        List<String> result =
                CollectionUtil.iteratorToList(tableResult).stream()
                        .map(Row::toString)
                        .sorted()
                        .collect(Collectors.toList());
        // Flink does not support getting the real affected row count now.
        // So the affected row count is always -1 (unknown) for every sink.
        List<String> expected = Stream.of("+I[-1]").collect(Collectors.toList());

        Assertions.assertEquals(expected, result);
    }

    @Test
    public void testSinkExcessParallelismError() throws IOException {
        tEnv.createTable("bigquery_sink", createTestDDl(DeliveryGuarantee.AT_LEAST_ONCE));

        Sink.InitContext mockedContext = Mockito.mock(Sink.InitContext.class);
        //        Mockito.when(mockedContext.getSubtaskId()).thenReturn(1);
        Mockito.when(mockedContext.getNumberOfParallelSubtasks()).thenReturn(129);

        BigQuerySink bigQuerySink = Mockito.mock(BigQuerySink.class);
        //        Mockito.when(BigQuerySink.get(Mockito.any(), null)).thenReturn(FakeSink)
        Iterator<Row> tableResult =
                tEnv.executeSql(
                                "Insert into bigquery_sink (name, number, ts) "
                                        + "values ('test_name', 12345, TIMESTAMP '2023-01-01 00:00:00.000000');")
                        .collect();

        List<String> result =
                CollectionUtil.iteratorToList(tableResult).stream()
                        .map(Row::toString)
                        .sorted()
                        .collect(Collectors.toList());
        // Flink does not support getting the real affected row count now.
        // So the affected row count is always -1 (unknown) for every sink.
        List<String> expected = Stream.of("+I[-1]").collect(Collectors.toList());
        Assertions.assertEquals(expected, result);
    }

    @Test
    public void testErrorInOptionResolution() throws IOException {
        tEnv.createTable("bigquery_sink", createTestDDl(DeliveryGuarantee.EXACTLY_ONCE));
        tEnv.createTable("bigquery_source", createReadTestDDl());
        // Resolved Schema is obtained after resolution and validation.
        Table table = tEnv.from("bigquery_source");

        UnsupportedOperationException exception =
                assertThrows(
                        UnsupportedOperationException.class,
                        () -> table.executeInsert("bigquery_sink"));
    }

    private static TableDescriptor createTestDDl(DeliveryGuarantee deliveryGuarantee)
            throws IOException {
        BigQueryTableConfig tableConfig =
                BigQuerySinkTableConfig.newBuilder()
                        .project("project")
                        .dataset("dataset")
                        .table("table")
                        .testMode(true)
                        .deliveryGuarantee(deliveryGuarantee)
                        .build();
        return BigQueryTableSchemaProvider.getTableDescriptor(tableConfig);
    }

    private static TableDescriptor createReadTestDDl() throws IOException {
        BigQueryTableConfig tableConfig =
                BigQueryReadTableConfig.newBuilder()
                        .project("project")
                        .dataset("dataset")
                        .table("table")
                        .testMode(true)
                        .build();
        return BigQueryTableSchemaProvider.getTableDescriptor(tableConfig);
    }
}
