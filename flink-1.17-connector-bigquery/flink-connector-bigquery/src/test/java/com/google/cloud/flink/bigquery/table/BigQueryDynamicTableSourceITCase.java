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

import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.function.SerializableFunction;

import com.google.cloud.flink.bigquery.fakes.StorageClientFaker;
import com.google.cloud.flink.bigquery.table.config.BigQueryConnectorOptions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/** An integration test for the SQL interface of the BigQuery connector. */
public class BigQueryDynamicTableSourceITCase {

    private static final int PARALLELISM = 1;
    private static final Integer TOTAL_ROW_COUNT_PER_STREAM = 10000;
    private static final Integer STREAM_COUNT = 2;
    private static final Schema AVRO_SCHEMA =
            SchemaBuilder.record("testRecord")
                    .fields()
                    .requiredInt("id")
                    .optionalDouble("optDouble")
                    .optionalString("optString")
                    .name("ts")
                    .type(
                            LogicalTypes.timestampMillis()
                                    .addToSchema(Schema.create(Schema.Type.LONG)))
                    .noDefault()
                    .name("reqSubRecord")
                    .type(
                            SchemaBuilder.record("reqSubRecord")
                                    .fields()
                                    .requiredBoolean("reqBoolean")
                                    .name("reqTs")
                                    .type(
                                            LogicalTypes.timestampMillis()
                                                    .addToSchema(Schema.create(Schema.Type.LONG)))
                                    .noDefault()
                                    .endRecord())
                    .noDefault()
                    .name("optArraySubRecords")
                    .type()
                    .nullable()
                    .array()
                    .items(
                            SchemaBuilder.record("myOptionalArraySubRecordType")
                                    .fields()
                                    .requiredLong("reqLong")
                                    .optionalBytes("optBytes")
                                    .endRecord())
                    .noDefault()
                    .endRecord();

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
                                                record.put("id", i);
                                                record.put("optDouble", (double) i * 2);
                                                record.put("optString", "s" + i);
                                                record.put("ts", Instant.now().toEpochMilli());

                                                GenericData.Record reqSubRecord =
                                                        new GenericData.Record(
                                                                schema.getField("reqSubRecord")
                                                                        .schema());
                                                reqSubRecord.put("reqBoolean", false);
                                                reqSubRecord.put(
                                                        "reqTs", Instant.now().toEpochMilli());
                                                record.put("reqSubRecord", reqSubRecord);

                                                GenericData.Record optArraySubRecords =
                                                        new GenericData.Record(
                                                                schema.getField(
                                                                                "optArraySubRecords")
                                                                        .schema()
                                                                        .getTypes()
                                                                        .get(0)
                                                                        .getElementType());
                                                optArraySubRecords.put("reqLong", (long) i * 100);
                                                optArraySubRecords.put(
                                                        "optBytes",
                                                        ByteBuffer.wrap(new byte[4]).putInt(i));
                                                record.put(
                                                        "optArraySubRecords",
                                                        Arrays.asList(
                                                                optArraySubRecords,
                                                                optArraySubRecords));

                                                return record;
                                            })
                                    .collect(Collectors.toList());
                        };

        // init the testing services and inject them into the table factory
        BigQueryDynamicTableSourceFactory.setTestingServices(
                StorageClientFaker.createReadOptions(
                                TOTAL_ROW_COUNT_PER_STREAM,
                                STREAM_COUNT,
                                AVRO_SCHEMA.toString(),
                                dataGenerator)
                        .getBigQueryConnectOptions()
                        .getTestingBigQueryServices());
    }

    public static StreamExecutionEnvironment env;
    public static StreamTableEnvironment tEnv;

    @BeforeEach
    public void before() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        tEnv = StreamTableEnvironment.create(env);
    }

    @Test
    public void testSource() {
        tEnv.executeSql(createTestDDl(null));

        Iterator<Row> collected = tEnv.executeSql("SELECT * FROM bigquery_source").collect();
        List<String> result =
                CollectionUtil.iteratorToList(collected).stream()
                        .map(Row::toString)
                        .sorted()
                        .collect(Collectors.toList());

        // we check the data count correlates with the generated total
        Assertions.assertThat(result).hasSize(TOTAL_ROW_COUNT_PER_STREAM * STREAM_COUNT);
    }

    @Test
    public void testLimit() {
        tEnv.executeSql(createTestDDl(null));

        Iterator<Row> collected =
                tEnv.executeSql("SELECT * FROM bigquery_source LIMIT 1").collect();
        List<String> result =
                CollectionUtil.iteratorToList(collected).stream()
                        .map(Row::toString)
                        .sorted()
                        .collect(Collectors.toList());

        Assertions.assertThat(result).hasSize(1);
    }

    @Test
    public void testProject() {
        tEnv.executeSql(createTestDDl(null));

        Iterator<Row> collected =
                tEnv.executeSql("SELECT id, optDouble, optString FROM bigquery_source LIMIT 1")
                        .collect();
        List<String> result =
                CollectionUtil.iteratorToList(collected).stream()
                        .map(Row::toString)
                        .sorted()
                        .collect(Collectors.toList());

        List<String> expected = Stream.of("+I[0, 0.0, s0]").sorted().collect(Collectors.toList());

        Assertions.assertThat(result).isEqualTo(expected);
    }

    @Test
    public void testRestriction() {
        String anHourFromNow =
                Instant.now()
                        .atOffset(ZoneOffset.UTC)
                        .plusHours(1)
                        .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        String anHourBefore =
                Instant.now()
                        .atOffset(ZoneOffset.UTC)
                        .minusHours(1)
                        .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        String sqlFilter =
                "id = 0"
                        + " AND NOT optString IS NULL"
                        + " AND optString LIKE 's%'"
                        + " AND optDouble > -1"
                        + " AND optDouble <= 1.0 "
                        + " AND ts BETWEEN '"
                        + anHourBefore
                        + "' AND '"
                        + anHourFromNow
                        + "' ";
        tEnv.executeSql(createTestDDl(null));

        Iterator<Row> collected =
                tEnv.executeSql(
                                "SELECT id, optDouble, optString "
                                        + "FROM bigquery_source "
                                        + "WHERE "
                                        + sqlFilter
                                        + "LIMIT 1")
                        .collect();
        List<String> result =
                CollectionUtil.iteratorToList(collected).stream()
                        .map(Row::toString)
                        .sorted()
                        .collect(Collectors.toList());

        List<String> expected = Stream.of("+I[0, 0.0, s0]").sorted().collect(Collectors.toList());

        Assertions.assertThat(result).isEqualTo(expected);
    }

    private static String createTestDDl(Map<String, String> extraOptions) {
        Map<String, String> options = new HashMap<>();
        options.put(FactoryUtil.CONNECTOR.key(), "bigquery");
        options.put(BigQueryConnectorOptions.PROJECT.key(), "project");
        options.put(BigQueryConnectorOptions.DATASET.key(), "dataset");
        options.put(BigQueryConnectorOptions.TABLE.key(), "table");
        options.put(BigQueryConnectorOptions.TEST_MODE.key(), "true");
        if (extraOptions != null) {
            options.putAll(extraOptions);
        }

        String optionString =
                options.entrySet().stream()
                        .map(e -> String.format("'%s' = '%s'", e.getKey(), e.getValue()))
                        .collect(Collectors.joining(",\n"));

        return String.join(
                "\n",
                Arrays.asList(
                        "CREATE TABLE bigquery_source",
                        "(",
                        "  id INTEGER,",
                        "  optDouble DOUBLE,",
                        "  optString VARCHAR,",
                        "  ts TIMESTAMP,",
                        "  reqSubRecord ROW<reqBoolean BOOLEAN, reqTs TIMESTAMP>,",
                        "  optArraySubRecords ARRAY<ROW<reqLong BIGINT, optBytes BINARY>>",
                        ") PARTITIONED BY (ts) WITH (",
                        optionString,
                        ")"));
    }
}
