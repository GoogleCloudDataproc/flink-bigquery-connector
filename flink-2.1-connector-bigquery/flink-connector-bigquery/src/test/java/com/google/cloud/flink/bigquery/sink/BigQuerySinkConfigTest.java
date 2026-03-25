/*
 * Copyright 2024 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.flink.bigquery.sink;

import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.google.cloud.bigquery.TimePartitioning;
import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;
import com.google.cloud.flink.bigquery.sink.serializer.AvroToProtoSerializer;
import com.google.cloud.flink.bigquery.sink.serializer.BigQueryProtoSerializer;
import com.google.cloud.flink.bigquery.sink.serializer.BigQuerySchemaProvider;
import com.google.cloud.flink.bigquery.sink.serializer.CdcChangeTypeProvider;
import com.google.cloud.flink.bigquery.sink.serializer.TestSchemaProvider;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

import java.time.Duration;
import java.util.Arrays;

import static com.google.cloud.flink.bigquery.RestartStrategyConfigUtils.exponentialDelayRestartStrategyConfig;
import static com.google.cloud.flink.bigquery.RestartStrategyConfigUtils.failureRateRestartStrategyConfig;
import static com.google.cloud.flink.bigquery.RestartStrategyConfigUtils.fixedDelayRestartStrategyConfig;
import static com.google.cloud.flink.bigquery.RestartStrategyConfigUtils.noRestartStrategyConfig;
import static com.google.cloud.flink.bigquery.sink.BigQuerySinkConfig.validateStreamExecutionEnvironment;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** Tests for {@link BigQuerySinkConfig}. */
public class BigQuerySinkConfigTest {

    @Test
    public void testConstructor() throws Exception {
        BigQueryConnectOptions connectOptions =
                BigQueryConnectOptions.builder()
                        .setProjectId("project")
                        .setDataset("dataset")
                        .setTable("table")
                        .build();
        BigQuerySchemaProvider schemaProvider = new TestSchemaProvider(null, null);
        BigQueryProtoSerializer<GenericRecord> serializer = new AvroToProtoSerializer();

        try (StreamExecutionEnvironment env =
                new StreamExecutionEnvironment(
                        fixedDelayRestartStrategyConfig(5, Duration.ofSeconds(5)))) {
            BigQuerySinkConfig<GenericRecord> sinkConfig =
                    BigQuerySinkConfig.<GenericRecord>newBuilder()
                            .connectOptions(connectOptions)
                            .schemaProvider(schemaProvider)
                            .serializer(serializer)
                            .streamExecutionEnvironment(env)
                            .deliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                            .enableTableCreation(true)
                            .partitionType(TimePartitioning.Type.DAY)
                            .partitionField("timestamp")
                            .partitionExpirationMillis(10000000000L)
                            .clusteredFields(Arrays.asList("foo", "bar", "qux"))
                            .region("LaLaLand")
                            .fatalizeSerializer(true)
                            .build();
            assertEquals(connectOptions, sinkConfig.getConnectOptions());
            assertEquals(schemaProvider, sinkConfig.getSchemaProvider());
            assertEquals(serializer, sinkConfig.getSerializer());
            assertEquals(DeliveryGuarantee.EXACTLY_ONCE, sinkConfig.getDeliveryGuarantee());
            assertTrue(sinkConfig.enableTableCreation());
            assertEquals(TimePartitioning.Type.DAY, sinkConfig.getPartitionType());
            assertEquals("timestamp", sinkConfig.getPartitionField());
            assertEquals(10000000000L, sinkConfig.getPartitionExpirationMillis().longValue());
            assertEquals(Arrays.asList("foo", "bar", "qux"), sinkConfig.getClusteredFields());
            assertEquals("LaLaLand", sinkConfig.getRegion());
            assertTrue(sinkConfig.fatalizeSerializer());
        }
    }

    @Test
    public void testConstructor_defaults() throws Exception {
        try (StreamExecutionEnvironment env =
                new StreamExecutionEnvironment(
                        fixedDelayRestartStrategyConfig(5, Duration.ofSeconds(5)))) {
            BigQuerySinkConfig sinkConfig =
                    BigQuerySinkConfig.newBuilder().streamExecutionEnvironment(env).build();
            assertNull(sinkConfig.getConnectOptions());
            assertNull(sinkConfig.getSchemaProvider());
            assertNull(sinkConfig.getSerializer());
            assertNull(sinkConfig.getDeliveryGuarantee());
            assertFalse(sinkConfig.enableTableCreation());
            assertNull(sinkConfig.getPartitionType());
            assertNull(sinkConfig.getPartitionField());
            assertNull(sinkConfig.getPartitionExpirationMillis());
            assertNull(sinkConfig.getClusteredFields());
            assertNull(sinkConfig.getRegion());
            assertFalse(sinkConfig.fatalizeSerializer());
        }
    }

    @Test
    public void testValidation_withFixedDelayRestart_withValidConfiguration() throws Exception {
        try (StreamExecutionEnvironment env =
                new StreamExecutionEnvironment(
                        fixedDelayRestartStrategyConfig(5, Duration.ofSeconds(5)))) {
            validateStreamExecutionEnvironment(env);
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidation_withFixedDelayRestart_withInvalidAttempts() throws Exception {
        try (StreamExecutionEnvironment env =
                new StreamExecutionEnvironment(
                        fixedDelayRestartStrategyConfig(15, Duration.ofSeconds(5)))) {
            validateStreamExecutionEnvironment(env);
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidation_withFixedDelayRestart_withInvalidDelay() throws Exception {
        try (StreamExecutionEnvironment env =
                new StreamExecutionEnvironment(
                        fixedDelayRestartStrategyConfig(5, Duration.ofMillis(500)))) {
            validateStreamExecutionEnvironment(env);
        }
    }

    @Test
    public void testValidation_withExponentialDelayRestart_withValidConfiguration()
            throws Exception {
        try (StreamExecutionEnvironment env =
                new StreamExecutionEnvironment(
                        exponentialDelayRestartStrategyConfig(
                                Duration.ofSeconds(5),
                                Duration.ofMinutes(10),
                                3.0,
                                Duration.ofHours(2),
                                0.0))) {
            validateStreamExecutionEnvironment(env);
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidation_withExponentialDelayRestart_withInvalidInitialBackoff()
            throws Exception {
        try (StreamExecutionEnvironment env =
                new StreamExecutionEnvironment(
                        exponentialDelayRestartStrategyConfig(
                                Duration.ofMillis(500),
                                Duration.ofMinutes(10),
                                3.0,
                                Duration.ofHours(2),
                                0.0))) {
            validateStreamExecutionEnvironment(env);
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidation_withExponentialDelayRestart_withInvalidMaxBackoff()
            throws Exception {
        try (StreamExecutionEnvironment env =
                new StreamExecutionEnvironment(
                        exponentialDelayRestartStrategyConfig(
                                Duration.ofSeconds(5),
                                Duration.ofMinutes(1),
                                3.0,
                                Duration.ofHours(2),
                                0.0))) {
            validateStreamExecutionEnvironment(env);
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidation_withExponentialDelayRestart_withInvalidMultiplier()
            throws Exception {
        try (StreamExecutionEnvironment env =
                new StreamExecutionEnvironment(
                        exponentialDelayRestartStrategyConfig(
                                Duration.ofSeconds(5),
                                Duration.ofMinutes(10),
                                1.5,
                                Duration.ofHours(2),
                                0.0))) {
            validateStreamExecutionEnvironment(env);
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidation_withExponentialDelayRestart_withInvalidResetThreshold()
            throws Exception {
        try (StreamExecutionEnvironment env =
                new StreamExecutionEnvironment(
                        exponentialDelayRestartStrategyConfig(
                                Duration.ofSeconds(5),
                                Duration.ofMinutes(10),
                                3.0,
                                Duration.ofMinutes(30),
                                0.0))) {
            validateStreamExecutionEnvironment(env);
        }
    }

    @Test
    public void testValidation_withFailureRateRestart_withValidConfiguration() throws Exception {
        try (StreamExecutionEnvironment env =
                new StreamExecutionEnvironment(
                        failureRateRestartStrategyConfig(
                                1, Duration.ofMinutes(2), Duration.ofSeconds(5)))) {
            validateStreamExecutionEnvironment(env);
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidation_withFailureRateRestart_withInvalidDelay() throws Exception {
        try (StreamExecutionEnvironment env =
                new StreamExecutionEnvironment(
                        failureRateRestartStrategyConfig(
                                1, Duration.ofMinutes(2), Duration.ofMillis(500)))) {
            validateStreamExecutionEnvironment(env);
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidation_withFailureRateRestart_withInvalidRate() throws Exception {
        try (StreamExecutionEnvironment env =
                new StreamExecutionEnvironment(
                        failureRateRestartStrategyConfig(
                                2, Duration.ofMinutes(1), Duration.ofSeconds(5)))) {
            validateStreamExecutionEnvironment(env);
        }
    }

    @Test
    public void testValidation_withNoRestart() throws Exception {
        try (StreamExecutionEnvironment env =
                new StreamExecutionEnvironment(noRestartStrategyConfig())) {
            validateStreamExecutionEnvironment(env);
        }
    }

    // ---------- CDC Configuration Tests ----------

    @Test
    public void testCdcConfiguration() throws Exception {
        BigQueryConnectOptions connectOptions =
                BigQueryConnectOptions.builder()
                        .setProjectId("project")
                        .setDataset("dataset")
                        .setTable("table")
                        .build();
        BigQuerySchemaProvider schemaProvider = new TestSchemaProvider(null, null);
        BigQueryProtoSerializer<GenericRecord> serializer = new AvroToProtoSerializer();
        CdcChangeTypeProvider<GenericRecord> changeTypeProvider =
                CdcChangeTypeProvider.upsertOnly();

        try (StreamExecutionEnvironment env =
                new StreamExecutionEnvironment(
                        fixedDelayRestartStrategyConfig(5, Duration.ofSeconds(5)))) {
            BigQuerySinkConfig<GenericRecord> sinkConfig =
                    BigQuerySinkConfig.<GenericRecord>newBuilder()
                            .connectOptions(connectOptions)
                            .schemaProvider(schemaProvider)
                            .serializer(serializer)
                            .streamExecutionEnvironment(env)
                            .deliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                            .enableCdc(true)
                            .cdcSequenceField("timestamp_field")
                            .cdcPrimaryKeyColumns(Arrays.asList("shop_id", "event_date"))
                            .cdcMaxStaleness("INTERVAL 10 MINUTE")
                            .cdcChangeTypeProvider(changeTypeProvider)
                            .build();

            assertTrue(sinkConfig.isCdcEnabled());
            assertEquals("timestamp_field", sinkConfig.getCdcSequenceField());
            assertEquals(
                    Arrays.asList("shop_id", "event_date"), sinkConfig.getCdcPrimaryKeyColumns());
            assertEquals("INTERVAL 10 MINUTE", sinkConfig.getCdcMaxStaleness());
            assertNotNull(sinkConfig.getCdcChangeTypeProvider());
            assertEquals("UPSERT", sinkConfig.getCdcChangeTypeProvider().getChangeType(null));
        }
    }

    @Test
    public void testCdcConfiguration_defaults() throws Exception {
        try (StreamExecutionEnvironment env =
                new StreamExecutionEnvironment(
                        fixedDelayRestartStrategyConfig(5, Duration.ofSeconds(5)))) {
            BigQuerySinkConfig<?> sinkConfig =
                    BigQuerySinkConfig.newBuilder().streamExecutionEnvironment(env).build();

            assertFalse(sinkConfig.isCdcEnabled());
            assertNull(sinkConfig.getCdcSequenceField());
            assertNull(sinkConfig.getCdcPrimaryKeyColumns());
            assertNull(sinkConfig.getCdcMaxStaleness());
            assertNull(sinkConfig.getCdcChangeTypeProvider());
        }
    }

    @Test
    public void testCdcConfiguration_withCustomChangeTypeProvider() throws Exception {
        BigQueryConnectOptions connectOptions =
                BigQueryConnectOptions.builder()
                        .setProjectId("project")
                        .setDataset("dataset")
                        .setTable("table")
                        .build();
        BigQuerySchemaProvider schemaProvider = new TestSchemaProvider(null, null);
        BigQueryProtoSerializer<String> serializer =
                new BigQueryProtoSerializer<String>() {
                    @Override
                    public void init(BigQuerySchemaProvider schemaProvider) {}

                    @Override
                    public com.google.protobuf.ByteString serialize(String record) {
                        return com.google.protobuf.ByteString.copyFromUtf8(record);
                    }

                    @Override
                    public org.apache.avro.Schema getAvroSchema(String record) {
                        return null;
                    }
                };

        CdcChangeTypeProvider<String> customProvider =
                record -> {
                    if (record != null && record.startsWith("DELETE:")) {
                        return "DELETE";
                    }
                    return "UPSERT";
                };

        try (StreamExecutionEnvironment env =
                new StreamExecutionEnvironment(noRestartStrategyConfig())) {
            BigQuerySinkConfig<String> sinkConfig =
                    BigQuerySinkConfig.<String>newBuilder()
                            .connectOptions(connectOptions)
                            .schemaProvider(schemaProvider)
                            .serializer(serializer)
                            .streamExecutionEnvironment(env)
                            .deliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                            .enableCdc(true)
                            .cdcChangeTypeProvider(customProvider)
                            .build();

            assertTrue(sinkConfig.isCdcEnabled());
            CdcChangeTypeProvider<String> provider =
                    (CdcChangeTypeProvider<String>) sinkConfig.getCdcChangeTypeProvider();
            assertEquals("UPSERT", provider.getChangeType("INSERT:data"));
            assertEquals("DELETE", provider.getChangeType("DELETE:data"));
        }
    }
}
