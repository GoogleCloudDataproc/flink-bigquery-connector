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

package com.google.cloud.flink.bigquery.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.testutils.junit.SharedObjectsExtension;
import org.apache.flink.testutils.junit.SharedReference;
import org.apache.flink.util.CollectionUtil;

import com.google.cloud.flink.bigquery.fakes.StorageClientFaker;
import com.google.cloud.flink.bigquery.source.config.BigQueryReadOptions;
import com.google.cloud.flink.bigquery.source.reader.deserializer.AvroToRowDataDeserializationSchema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.truth.Truth.assertThat;

/** */
@TestInstance(Lifecycle.PER_CLASS)
public class BigQuerySourceIntegrationTestCase {

    private static final int PARALLELISM = 2;
    private static final Integer TOTAL_ROW_COUNT_PER_STREAM = 10000;
    private static final Integer STREAM_COUNT = 2;

    @RegisterExtension
    static final MiniClusterExtension MINI_CLUSTER_RESOURCE =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(PARALLELISM)
                            .build());

    @RegisterExtension final SharedObjectsExtension sharedObjects = SharedObjectsExtension.create();

    private BigQuerySource.Builder<RowData> defaultSourceBuilder() throws IOException {
        return defaultSourceBuilder(
                StorageClientFaker.createReadOptions(
                        TOTAL_ROW_COUNT_PER_STREAM,
                        STREAM_COUNT,
                        StorageClientFaker.SIMPLE_AVRO_SCHEMA_STRING));
    }

    private BigQuerySource.Builder<RowData> defaultSourceBuilder(int limit) throws IOException {
        return defaultSourceBuilder(
                StorageClientFaker.createReadOptions(
                        TOTAL_ROW_COUNT_PER_STREAM,
                        STREAM_COUNT,
                        StorageClientFaker.SIMPLE_AVRO_SCHEMA_STRING,
                        limit));
    }

    private BigQuerySource.Builder<RowData> defaultSourceBuilder(BigQueryReadOptions rOptions) {
        RowType rowType = defaultSourceRowType();
        TypeInformation<RowData> typeInfo = InternalTypeInfo.of(rowType);

        return BigQuerySource.<RowData>builder()
                .setReadOptions(rOptions)
                .setDeserializationSchema(
                        new AvroToRowDataDeserializationSchema(rowType, typeInfo));
    }

    private static RowType defaultSourceRowType() {
        ResolvedSchema schema =
                ResolvedSchema.of(
                        Column.physical("name", DataTypes.STRING()),
                        Column.physical("number", DataTypes.BIGINT()));
        return (RowType) schema.toPhysicalRowDataType().getLogicalType();
    }

    @Test
    public void testReadCount() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        BigQuerySource<RowData> bqSource = defaultSourceBuilder().build();

        List<RowData> results =
                CollectionUtil.iteratorToList(
                        env.fromSource(
                                        bqSource,
                                        WatermarkStrategy.noWatermarks(),
                                        "BigQuery-Source")
                                .executeAndCollect());

        // we only create 2 streams as response
        assertThat(results).hasSize(TOTAL_ROW_COUNT_PER_STREAM * STREAM_COUNT);
    }

    @Test
    public void testLimit() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final int limitSize = 10;
        BigQuerySource<RowData> bqSource = defaultSourceBuilder(limitSize).build();

        List<RowData> results =
                env.fromSource(bqSource, WatermarkStrategy.noWatermarks(), "BigQuery-Source")
                        .executeAndCollect(TOTAL_ROW_COUNT_PER_STREAM);
        // need to check on parallelism since the limit is triggered per task + reader contexts = 2
        assertThat(results).hasSize(limitSize * PARALLELISM);
    }

    @Test
    public void testDownstreamRecovery() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(300L);

        BigQuerySource<RowData> bqSource = defaultSourceBuilder().build();
        final SharedReference<AtomicBoolean> failed = sharedObjects.add(new AtomicBoolean(false));

        List<RowData> results =
                CollectionUtil.iteratorToList(
                        env.fromSource(
                                        bqSource,
                                        WatermarkStrategy.noWatermarks(),
                                        "BigQuery-Source")
                                .map(new FailingMapper(failed))
                                .executeAndCollect());

        assertThat(results).hasSize(TOTAL_ROW_COUNT_PER_STREAM * STREAM_COUNT);
    }

    @Test
    public void testReaderRecovery() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(300L);

        ResolvedSchema schema =
                ResolvedSchema.of(
                        Column.physical("name", DataTypes.STRING()),
                        Column.physical("number", DataTypes.BIGINT()));
        RowType rowType = (RowType) schema.toPhysicalRowDataType().getLogicalType();

        TypeInformation<RowData> typeInfo = InternalTypeInfo.of(rowType);

        BigQuerySource<RowData> bqSource =
                BigQuerySource.<RowData>builder()
                        .setReadOptions(
                                StorageClientFaker.createReadOptions(
                                        // just put more rows JIC
                                        TOTAL_ROW_COUNT_PER_STREAM,
                                        STREAM_COUNT,
                                        StorageClientFaker.SIMPLE_AVRO_SCHEMA_STRING,
                                        params -> StorageClientFaker.createRecordList(params),
                                        // we want this to fail 10% of the time (1 in 10 times)
                                        10D))
                        .setDeserializationSchema(
                                new AvroToRowDataDeserializationSchema(rowType, typeInfo))
                        .build();

        List<RowData> results =
                CollectionUtil.iteratorToList(
                        env.fromSource(
                                        bqSource,
                                        WatermarkStrategy.noWatermarks(),
                                        "BigQuery-Source")
                                .executeAndCollect());

        assertThat(results).hasSize(TOTAL_ROW_COUNT_PER_STREAM * STREAM_COUNT);
    }

    private static class FailingMapper
            implements MapFunction<RowData, RowData>, CheckpointListener {

        private final SharedReference<AtomicBoolean> failed;
        private int emittedRecords = 0;

        private FailingMapper(SharedReference<AtomicBoolean> failed) {
            this.failed = failed;
        }

        @Override
        public RowData map(RowData value) {
            emittedRecords++;
            return value;
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) throws Exception {
            if (failed.get().get() || emittedRecords == 0) {
                return;
            }
            failed.get().set(true);
            throw new Exception("Expected failure");
        }
    }
}
