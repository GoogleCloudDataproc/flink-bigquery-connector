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

package org.apache.flink.connector.bigquery.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.bigquery.source.config.BigQueryReadOptions;
import org.apache.flink.connector.bigquery.table.serde.AvroToRowDataDeserializationSchema;
import org.apache.flink.connector.bigquery.utils.StorageClientMocker;
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

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/** */
@TestInstance(Lifecycle.PER_CLASS)
public class BigQuerySourceITCase {

    private static final int PARALLELISM = 2;
    private static final Integer TOTAL_ROW_COUNT_PER_STREAM = 100000;

    @RegisterExtension
    static final MiniClusterExtension MINI_CLUSTER_RESOURCE =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(PARALLELISM)
                            .build());

    @RegisterExtension final SharedObjectsExtension sharedObjects = SharedObjectsExtension.create();

    private BigQueryReadOptions readOptions;

    @BeforeAll
    public void beforeTest() throws Exception {
        // init the read options for BQ
        readOptions =
                StorageClientMocker.createReadOptions(
                        TOTAL_ROW_COUNT_PER_STREAM,
                        2,
                        StorageClientMocker.SIMPLE_AVRO_SCHEMA_STRING);
    }

    private BigQuerySource.Builder<RowData> defaultSourceBuilder() {
        RowType rowType = defaultSourceRowType();
        TypeInformation<RowData> typeInfo = InternalTypeInfo.of(rowType);

        return BigQuerySource.<RowData>builder()
                .setReadOptions(readOptions)
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
        Assertions.assertThat(results).hasSize(TOTAL_ROW_COUNT_PER_STREAM * 2);
    }

    @Test
    public void testLimit() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final int limitSize = 10;
        BigQuerySource<RowData> bqSource = defaultSourceBuilder().setLimit(limitSize).build();

        List<RowData> results =
                env.fromSource(bqSource, WatermarkStrategy.noWatermarks(), "BigQuery-Source")
                        .executeAndCollect(TOTAL_ROW_COUNT_PER_STREAM);

        Assertions.assertThat(results).hasSize(limitSize * PARALLELISM);
    }

    @Test
    public void testRecovery() throws Exception {
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

        Assertions.assertThat(results).hasSize(TOTAL_ROW_COUNT_PER_STREAM * 2);
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
