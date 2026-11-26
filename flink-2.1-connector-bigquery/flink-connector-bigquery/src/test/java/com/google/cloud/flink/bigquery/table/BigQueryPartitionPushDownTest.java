/*
 * Copyright (C) 2026 Google Inc.
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

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.SourceTransformation;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.source.abilities.SupportsPartitionPushDown;
import org.apache.flink.table.factories.utils.FactoryMocks;
import org.apache.flink.table.types.DataType;

import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.flink.bigquery.common.config.CredentialsOptions;
import com.google.cloud.flink.bigquery.fakes.StorageClientFaker;
import com.google.cloud.flink.bigquery.services.BigQueryServices;
import com.google.cloud.flink.bigquery.services.TablePartitionInfo;
import com.google.cloud.flink.bigquery.services.TablePartitionInfo.PartitionType;
import com.google.cloud.flink.bigquery.source.BigQuerySource;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.InterruptedIOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CancellationException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests partition predicates through the Flink planner. */
public class BigQueryPartitionPushDownTest {

    @Test
    public void testTimestampPredicateInsideHourReachesReadRestriction() {
        String restriction =
                planRestriction(
                        StandardSQLTypeName.TIMESTAMP,
                        PartitionType.HOUR,
                        "TIMESTAMP(6)",
                        Arrays.asList("2026010105", "2026010106"),
                        "p > TIMESTAMP '2026-01-01 05:30:00'");
        assertThat(restriction).isEqualTo("(p > '2026-01-01T05:30:00.000000')");
    }

    @Test
    public void testTemporalBucketsUseExactFilters() {
        for (StandardSQLTypeName type :
                Arrays.asList(StandardSQLTypeName.TIMESTAMP, StandardSQLTypeName.DATETIME)) {
            for (PartitionType grain :
                    Arrays.asList(
                            PartitionType.HOUR,
                            PartitionType.DAY,
                            PartitionType.MONTH,
                            PartitionType.YEAR)) {
                assertThat(
                                planRestriction(
                                        type,
                                        grain,
                                        "TIMESTAMP(6)",
                                        Collections.singletonList("2026010105"),
                                        "p > TIMESTAMP '2026-01-01 05:30:00'"))
                        .isEqualTo("(p > '2026-01-01T05:30:00.000000')");
            }
        }
        for (PartitionType grain : Arrays.asList(PartitionType.MONTH, PartitionType.YEAR)) {
            assertThat(
                            planRestriction(
                                    StandardSQLTypeName.DATE,
                                    grain,
                                    "DATE",
                                    Collections.singletonList("202601"),
                                    "p > DATE '2026-01-15'"))
                    .isEqualTo("(p > '2026-01-15')");
        }
    }

    @Test
    public void testIntegerRangeUsesExactFilter() {
        assertThat(
                        planRestriction(
                                StandardSQLTypeName.INT64,
                                PartitionType.INT_RANGE,
                                "BIGINT",
                                Arrays.asList("0", "100"),
                                "p > 50"))
                .isEqualTo("(p > 50)");
    }

    @Test
    public void testUnpartitionedTableUsesFilterWithPartitionedByDdl() {
        assertThat(
                        planRestriction(
                                StandardSQLTypeName.DATE,
                                null,
                                "DATE",
                                Collections.emptyList(),
                                "p > DATE '2026-01-15'"))
                .isEqualTo("(p > '2026-01-15')");
    }

    @Test
    public void testDateDayStillUsesPartitionPushDown() {
        assertThat(
                        planRestriction(
                                StandardSQLTypeName.DATE,
                                PartitionType.DAY,
                                "DATE",
                                Arrays.asList("20260101", "20260102", "__NULL__"),
                                "p = DATE '2026-01-02'"))
                .isEqualTo("(p >= '2026-01-02' AND p < '2026-01-03')");
    }

    @Test
    public void testDateDayNullPartitionStillUsesPartitionPushDown() {
        assertThat(
                        planRestriction(
                                StandardSQLTypeName.DATE,
                                PartitionType.DAY,
                                "DATE",
                                Arrays.asList("20260101", "__NULL__"),
                                "p IS NULL"))
                .isEqualTo("(p IS NULL)");
    }

    @Test
    public void testDateDayMetadataWithTimestampDdlUsesFilter() {
        assertThat(
                        planRestriction(
                                StandardSQLTypeName.DATE,
                                PartitionType.DAY,
                                "TIMESTAMP(6)",
                                Arrays.asList("20260101", "20260102"),
                                "p > TIMESTAMP '2026-01-01 05:30:00'"))
                .isEqualTo("(p > '2026-01-01T05:30:00.000000')");
    }

    @Test
    public void testDateDayWithDifferentOrMultipleDdlPartitionKeysUsesFilter() {
        for (String keys : Arrays.asList("id", "id, p")) {
            assertThat(
                            planRestriction(
                                    StandardSQLTypeName.DATE,
                                    PartitionType.DAY,
                                    "DATE",
                                    Collections.singletonList("20260101"),
                                    "p > DATE '2026-01-15'",
                                    null,
                                    keys))
                    .isEqualTo("(p > '2026-01-15')");
        }
    }

    @Test
    public void testMetadataLookupFailureUsesFilter() {
        assertThat(
                        planRestriction(
                                StandardSQLTypeName.DATE,
                                PartitionType.DAY,
                                "DATE",
                                Collections.emptyList(),
                                "p > DATE '2026-01-15'",
                                new RuntimeException("metadata unavailable")))
                .isEqualTo("(p > '2026-01-15')");
    }

    @Test
    public void testIngestionTimePartitionMetadataFailureUsesFilter() {
        RuntimeException failure =
                new RuntimeException(
                        "Problems while trying to retrieve table partition's column name",
                        new IllegalStateException(
                                "Partition column null does not correlate with a first level column in the schema"));
        assertThat(
                        planRestriction(
                                StandardSQLTypeName.DATE,
                                PartitionType.DAY,
                                "DATE",
                                Collections.emptyList(),
                                "p > DATE '2026-01-15'",
                                failure))
                .isEqualTo("(p > '2026-01-15')");
    }

    @Test
    public void testMetadataLookupInterruptionStillFails() {
        for (Exception cause :
                Arrays.asList(
                        new InterruptedException("stopped"),
                        new InterruptedIOException("stopped"),
                        new CancellationException("stopped"))) {
            assertThatThrownBy(
                            () ->
                                    planRestriction(
                                            StandardSQLTypeName.DATE,
                                            PartitionType.DAY,
                                            "DATE",
                                            Collections.emptyList(),
                                            "p > DATE '2026-01-15'",
                                            new RuntimeException(cause)))
                    .hasStackTraceContaining("stopped");
        }
    }

    @Test
    public void testMetadataLookupErrorStillFails() {
        assertThatThrownBy(
                        () ->
                                planRestriction(
                                        StandardSQLTypeName.DATE,
                                        PartitionType.DAY,
                                        "DATE",
                                        Collections.emptyList(),
                                        "p > DATE '2026-01-15'",
                                        new AssertionError("fatal lookup error")))
                .hasRootCauseInstanceOf(AssertionError.class)
                .hasStackTraceContaining("fatal lookup error");
    }

    private String planRestriction(
            StandardSQLTypeName type,
            PartitionType grain,
            String flinkType,
            List<String> partitions,
            String predicate) {
        return planRestriction(type, grain, flinkType, partitions, predicate, null);
    }

    private String planRestriction(
            StandardSQLTypeName type,
            PartitionType grain,
            String flinkType,
            List<String> partitions,
            String predicate,
            Throwable lookupFailure) {
        return planRestriction(type, grain, flinkType, partitions, predicate, lookupFailure, "p");
    }

    private String planRestriction(
            StandardSQLTypeName type,
            PartitionType grain,
            String flinkType,
            List<String> partitions,
            String predicate,
            Throwable lookupFailure,
            String partitionKeys) {
        StorageClientFaker.FakeBigQueryServices.FakeQueryDataClient client =
                new StorageClientFaker.FakeBigQueryServices.FakeQueryDataClient(
                        true, null, null, null) {
                    @Override
                    public Optional<TablePartitionInfo> retrievePartitionColumnInfo(
                            String project, String dataset, String table) {
                        if (lookupFailure instanceof Error) {
                            throw (Error) lookupFailure;
                        }
                        if (lookupFailure instanceof RuntimeException) {
                            throw (RuntimeException) lookupFailure;
                        }
                        return grain == null
                                ? Optional.empty()
                                : Optional.of(new TablePartitionInfo("p", grain, type, null));
                    }

                    @Override
                    public List<String> retrieveTablePartitions(
                            String project, String dataset, String table) {
                        if (type != StandardSQLTypeName.DATE
                                || grain != PartitionType.DAY
                                || !partitionKeys.equals("p")) {
                            throw new AssertionError("Non-exact partitions must not be listed");
                        }
                        return partitions;
                    }
                };
        BigQueryServices services =
                Mockito.mock(BigQueryServices.class, Mockito.withSettings().serializable());
        Mockito.when(services.createQueryDataClient(Mockito.any(CredentialsOptions.class)))
                .thenReturn(client);
        BigQueryDynamicTableFactory.setTestingServices(() -> services);
        DataType columnType =
                flinkType.equals("DATE")
                        ? DataTypes.DATE()
                        : type == StandardSQLTypeName.INT64
                                ? DataTypes.BIGINT()
                                : DataTypes.TIMESTAMP(6);
        Map<String, String> options = new HashMap<>();
        options.put("connector", "bigquery");
        options.put("project", "project");
        options.put("dataset", "dataset");
        options.put("table", "table");
        options.put("test.enabled", "true");
        BigQueryDynamicTableSource factorySource =
                (BigQueryDynamicTableSource)
                        FactoryMocks.createTableSource(
                                ResolvedSchema.physical(
                                        new String[] {"id", "p"},
                                        new DataType[] {DataTypes.BIGINT(), columnType}),
                                options);
        boolean exact =
                lookupFailure == null
                        && type == StandardSQLTypeName.DATE
                        && grain == PartitionType.DAY
                        && flinkType.equals("DATE");
        assertThat(factorySource instanceof SupportsPartitionPushDown).isEqualTo(exact);
        factorySource.applyLimit(10);
        factorySource.applyProjection(
                new int[][] {{0}}, DataTypes.ROW(DataTypes.FIELD("id", DataTypes.BIGINT())));
        BigQueryDynamicTableSource copy = (BigQueryDynamicTableSource) factorySource.copy();
        assertThat(copy).isEqualTo(factorySource).isNotSameAs(factorySource);
        assertThat(copy instanceof SupportsPartitionPushDown).isEqualTo(exact);
        assertThat(copy.getReadOptions().getLimit()).contains(10);
        assertThat(copy.getReadOptions().getColumnNames()).containsExactly("id");
        factorySource.applyLimit(20);
        assertThat(copy.getReadOptions().getLimit()).contains(10);
        StreamTableEnvironment tableEnv =
                StreamTableEnvironment.create(StreamExecutionEnvironment.getExecutionEnvironment());
        tableEnv.executeSql(
                "CREATE TABLE bq (id BIGINT, p "
                        + flinkType
                        + ") PARTITIONED BY ("
                        + partitionKeys
                        + ") WITH ("
                        + "'connector'='bigquery', 'project'='project', 'dataset'='dataset', 'table'='table', 'test.enabled'='true')");
        Transformation<?> transformation =
                tableEnv.toDataStream(tableEnv.sqlQuery("SELECT id FROM bq WHERE " + predicate))
                        .getTransformation();
        BigQuerySource<?> source =
                transformation.getTransitivePredecessors().stream()
                        .filter(t -> t instanceof SourceTransformation)
                        .map(t -> (SourceTransformation<?, ?, ?>) t)
                        .map(SourceTransformation::getSource)
                        .filter(s -> s instanceof BigQuerySource)
                        .map(s -> (BigQuerySource<?>) s)
                        .findFirst()
                        .orElseThrow(() -> new AssertionError("BigQuery source not found"));
        assertThat(source.getReadOptions().getColumnNames()).containsExactly("id");
        return source.getReadOptions().getRowRestriction();
    }
}
