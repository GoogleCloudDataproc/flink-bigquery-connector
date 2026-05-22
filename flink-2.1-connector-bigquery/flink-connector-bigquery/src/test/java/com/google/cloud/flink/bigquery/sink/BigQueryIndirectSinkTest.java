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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobConfiguration;
import com.google.cloud.bigquery.JobStatus;
import com.google.cloud.bigquery.LoadJobConfiguration;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;
import com.google.cloud.flink.bigquery.common.config.CredentialsOptions;
import com.google.cloud.flink.bigquery.services.BigQueryServices;
import com.google.cloud.flink.bigquery.services.TablePartitionInfo;
import com.google.cloud.flink.bigquery.sink.indirect.RowDataParquetWriterFactory;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests for {@link BigQueryIndirectSink}. */
public class BigQueryIndirectSinkTest {

    private static final String PROJECT = "test-project";
    private static final String DATASET = "test-dataset";
    private static final String TABLE = "test-table";

    private static final RowType ROW_TYPE =
            RowType.of(new BigIntType(), new VarCharType(VarCharType.MAX_LENGTH));

    @Rule public final TemporaryFolder tmp = new TemporaryFolder();

    @Before
    public void resetCapturedState() {
        CapturingQueryDataClient.reset();
    }

    private static String localPath() throws IOException {
        return Files.createTempDirectory("bq-indirect-sink-test").toUri().toString();
    }

    private static BigQueryConnectOptions connectOptions() {
        return BigQueryConnectOptions.builder()
                .setProjectId(PROJECT)
                .setDataset(DATASET)
                .setTable(TABLE)
                .setTestingBigQueryServices(CapturingBigQueryServices::new)
                .build();
    }

    private static BigQuerySinkConfig<String> config(final BigQueryConnectOptions opts)
            throws IOException {
        return BigQuerySinkConfig.<String>newBuilder()
                .writeMode(WriteMode.INDIRECT)
                .connectOptions(opts)
                .tempGcsPath(localPath())
                .bulkWriterFactory(new DummyFactory())
                .formatOptions(FormatOptions.parquet())
                .build();
    }

    @Test
    public void getCommittableSerializerDelegatesToFileSink() throws Exception {
        final BigQueryIndirectSink<String> sink =
                new BigQueryIndirectSink<>(config(connectOptions()));
        final FileSink<String> reference =
                FileSink.forBulkFormat(new Path(localPath()), new DummyFactory()).build();
        assertEquals(
                reference.getCommittableSerializer().getClass(),
                sink.getCommittableSerializer().getClass());
    }

    @Test
    public void getWriterStateSerializerDelegatesToFileSink() throws Exception {
        final BigQueryIndirectSink<String> sink =
                new BigQueryIndirectSink<>(config(connectOptions()));
        final FileSink<String> reference =
                FileSink.forBulkFormat(new Path(localPath()), new DummyFactory()).build();
        assertEquals(
                reference.getWriterStateSerializer().getClass(),
                sink.getWriterStateSerializer().getClass());
    }

    @Test
    public void sinkWriteFilesSubmitsLoadJobCleansUpFiles() throws Exception {
        final File gcsRoot = tmp.newFolder("gcs");

        try (StreamExecutionEnvironment env =
                runFlinkJob(
                        List.of(indirectSink(connectOptions(), UUID.randomUUID(), gcsRoot)),
                        RuntimeExecutionMode.BATCH,
                        1,
                        GenericRowData.of(0L, StringData.fromString("hello")),
                        GenericRowData.of(0L, StringData.fromString("world")))) {
            env.execute("BigQueryIndirectSinkTest");
        }

        assertEquals(
                "expected exactly one load job submission",
                1,
                CapturingQueryDataClient.submittedJobs.size());

        final JobConfiguration submitted = CapturingQueryDataClient.submittedJobs.get(0);
        assertTrue(
                "submitted job must be a LoadJobConfiguration, was " + submitted.getClass(),
                submitted instanceof LoadJobConfiguration);
        final LoadJobConfiguration loadCfg = (LoadJobConfiguration) submitted;
        assertEquals(TableId.of(PROJECT, DATASET, TABLE), loadCfg.getDestinationTable());
        assertEquals(
                "parallelism=1 with a tiny payload should produce one part file",
                1,
                loadCfg.getSourceUris().size());
        final String sourceUri = loadCfg.getSourceUris().get(0);
        assertTrue(
                "source URI should sit under the configured tempGcsPath: " + sourceUri,
                sourceUri.startsWith(gcsRoot.toURI().toString()));

        final File written = new File(URI.create(sourceUri));
        assertFalse(
                "file should be cleaned up after successful load: " + written, written.exists());
    }

    @Test
    public void sinkWriteFilesSubmitsLoadJobCleansUpFilesParallelism2() throws Exception {
        final File gcsRoot = tmp.newFolder("gcs");

        try (StreamExecutionEnvironment env =
                runFlinkJob(
                        List.of(indirectSink(connectOptions(), UUID.randomUUID(), gcsRoot)),
                        RuntimeExecutionMode.BATCH,
                        2,
                        GenericRowData.of(0L, StringData.fromString("a")),
                        GenericRowData.of(1L, StringData.fromString("b")))) {
            env.execute("BigQueryIndirectSinkTest");
        }

        assertEquals(
                "expected exactly one load job submission (post-commit operator is global)",
                1,
                CapturingQueryDataClient.submittedJobs.size());

        final JobConfiguration submitted = CapturingQueryDataClient.submittedJobs.get(0);
        assertTrue(
                "submitted job must be a LoadJobConfiguration, was " + submitted.getClass(),
                submitted instanceof LoadJobConfiguration);
        final LoadJobConfiguration loadCfg = (LoadJobConfiguration) submitted;
        assertEquals(TableId.of(PROJECT, DATASET, TABLE), loadCfg.getDestinationTable());
        assertEquals(
                "parallelism=2 with keys hitting both subtasks should produce two part files",
                2,
                loadCfg.getSourceUris().size());
        for (final String sourceUri : loadCfg.getSourceUris()) {
            assertTrue(
                    "source URI should sit under the configured tempGcsPath: " + sourceUri,
                    sourceUri.startsWith(gcsRoot.toURI().toString()));
            final File written = new File(URI.create(sourceUri));
            assertFalse(
                    "file should be cleaned up after successful load: " + written,
                    written.exists());
        }
    }

    @Test
    public void sinkRejectsStreamingMode() throws Exception {
        final File gcsRoot = tmp.newFolder("gcs");
        try (StreamExecutionEnvironment env =
                runFlinkJob(
                        List.of(indirectSink(connectOptions(), UUID.randomUUID(), gcsRoot)),
                        RuntimeExecutionMode.STREAMING,
                        1,
                        GenericRowData.of(0L, StringData.fromString("hello")))) {

            assertThatThrownBy(() -> env.execute("BigQueryIndirectSinkTest"))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("INDIRECT write mode is only supported in BATCH");
        }
    }

    @Test
    public void twoSinksToSameTableSubmitDistinctLoadJobs() throws Exception {
        final File gcsRoot = tmp.newFolder("gcs");
        final UUID uuid1 = UUID.fromString("11111111-1111-1111-1111-111111111111");
        final UUID uuid2 = UUID.fromString("22222222-2222-2222-2222-222222222222");

        final JobID flinkJobId;
        try (StreamExecutionEnvironment env =
                runFlinkJob(
                        List.of(
                                indirectSink(connectOptions(), uuid1, gcsRoot),
                                indirectSink(connectOptions(), uuid2, gcsRoot)),
                        RuntimeExecutionMode.BATCH,
                        1,
                        GenericRowData.of(0L, StringData.fromString("hello")))) {
            flinkJobId = env.execute("BigQueryIndirectSinkTest").getJobID();
        }

        // Two sinks share the Flink job ID and the (single, batch-mode) checkpoint ID, so the
        // load job IDs differ only at the per-sink UUID component.
        assertThat(CapturingQueryDataClient.submittedJobIds)
                .containsExactlyInAnyOrder(
                        "flink-bq-load_" + flinkJobId.toHexString() + "_" + uuid1 + "_c1",
                        "flink-bq-load_" + flinkJobId.toHexString() + "_" + uuid2 + "_c1");

        final TableId expectedTable = TableId.of(PROJECT, DATASET, TABLE);
        final List<TableId> destinationTables =
                CapturingQueryDataClient.submittedJobs.stream()
                        .map(c -> ((LoadJobConfiguration) c).getDestinationTable())
                        .collect(Collectors.toList());
        assertThat(destinationTables).containsExactly(expectedTable, expectedTable);
    }

    @Test
    public void twoSinksToDifferentTablesSubmitDistinctLoadJobs() throws Exception {
        final File gcsRoot = tmp.newFolder("gcs");
        final UUID uuid1 = UUID.fromString("11111111-1111-1111-1111-111111111111");
        final UUID uuid2 = UUID.fromString("22222222-2222-2222-2222-222222222222");
        final String otherTable = "other-table";
        final BigQueryConnectOptions opts1 = connectOptions();
        final BigQueryConnectOptions opts2 = opts1.toBuilder().setTable(otherTable).build();

        final JobID flinkJobId;
        try (StreamExecutionEnvironment env =
                runFlinkJob(
                        List.of(
                                indirectSink(opts1, uuid1, gcsRoot),
                                indirectSink(opts2, uuid2, gcsRoot)),
                        RuntimeExecutionMode.BATCH,
                        1,
                        GenericRowData.of(0L, StringData.fromString("hello")))) {
            flinkJobId = env.execute("BigQueryIndirectSinkTest").getJobID();
        }

        // Destination table is not part of the load job ID, so even with different tables the
        // two job IDs differ only at the per-sink UUID component.
        assertThat(CapturingQueryDataClient.submittedJobIds)
                .containsExactlyInAnyOrder(
                        "flink-bq-load_" + flinkJobId.toHexString() + "_" + uuid1 + "_c1",
                        "flink-bq-load_" + flinkJobId.toHexString() + "_" + uuid2 + "_c1");

        final List<TableId> destinationTables =
                CapturingQueryDataClient.submittedJobs.stream()
                        .map(c -> ((LoadJobConfiguration) c).getDestinationTable())
                        .collect(Collectors.toList());
        assertThat(destinationTables)
                .containsExactlyInAnyOrder(
                        TableId.of(PROJECT, DATASET, TABLE),
                        TableId.of(PROJECT, DATASET, otherTable));
    }

    private static BigQueryIndirectSink<RowData> indirectSink(
            final BigQueryConnectOptions opts, final UUID uuid, final File gcsRoot) {
        final BigQuerySinkConfig<RowData> conf =
                BigQuerySinkConfig.<RowData>newBuilder()
                        .writeMode(WriteMode.INDIRECT)
                        .connectOptions(opts)
                        .tempGcsPath(gcsRoot.toURI().toString())
                        .bulkWriterFactory(RowDataParquetWriterFactory.create(ROW_TYPE))
                        .formatOptions(FormatOptions.parquet())
                        .build();
        return new BigQueryIndirectSink<>(conf, uuid);
    }

    private static StreamExecutionEnvironment runFlinkJob(
            final List<BigQueryIndirectSink<RowData>> sinks,
            final RuntimeExecutionMode mode,
            final int parallelism,
            final RowData... rows) {
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironment(parallelism, new Configuration());
        env.setRuntimeMode(mode);

        final TypeInformation<RowData> rowTypeInfo = InternalTypeInfo.of(ROW_TYPE);
        final DataStream<RowData> partitioned =
                env.fromData(rowTypeInfo, rows)
                        // The bigint column IS the target subtask - pick keys in [0, parallelism)
                        .partitionCustom(
                                (Partitioner<Long>) (key, parts) -> key.intValue(),
                                (KeySelector<RowData, Long>) row -> row.getLong(0));

        for (final BigQueryIndirectSink<RowData> sink : sinks) {
            partitioned.sinkTo(sink).setParallelism(parallelism);
        }

        return env;
    }

    /** Minimal serializable BulkWriter.Factory - no-op, just exists to satisfy the builder. */
    private static final class DummyFactory implements BulkWriter.Factory<String>, Serializable {
        private static final long serialVersionUID = 1L;

        @Override
        public BulkWriter<String> create(final FSDataOutputStream out) throws IOException {
            throw new UnsupportedOperationException("not used in test");
        }
    }

    /**
     * Fake {@link BigQueryServices} that returns a {@link CapturingQueryDataClient}. Storage
     * clients are not used by the indirect-writes post-commit topology, so they throw.
     */
    public static class CapturingBigQueryServices implements BigQueryServices, Serializable {
        private static final long serialVersionUID = 1L;

        @Override
        public QueryDataClient createQueryDataClient(final CredentialsOptions credentialsOptions) {
            return new CapturingQueryDataClient();
        }

        @Override
        public StorageReadClient createStorageReadClient(
                final CredentialsOptions credentialsOptions) {
            throw new UnsupportedOperationException("not used in indirect-writes post-commit");
        }

        @Override
        public StorageWriteClient createStorageWriteClient(
                final CredentialsOptions credentialsOptions) {
            throw new UnsupportedOperationException("not used in indirect-writes post-commit");
        }
    }

    /**
     * Captures load job submissions in static state - the operator runs in the same JVM as this
     * test (local mini-cluster), so the static fields are shared across the supplier-created
     * instances and the test assertions.
     */
    public static class CapturingQueryDataClient
            implements BigQueryServices.QueryDataClient, Serializable {
        private static final long serialVersionUID = 1L;

        // Synchronized - two sinks in one Flink job submit jobs from independent operator
        // threads in the local mini-cluster.
        private static final List<JobConfiguration> submittedJobs =
                Collections.synchronizedList(new ArrayList<>());
        private static final List<String> submittedJobIds =
                Collections.synchronizedList(new ArrayList<>());

        static void reset() {
            submittedJobs.clear();
            submittedJobIds.clear();
        }

        @Override
        public Job submitJob(
                final String project, final String jobId, final JobConfiguration jobConfiguration) {
            submittedJobs.add(jobConfiguration);
            submittedJobIds.add(jobId);
            return successJob();
        }

        @Override
        public Job getJob(final String project, final String jobId) {
            return null;
        }

        @Override
        public Job waitForJob(final Job job) {
            return job;
        }

        @Override
        public List<String> retrieveTablePartitions(
                final String p, final String d, final String t) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<TablePartitionInfo> retrievePartitionColumnInfo(
                final String p, final String d, final String t) {
            throw new UnsupportedOperationException();
        }

        @Override
        public TableSchema getTableSchema(final String p, final String d, final String t) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Dataset getDataset(final String p, final String d) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void createDataset(final String p, final String d, final String r) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Boolean tableExists(final String p, final String d, final String t) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void createTable(
                final String p, final String d, final String t, final TableDefinition td) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Boolean isView(final String p, final String d, final String t) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String materializeView(
                final String p,
                final String d,
                final String t,
                final List<String> selectedFields,
                final String rowRestriction,
                final Integer expirationHours,
                final String materializationProject,
                final String materializationDataset,
                final String billingProject) {
            throw new UnsupportedOperationException();
        }
    }

    private static Job successJob() {
        final Job job = mock(Job.class);
        final JobStatus status = mock(JobStatus.class);
        when(status.getError()).thenReturn(null);
        when(job.getStatus()).thenReturn(status);
        return job;
    }
}
