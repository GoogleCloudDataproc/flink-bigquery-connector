package com.google.cloud.flink.bigquery.source.split;

import com.google.cloud.bigquery.storage.v1.DataFormat;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.cloud.bigquery.storage.v1.ReadStream;
import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;
import com.google.cloud.flink.bigquery.common.config.CredentialsOptions;
import com.google.cloud.flink.bigquery.services.BigQueryServices;
import com.google.cloud.flink.bigquery.services.BigQueryServicesFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static com.google.common.truth.Truth.assertThat;

/** Tests for {@link SplitDiscoverer}. */
public class SplitDiscovererTest {

    private BigQueryConnectOptions options;
    private FakeBigQueryServices fakeServices;

    @Before
    public void setUp() {
        fakeServices = new FakeBigQueryServices();
        options =
                BigQueryConnectOptions.builder()
                        .setProjectId("project")
                        .setDataset("dataset")
                        .setTable("table")
                        .setTestingBigQueryServices(() -> fakeServices)
                        .build();
    }

    @After
    public void tearDown() {
        BigQueryServicesFactory.instance(options).defaultImplementation();
    }

    @Test
    public void testDiscoverSplitsRegularTable() {
        fakeServices.isView = false;
        fakeServices.readSession =
                ReadSession.newBuilder()
                        .addStreams(ReadStream.newBuilder().setName("stream1").build())
                        .build();

        List<String> splits =
                SplitDiscoverer.discoverSplits(
                        options,
                        DataFormat.AVRO,
                        Collections.emptyList(),
                        null,
                        Optional.empty(),
                        null,
                        null);

        assertThat(splits).containsExactly("stream1");
        assertThat(fakeServices.materializeViewCalled).isFalse();
    }

    @Test
    public void testDiscoverSplitsView() {
        fakeServices.isView = true;
        fakeServices.materializedTableName = "temp_table";
        fakeServices.readSession =
                ReadSession.newBuilder()
                        .addStreams(ReadStream.newBuilder().setName("stream_temp").build())
                        .build();

        BigQueryConnectOptions optionsWithViews = options.toBuilder().setViewsEnabled(true).build();

        List<String> splits =
                SplitDiscoverer.discoverSplits(
                        optionsWithViews,
                        DataFormat.AVRO,
                        Collections.emptyList(),
                        null,
                        Optional.empty(),
                        null,
                        null);

        assertThat(splits).containsExactly("stream_temp");
        assertThat(fakeServices.materializeViewCalled).isTrue();
        assertThat(fakeServices.lastMaterializedView).isEqualTo("table");
    }

    @Test(expected = IllegalStateException.class)
    public void testDiscoverSplitsViewDisabled() {
        fakeServices.isView = true;
        // options has viewsEnabled = false by default

        SplitDiscoverer.discoverSplits(
                options,
                DataFormat.AVRO,
                Collections.emptyList(),
                null,
                Optional.empty(),
                null,
                null);
    }

    @Test
    public void testDiscoverSplitsViewEmptyColumns() {
        fakeServices.isView = true;
        fakeServices.materializedTableName = "temp_table";
        fakeServices.readSession =
                ReadSession.newBuilder()
                        .addStreams(ReadStream.newBuilder().setName("stream_temp").build())
                        .build();

        BigQueryConnectOptions optionsWithViews = options.toBuilder().setViewsEnabled(true).build();

        List<String> splits =
                SplitDiscoverer.discoverSplits(
                        optionsWithViews,
                        DataFormat.AVRO,
                        Collections.emptyList(),
                        null,
                        Optional.empty(),
                        null,
                        null);

        assertThat(splits).containsExactly("stream_temp");
        assertThat(fakeServices.materializeViewCalled).isTrue();
        assertThat(fakeServices.lastSelectedFields).isEmpty();
    }

    @Test
    public void testDiscoverSplitsViewNullRowRestriction() {
        fakeServices.isView = true;
        fakeServices.materializedTableName = "temp_table";
        fakeServices.readSession =
                ReadSession.newBuilder()
                        .addStreams(ReadStream.newBuilder().setName("stream_temp").build())
                        .build();

        BigQueryConnectOptions optionsWithViews = options.toBuilder().setViewsEnabled(true).build();

        List<String> splits =
                SplitDiscoverer.discoverSplits(
                        optionsWithViews,
                        DataFormat.AVRO,
                        Collections.singletonList("col1"),
                        null,
                        Optional.empty(),
                        null,
                        null);

        assertThat(splits).containsExactly("stream_temp");
        assertThat(fakeServices.materializeViewCalled).isTrue();
        assertThat(fakeServices.lastRowRestriction).isNull();
    }

    @Test(expected = RuntimeException.class)
    public void testDiscoverSplitsViewMaterializationFailure() {
        fakeServices.isView = true;
        fakeServices.readSession =
                ReadSession.newBuilder()
                        .addStreams(ReadStream.newBuilder().setName("stream_temp").build())
                        .build();

        BigQueryConnectOptions optionsWithViews = options.toBuilder().setViewsEnabled(true).build();

        fakeServices.materializeViewError = new RuntimeException("Materialization failed");

        SplitDiscoverer.discoverSplits(
                optionsWithViews,
                DataFormat.AVRO,
                Collections.emptyList(),
                null,
                Optional.empty(),
                null,
                null);
    }

    @Test
    public void testDiscoverSplitsViewCustomMaterializationDestination() {
        fakeServices.isView = true;
        fakeServices.materializedTableName = "custom_temp_table";
        fakeServices.readSession =
                ReadSession.newBuilder()
                        .addStreams(ReadStream.newBuilder().setName("stream_temp").build())
                        .build();

        BigQueryConnectOptions optionsWithCustomMat =
                options.toBuilder()
                        .setViewsEnabled(true)
                        .setMaterializationProject("custom-project")
                        .setMaterializationDataset("custom_dataset")
                        .build();

        List<String> splits =
                SplitDiscoverer.discoverSplits(
                        optionsWithCustomMat,
                        DataFormat.AVRO,
                        Collections.emptyList(),
                        null,
                        Optional.empty(),
                        null,
                        null);

        assertThat(splits).containsExactly("stream_temp");
        assertThat(fakeServices.materializeViewCalled).isTrue();
        assertThat(fakeServices.lastMatProject).isEqualTo("custom-project");
        assertThat(fakeServices.lastMatDataset).isEqualTo("custom_dataset");
    }

    @Test
    public void testDiscoverSplitsViewCustomBilling() {
        fakeServices.isView = true;
        fakeServices.materializedTableName = "temp_table";
        fakeServices.readSession =
                ReadSession.newBuilder()
                        .addStreams(ReadStream.newBuilder().setName("stream_temp").build())
                        .build();

        BigQueryConnectOptions optionsWithCustomBilling =
                options.toBuilder()
                        .setViewsEnabled(true)
                        .setBillingProject("billing-project")
                        .build();

        List<String> splits =
                SplitDiscoverer.discoverSplits(
                        optionsWithCustomBilling,
                        DataFormat.AVRO,
                        Collections.emptyList(),
                        null,
                        Optional.empty(),
                        null,
                        null);

        assertThat(splits).containsExactly("stream_temp");
        assertThat(fakeServices.materializeViewCalled).isTrue();
        assertThat(fakeServices.lastBillingProject).isEqualTo("billing-project");
    }

    static class FakeBigQueryServices implements BigQueryServices {
        boolean isView = false;
        boolean materializeViewCalled = false;
        String lastMaterializedView = null;
        String materializedTableName = "default_temp";
        List<String> lastSelectedFields = null;
        String lastRowRestriction = null;
        String lastMatProject = null;
        String lastMatDataset = null;
        String lastBillingProject = null;
        RuntimeException materializeViewError = null;
        ReadSession readSession;

        @Override
        public QueryDataClient createQueryDataClient(CredentialsOptions credentialsOptions) {
            return new QueryDataClient() {
                @Override
                public List<String> retrieveTablePartitions(
                        String project, String dataset, String table) {
                    return Collections.emptyList();
                }

                @Override
                public Optional<com.google.cloud.flink.bigquery.services.TablePartitionInfo>
                        retrievePartitionColumnInfo(String project, String dataset, String table) {
                    return Optional.empty();
                }

                @Override
                public com.google.api.services.bigquery.model.TableSchema getTableSchema(
                        String project, String dataset, String table) {
                    return null;
                }

                @Override
                public com.google.cloud.bigquery.Dataset getDataset(
                        String project, String dataset) {
                    return null;
                }

                @Override
                public void createDataset(String project, String dataset, String region) {}

                @Override
                public Boolean tableExists(
                        String projectName, String datasetName, String tableName) {
                    return true;
                }

                @Override
                public void createTable(
                        String project,
                        String dataset,
                        String table,
                        com.google.cloud.bigquery.TableDefinition tableDefinition) {}

                @Override
                public Boolean isView(String project, String dataset, String table) {
                    return isView;
                }

                @Override
                public String materializeView(
                        String project,
                        String dataset,
                        String table,
                        List<String> selectedFields,
                        String rowRestriction,
                        Integer expirationHours,
                        String materializationProject,
                        String materializationDataset,
                        String billingProject) {
                    materializeViewCalled = true;
                    lastMaterializedView = table;
                    lastSelectedFields = selectedFields;
                    lastRowRestriction = rowRestriction;
                    lastMatProject = materializationProject;
                    lastMatDataset = materializationDataset;
                    lastBillingProject = billingProject;
                    if (materializeViewError != null) {
                        throw materializeViewError;
                    }
                    return materializedTableName;
                }

                @Override
                public com.google.cloud.bigquery.Job submitJob(
                        String project,
                        String jobId,
                        com.google.cloud.bigquery.JobConfiguration jobConfiguration) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public com.google.cloud.bigquery.Job getJob(String project, String jobId) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public com.google.cloud.bigquery.Job waitForJob(com.google.cloud.bigquery.Job job) {
                    throw new UnsupportedOperationException();
                }
            };
        }

        @Override
        public StorageReadClient createStorageReadClient(CredentialsOptions credentialsOptions)
                throws IOException {
            return new StorageReadClient() {
                @Override
                public ReadSession createReadSession(
                        com.google.cloud.bigquery.storage.v1.CreateReadSessionRequest request) {
                    return readSession;
                }

                @Override
                public BigQueryServices.BigQueryServerStream<
                                com.google.cloud.bigquery.storage.v1.ReadRowsResponse>
                        readRows(com.google.cloud.bigquery.storage.v1.ReadRowsRequest request) {
                    return null;
                }

                @Override
                public void close() {}
            };
        }

        @Override
        public StorageWriteClient createStorageWriteClient(CredentialsOptions credentialsOptions)
                throws IOException {
            return null;
        }
    }
}
