/*
 * Copyright 2025 The Apache Software Foundation.
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

package com.google.cloud.flink.bigquery.sink.client;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpResponseException;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.Table;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;
import com.google.cloud.flink.bigquery.common.exceptions.BigQueryConnectorException;
import com.google.cloud.flink.bigquery.fakes.StorageClientFaker;
import com.google.cloud.flink.bigquery.services.BigQueryUtils;
import com.google.cloud.flink.bigquery.sink.writer.CreateTableOptions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Arrays;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.when;

/** Tests for {@link BigQueryClientWithErrorHandling}. */
public class BigQueryClientWithErrorHandlingTest {

    BigQueryException mockedException;

    @Before
    public void setUp() {
        mockedException = Mockito.mock(BigQueryException.class);
    }

    @After
    public void tearDown() {
        mockedException = null;
    }

    @Test
    public void testTableExistsError() {
        BigQueryConnectOptions options =
                StorageClientFaker.createConnectOptionsForQuery(
                        false, Mockito.mock(BigQueryException.class), null, null);
        BigQueryConnectorException exception =
                assertThrows(
                        BigQueryConnectorException.class,
                        () -> BigQueryClientWithErrorHandling.tableExists(options));
        assertThat(exception)
                .hasMessageThat()
                .contains("Unable to check existence of BigQuery table");
    }

    @Test
    public void testCreateDataset_withBigQueryException() {
        when(mockedException.getCode()).thenReturn(400);
        BigQueryConnectOptions options =
                StorageClientFaker.createConnectOptionsForQuery(false, null, mockedException, null);
        BigQueryConnectorException exception =
                assertThrows(
                        BigQueryConnectorException.class,
                        () -> BigQueryClientWithErrorHandling.createDataset(options, "foo"));
        assertThat(exception).hasMessageThat().contains("Unable to create BigQuery dataset");
    }

    @Test
    public void testCreateDataset_ignoreAlreadyExistsError() {
        when(mockedException.getCode()).thenReturn(409);
        BigQueryConnectOptions options =
                StorageClientFaker.createConnectOptionsForQuery(false, null, mockedException, null);
        BigQueryClientWithErrorHandling.createDataset(options, "foo");
    }

    @Test
    public void testCreateTable_withBigQueryException() {
        when(mockedException.getCode()).thenReturn(400);
        BigQueryConnectOptions options =
                StorageClientFaker.createConnectOptionsForQuery(false, null, null, mockedException);
        BigQueryConnectorException exception =
                assertThrows(
                        BigQueryConnectorException.class,
                        () ->
                                BigQueryClientWithErrorHandling.createTable(
                                        options,
                                        null,
                                        new CreateTableOptions(
                                                false, null, null, null, null, null, false, null,
                                                null)));
        assertThat(exception).hasMessageThat().contains("Unable to create BigQuery table");
    }

    @Test
    public void testCreateTable_ignoreAlreadyExistsError() {
        when(mockedException.getCode()).thenReturn(409);
        BigQueryConnectOptions options =
                StorageClientFaker.createConnectOptionsForQuery(false, null, null, mockedException);
        BigQueryClientWithErrorHandling.createTable(
                options,
                null,
                new CreateTableOptions(false, null, null, null, null, null, false, null, null));
    }

    @Test
    public void testCreateCdcTable_ignoreAlreadyExistsError() throws Exception {
        try (MockedStatic<BigQueryUtils> utils = mockCdcTableInsertFailingWith(409)) {
            BigQueryClientWithErrorHandling.createTable(
                    cdcConnectOptions(), cdcTableDefinition(), cdcCreateTableOptions());
        }
    }

    @Test
    public void testCreateCdcTable_withGoogleJsonResponseException() throws Exception {
        try (MockedStatic<BigQueryUtils> utils = mockCdcTableInsertFailingWith(403)) {
            BigQueryConnectorException exception =
                    assertThrows(
                            BigQueryConnectorException.class,
                            () ->
                                    BigQueryClientWithErrorHandling.createTable(
                                            cdcConnectOptions(),
                                            cdcTableDefinition(),
                                            cdcCreateTableOptions()));
            assertThat(exception).hasMessageThat().contains("Unable to create BigQuery table");
            assertThat(exception).hasCauseThat().isInstanceOf(BigQueryException.class);
        }
    }

    private static MockedStatic<BigQueryUtils> mockCdcTableInsertFailingWith(int statusCode)
            throws IOException {
        Bigquery.Builder builder = Mockito.mock(Bigquery.Builder.class);
        Bigquery bigquery = Mockito.mock(Bigquery.class);
        Bigquery.Tables tables = Mockito.mock(Bigquery.Tables.class);
        Bigquery.Tables.Insert insert = Mockito.mock(Bigquery.Tables.Insert.class);
        when(builder.build()).thenReturn(bigquery);
        when(bigquery.tables()).thenReturn(tables);
        when(tables.insert(Mockito.anyString(), Mockito.anyString(), Mockito.any(Table.class)))
                .thenReturn(insert);
        when(insert.setPrettyPrint(Mockito.anyBoolean())).thenReturn(insert);
        when(insert.execute())
                .thenThrow(
                        new GoogleJsonResponseException(
                                new HttpResponseException.Builder(
                                        statusCode, "error", new HttpHeaders()),
                                null));
        MockedStatic<BigQueryUtils> utils = Mockito.mockStatic(BigQueryUtils.class);
        utils.when(() -> BigQueryUtils.newBigqueryBuilder(Mockito.any())).thenReturn(builder);
        return utils;
    }

    private static BigQueryConnectOptions cdcConnectOptions() {
        return BigQueryConnectOptions.builder()
                .setProjectId("project")
                .setDataset("dataset")
                .setTable("table")
                .build();
    }

    private static StandardTableDefinition cdcTableDefinition() {
        return StandardTableDefinition.newBuilder()
                .setSchema(Schema.of(Field.of("shop_id", StandardSQLTypeName.INT64)))
                .build();
    }

    private static CreateTableOptions cdcCreateTableOptions() {
        return new CreateTableOptions(
                true,
                null,
                null,
                null,
                null,
                null,
                true,
                Arrays.asList("shop_id"),
                "INTERVAL 10 MINUTE");
    }

    @Test
    public void testToCdcTable() {
        BigQueryConnectOptions options =
                BigQueryConnectOptions.builder()
                        .setProjectId("project")
                        .setDataset("dataset")
                        .setTable("table")
                        .build();
        StandardTableDefinition tableDefinition =
                StandardTableDefinition.newBuilder()
                        .setSchema(Schema.of(Field.of("shop_id", StandardSQLTypeName.INT64)))
                        .build();

        Table table =
                BigQueryClientWithErrorHandling.toCdcTable(
                        options,
                        tableDefinition,
                        new CreateTableOptions(
                                true,
                                null,
                                null,
                                null,
                                null,
                                null,
                                true,
                                Arrays.asList("shop_id"),
                                "INTERVAL 10 MINUTE"));

        assertThat(table.getTableReference().getProjectId()).isEqualTo("project");
        assertThat(table.getTableConstraints().getPrimaryKey().getColumns())
                .containsExactly("shop_id");
        assertThat(table.getMaxStaleness()).isEqualTo("INTERVAL 10 MINUTE");
    }
}
