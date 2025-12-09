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

import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;
import com.google.cloud.flink.bigquery.common.exceptions.BigQueryConnectorException;
import com.google.cloud.flink.bigquery.fakes.StorageClientFaker;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

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
                        () -> BigQueryClientWithErrorHandling.createTable(options, null));
        assertThat(exception).hasMessageThat().contains("Unable to create BigQuery table");
    }

    @Test
    public void testCreateTable_ignoreAlreadyExistsError() {
        when(mockedException.getCode()).thenReturn(409);
        BigQueryConnectOptions options =
                StorageClientFaker.createConnectOptionsForQuery(false, null, null, mockedException);
        BigQueryClientWithErrorHandling.createTable(options, null);
    }
}
