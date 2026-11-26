/*
 * Copyright (C) 2026 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.flink.bigquery.sink.serializer;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.flink.bigquery.common.config.CredentialsOptions;
import com.google.cloud.flink.bigquery.services.BigQueryServices;
import com.google.cloud.flink.bigquery.table.config.BigQueryTableConfig;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests credentials used while deriving a table schema. */
public class BigQueryTableSchemaProviderTest {
    @Test
    public void schemaRequestsKeepQuotaProject() throws Exception {
        checkQuotaProject("billing-project");
    }

    @Test
    public void schemaRequestsAllowMissingQuotaProject() throws Exception {
        checkQuotaProject(null);
    }

    private void checkQuotaProject(String quotaProject) throws Exception {
        BigQueryServices services = mock(BigQueryServices.class);
        BigQueryServices.QueryDataClient client = mock(BigQueryServices.QueryDataClient.class);
        when(services.createQueryDataClient(any(CredentialsOptions.class))).thenReturn(client);
        when(client.tableExists("p", "d", "t")).thenReturn(true);
        when(client.getTableSchema("p", "d", "t"))
                .thenReturn(
                        new TableSchema()
                                .setFields(
                                        Collections.singletonList(
                                                new TableFieldSchema()
                                                        .setName("value")
                                                        .setType("STRING"))));
        BigQueryTableConfig config = mock(BigQueryTableConfig.class);
        when(config.getProject()).thenReturn("p");
        when(config.getDataset()).thenReturn("d");
        when(config.getTable()).thenReturn("t");
        when(config.getQuotaProjectId()).thenReturn(quotaProject);
        BigQueryTableSchemaProvider.setTestingServices(() -> services);
        try {
            BigQueryTableSchemaProvider.getTableDescriptor(config);
            ArgumentCaptor<CredentialsOptions> options =
                    ArgumentCaptor.forClass(CredentialsOptions.class);
            verify(services, times(2)).createQueryDataClient(options.capture());
            for (CredentialsOptions value : options.getAllValues()) {
                assertEquals(quotaProject, value.getQuotaProjectId());
            }
        } finally {
            BigQueryTableSchemaProvider.setTestingServices(null);
        }
    }
}
