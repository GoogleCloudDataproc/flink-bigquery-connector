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
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.Clustering;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableConstraints;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;
import com.google.cloud.flink.bigquery.common.exceptions.BigQueryConnectorException;
import com.google.cloud.flink.bigquery.common.utils.SchemaTransform;
import com.google.cloud.flink.bigquery.services.BigQueryServices;
import com.google.cloud.flink.bigquery.services.BigQueryServicesFactory;
import com.google.cloud.flink.bigquery.services.BigQueryUtils;
import com.google.cloud.flink.bigquery.sink.writer.CreateTableOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;

import static com.google.cloud.flink.bigquery.services.BigQueryServicesImpl.ALREADY_EXISTS_ERROR_CODE;

/** Wrapper around {@link BigQueryServices} with sink specific error handling. */
public class BigQueryClientWithErrorHandling {

    private static final Logger LOG =
            LoggerFactory.getLogger(BigQueryClientWithErrorHandling.class);

    private BigQueryClientWithErrorHandling() {}

    public static boolean tableExists(BigQueryConnectOptions connectOptions)
            throws BigQueryConnectorException {
        try {
            BigQueryServices.QueryDataClient queryDataClient =
                    BigQueryServicesFactory.instance(connectOptions).queryClient();
            return queryDataClient.tableExists(
                    connectOptions.getProjectId(),
                    connectOptions.getDataset(),
                    connectOptions.getTable());
        } catch (Exception e) {
            throw new BigQueryConnectorException(
                    String.format(
                            "Unable to check existence of BigQuery table %s.%s.%s",
                            connectOptions.getProjectId(),
                            connectOptions.getDataset(),
                            connectOptions.getTable()),
                    e);
        }
    }

    public static TableSchema getTableSchema(BigQueryConnectOptions connectOptions)
            throws BigQueryConnectorException {
        try {
            BigQueryServices.QueryDataClient queryDataClient =
                    BigQueryServicesFactory.instance(connectOptions).queryClient();
            return queryDataClient.getTableSchema(
                    connectOptions.getProjectId(),
                    connectOptions.getDataset(),
                    connectOptions.getTable());
        } catch (Exception e) {
            throw new BigQueryConnectorException(
                    String.format(
                            "Unable to get schema of BigQuery table %s.%s.%s",
                            connectOptions.getProjectId(),
                            connectOptions.getDataset(),
                            connectOptions.getTable()),
                    e);
        }
    }

    public static void createDataset(BigQueryConnectOptions connectOptions, String region) {
        try {
            BigQueryServices.QueryDataClient queryDataClient =
                    BigQueryServicesFactory.instance(connectOptions).queryClient();
            queryDataClient.createDataset(
                    connectOptions.getProjectId(), connectOptions.getDataset(), region);
            LOG.info(
                    "Created BigQuery dataset {}.{}",
                    connectOptions.getProjectId(),
                    connectOptions.getDataset());
        } catch (BigQueryException e) {
            if (e.getCode() == ALREADY_EXISTS_ERROR_CODE) {
                LOG.warn(
                        "Attempted creation of BigQuery dataset {}.{} failed, since it already exists",
                        connectOptions.getProjectId(),
                        connectOptions.getDataset());
                return;
            }
            throw new BigQueryConnectorException(
                    String.format(
                            "Unable to create BigQuery dataset %s.%s",
                            connectOptions.getProjectId(), connectOptions.getDataset()),
                    e);
        }
    }

    public static void createTable(
            BigQueryConnectOptions connectOptions,
            TableDefinition tableDefinition,
            CreateTableOptions createTableOptions) {
        try {
            if (createTableOptions.isCdcEnabled()) {
                Bigquery bigquery =
                        BigQueryUtils.newBigqueryBuilder(connectOptions.getCredentialsOptions())
                                .build();
                bigquery.tables()
                        .insert(
                                connectOptions.getProjectId(),
                                connectOptions.getDataset(),
                                toCdcTable(connectOptions, tableDefinition, createTableOptions))
                        .setPrettyPrint(false)
                        .execute();
            } else {
                BigQueryServices.QueryDataClient queryDataClient =
                        BigQueryServicesFactory.instance(connectOptions).queryClient();
                queryDataClient.createTable(
                        connectOptions.getProjectId(),
                        connectOptions.getDataset(),
                        connectOptions.getTable(),
                        tableDefinition);
            }
            LOG.info(
                    "Created BigQuery table {}.{}.{}",
                    connectOptions.getProjectId(),
                    connectOptions.getDataset(),
                    connectOptions.getTable());
        } catch (GoogleJsonResponseException e) {
            throw new BigQueryException(e.getStatusCode(), e.getMessage(), e);
        } catch (IOException e) {
            throw new BigQueryException(0, e.getMessage(), e);
        } catch (BigQueryException e) {
            if (e.getCode() == ALREADY_EXISTS_ERROR_CODE) {
                LOG.warn(
                        "Attempted creation of BigQuery table {}.{}.{} failed, since it already exists",
                        connectOptions.getProjectId(),
                        connectOptions.getDataset(),
                        connectOptions.getTable());
                return;
            }
            throw new BigQueryConnectorException(
                    String.format(
                            "Unable to create BigQuery table %s.%s.%s",
                            connectOptions.getProjectId(),
                            connectOptions.getDataset(),
                            connectOptions.getTable()),
                    e);
        }
    }

    static Table toCdcTable(
            BigQueryConnectOptions connectOptions,
            TableDefinition tableDefinition,
            CreateTableOptions createTableOptions) {
        Table table =
                new Table()
                        .setTableReference(
                                new com.google.api.services.bigquery.model.TableReference()
                                        .setProjectId(connectOptions.getProjectId())
                                        .setDatasetId(connectOptions.getDataset())
                                        .setTableId(connectOptions.getTable()))
                        .setSchema(
                                SchemaTransform.bigQuerySchemaToTableSchema(
                                        tableDefinition.getSchema()))
                        .setTableConstraints(
                                new TableConstraints()
                                        .setPrimaryKey(
                                                new TableConstraints.PrimaryKey()
                                                        .setColumns(
                                                                new ArrayList<>(
                                                                        createTableOptions
                                                                                .getCdcPrimaryKeyColumns()))))
                        .setMaxStaleness(createTableOptions.getCdcMaxStaleness());
        if (createTableOptions.getPartitionType() != null) {
            com.google.api.services.bigquery.model.TimePartitioning timePartitioning =
                    new com.google.api.services.bigquery.model.TimePartitioning()
                            .setType(createTableOptions.getPartitionType().name());
            if (createTableOptions.getPartitionField() != null) {
                timePartitioning.setField(createTableOptions.getPartitionField());
            }
            if (createTableOptions.getPartitionExpirationMillis() > 0) {
                timePartitioning.setExpirationMs(createTableOptions.getPartitionExpirationMillis());
            }
            table.setTimePartitioning(timePartitioning);
        }
        if (createTableOptions.getClusteredFields() != null
                && !createTableOptions.getClusteredFields().isEmpty()) {
            table.setClustering(
                    new Clustering().setFields(createTableOptions.getClusteredFields()));
        }
        return table;
    }

    public static Dataset getDataset(BigQueryConnectOptions connectOptions) {
        try {
            BigQueryServices.QueryDataClient queryDataClient =
                    BigQueryServicesFactory.instance(connectOptions).queryClient();
            return queryDataClient.getDataset(
                    connectOptions.getProjectId(), connectOptions.getDataset());
        } catch (BigQueryException e) {
            throw new BigQueryConnectorException(
                    String.format(
                            "Unable to check existence of BigQuery dataset %s.%s",
                            connectOptions.getProjectId(), connectOptions.getDataset()),
                    e);
        }
    }
}
