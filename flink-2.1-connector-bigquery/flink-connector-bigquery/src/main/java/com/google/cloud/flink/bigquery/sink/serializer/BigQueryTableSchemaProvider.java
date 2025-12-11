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

package com.google.cloud.flink.bigquery.sink.serializer;

import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.function.SerializableSupplier;

import com.google.api.client.util.Preconditions;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;
import com.google.cloud.flink.bigquery.common.config.CredentialsOptions;
import com.google.cloud.flink.bigquery.common.utils.SchemaTransform;
import com.google.cloud.flink.bigquery.services.BigQueryServices;
import com.google.cloud.flink.bigquery.sink.client.BigQueryClientWithErrorHandling;
import com.google.cloud.flink.bigquery.table.config.BigQueryTableConfig;
import org.apache.avro.Schema;

import java.io.IOException;

/**
 * Static utilities to derive Flink's Table schema and descriptor. Not to be confused with {@link
 * BigQuerySchemaProvider}.
 */
public class BigQueryTableSchemaProvider {
    // To ensure no instantiation
    private BigQueryTableSchemaProvider() {}

    private static SerializableSupplier<BigQueryServices> testingServices = null;

    public static void setTestingServices(SerializableSupplier<BigQueryServices> testingServices) {
        BigQueryTableSchemaProvider.testingServices = testingServices;
    }

    private static BigQueryConnectOptions getConnectOptionsFromTableConfig(
            BigQueryTableConfig tableConfig) throws IOException {
        return BigQueryConnectOptions.builder()
                .setTable(tableConfig.getTable())
                .setProjectId(tableConfig.getProject())
                .setDataset(tableConfig.getDataset())
                .setTestingBigQueryServices(testingServices)
                .setCredentialsOptions(
                        CredentialsOptions.builder()
                                .setAccessToken(tableConfig.getCredentialAccessToken())
                                .setCredentialsFile(tableConfig.getCredentialFile())
                                .setCredentialsKey(tableConfig.getCredentialKey())
                                .setQuotaProjectId(tableConfig.getQuotaProjectId())
                                .build())
                .build();
    }

    public static DataType getDataTypeSchemaFromAvroSchema(Schema avroSchema) {
        AvroSchemaConvertor avroSchemaConvertor = new AvroSchemaConvertor();
        return avroSchemaConvertor.convertToDataType(avroSchema.toString());
    }

    public static Schema getAvroSchemaFromLogicalSchema(LogicalType logicalType) {
        AvroSchemaConvertor avroSchemaConvertor = new AvroSchemaConvertor();
        return avroSchemaConvertor.convertToSchema(logicalType);
    }

    public static org.apache.flink.table.api.Schema getTableApiSchemaFromAvroSchema(
            Schema avroSchema) {
        Preconditions.checkNotNull(
                avroSchema, "Avro Schema not initialized before obtaining Table API Schema.");
        DataType dataTypeSchema = getDataTypeSchemaFromAvroSchema(avroSchema);
        return org.apache.flink.table.api.Schema.newBuilder()
                .fromRowDataType(dataTypeSchema)
                .build();
    }

    public static TableDescriptor getTableDescriptor(BigQueryTableConfig tableConfig)
            throws IOException {
        // Translate to connect options
        BigQueryConnectOptions connectOptions = getConnectOptionsFromTableConfig(tableConfig);
        // Check if table exists
        if (!BigQueryClientWithErrorHandling.tableExists(connectOptions)) {
            throw new IllegalStateException(
                    "Cannot derive Flink TableDescriptor because destination BigQuery table doesn't exist. User must provide a TableDescriptor with appropriate schema.");
        }
        // Obtain the desired BigQuery Table Schema
        TableSchema bigQueryTableSchema =
                BigQueryClientWithErrorHandling.getTableSchema(connectOptions);
        // Obtain Avro Schema
        Schema avroSchema =
                SchemaTransform.toGenericAvroSchema("root", bigQueryTableSchema.getFields());
        // Convert to Table API Schema
        org.apache.flink.table.api.Schema tableApiSchema =
                getTableApiSchemaFromAvroSchema(avroSchema);
        // Build the Table Descriptor
        return getTableDescriptor(tableConfig, tableApiSchema);
    }

    public static TableDescriptor getTableDescriptor(
            BigQueryTableConfig tableConfig, org.apache.flink.table.api.Schema tableSchema)
            throws IOException {
        // Build the Table Descriptor
        TableDescriptor tableDescriptor =
                TableDescriptor.forConnector("bigquery").schema(tableSchema).build();
        return tableConfig.updateTableDescriptor(tableDescriptor);
    }
}
