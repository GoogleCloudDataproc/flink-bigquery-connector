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
import com.google.cloud.flink.bigquery.services.BigQueryServices;
import com.google.cloud.flink.bigquery.table.config.BigQueryTableConfig;
import org.apache.avro.Schema;

import java.io.IOException;

/**
 * Default implementation of {@link BigQuerySchemaProvider} deriving Avro {@link Schema} from {@link
 * TableSchema}, which in turn is sourced from {@link BigQueryConnectOptions}.
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
                                .build())
                .build();
    }

    public static DataType getDataTypeSchemaFromAvroSchema(Schema avroSchema) {
        return AvroSchemaConvertor.convertToDataType(avroSchema.toString());
    }

    public static Schema getAvroSchemaFromLogicalSchema(LogicalType logicalType) {
        return AvroSchemaConvertor.convertToSchema(logicalType);
    }

    private static org.apache.flink.table.api.Schema getTableApiSchemaFromAvroSchema(
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
        // Translate to connect Options
        BigQueryConnectOptions connectOptions = getConnectOptionsFromTableConfig(tableConfig);
        // Obtain the desired BigQuery Table Schema
        TableSchema bigQueryTableSchema =
                BigQuerySchemaProviderImpl.getTableSchemaFromOptions(connectOptions);
        // Obtain Avro Schema
        Schema avroSchema = BigQuerySchemaProviderImpl.getAvroSchema(bigQueryTableSchema);
        // Convert to Table API Schema
        org.apache.flink.table.api.Schema tableApiSchema =
                getTableApiSchemaFromAvroSchema(avroSchema);
        // Build the Table Descriptor
        TableDescriptor tableDescriptor =
                TableDescriptor.forConnector("bigquery").schema(tableApiSchema).build();
        return tableConfig.updateTableDescriptor(tableDescriptor);
    }
}
