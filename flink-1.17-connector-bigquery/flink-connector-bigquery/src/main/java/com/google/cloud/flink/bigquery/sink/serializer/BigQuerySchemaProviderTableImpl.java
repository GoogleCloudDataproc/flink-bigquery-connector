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

import com.google.api.client.util.Preconditions;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;
import com.google.cloud.flink.bigquery.table.config.BigQueryConnectorOptions;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import org.apache.avro.Schema;

import java.io.IOException;
import java.util.Map;

/**
 * Default implementation of {@link BigQuerySchemaProvider} deriving Avro {@link Schema} from {@link
 * TableSchema}, which in turn is sourced from {@link BigQueryConnectOptions}.
 */
public class BigQuerySchemaProviderTableImpl implements BigQuerySchemaProvider {

    private final TableDescriptor tableDescriptor;
    private BigQuerySchemaProvider bigQuerySchemaProvider;

    private BigQueryConnectOptions getConnectOptionsFromMap(Map<String, String> options)
            throws IOException {
        return BigQueryConnectOptions.builder()
                .setTable(options.get(BigQueryConnectorOptions.TABLE.key()))
                .setProjectId(options.get(BigQueryConnectorOptions.PROJECT.key()))
                .setDataset(options.get(BigQueryConnectorOptions.DATASET.key()))
                .build();
    }

    public static DataType getDataTypeSchemaFromAvroSchema(Schema avroSchema) {
        return AvroSchemaConvertor.convertToDataType(avroSchema.toString());
    }

    private org.apache.flink.table.api.Schema getTableApiSchemaFromAvroSchema(Schema avroSchema) {
        DataType dataTypeSchema = getDataTypeSchemaFromAvroSchema(avroSchema);
        return org.apache.flink.table.api.Schema.newBuilder()
                .fromRowDataType(dataTypeSchema)
                .build();
    }

    private org.apache.flink.table.api.Schema getTableApiSchemaFromAvroSchema() {
        Schema avroSchema = this.getAvroSchema();
        if (avroSchema == null) {
            throw new RuntimeException(
                    "Avro Schema Not initialized before obtaining Table API schema");
        }
        return getTableApiSchemaFromAvroSchema(this.getAvroSchema());
    }

    public BigQuerySchemaProviderTableImpl(Map<String, String> options) throws IOException {

        // Form the Connect Options from Map of Options
        this.bigQuerySchemaProvider =
                new BigQuerySchemaProviderImpl(getConnectOptionsFromMap(options));

        org.apache.flink.table.api.Schema tableApiSchema = getTableApiSchemaFromAvroSchema();

        TableDescriptor.Builder tableDescriptorBuilder =
                TableDescriptor.forConnector("bigquery").schema(tableApiSchema);

        for (Map.Entry<String, String> entry : options.entrySet()) {
            tableDescriptorBuilder.option(entry.getKey(), entry.getValue());
        }
        this.tableDescriptor = tableDescriptorBuilder.build();
    }

    @Override
    public DescriptorProtos.DescriptorProto getDescriptorProto() {
        Preconditions.checkNotNull(
                this.bigQuerySchemaProvider,
                "Descriptor Proto Referenced before Schema Provider is initialized!");
        return this.bigQuerySchemaProvider.getDescriptorProto();
    }

    public TableDescriptor getTableDescriptor() {
        Preconditions.checkNotNull(
                this.bigQuerySchemaProvider,
                "TableDescriptor Referenced before Schema Provider is initialized!");
        return this.tableDescriptor;
    }

    @Override
    public Descriptors.Descriptor getDescriptor() {
        Preconditions.checkNotNull(
                this.bigQuerySchemaProvider,
                "Descriptor Referenced before Schema Provider is initialized!");
        return this.bigQuerySchemaProvider.getDescriptor();
    }

    @Override
    public Schema getAvroSchema() {
        Preconditions.checkNotNull(
                this.bigQuerySchemaProvider,
                "Avro Schema Referenced before Schema Provider is initialized!");
        return this.bigQuerySchemaProvider.getAvroSchema();
    }
}
