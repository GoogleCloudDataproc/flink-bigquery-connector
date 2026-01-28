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

import com.google.cloud.flink.bigquery.common.exceptions.BigQueryConnectorException;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FileDescriptor;
import org.apache.avro.Schema;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * A BigQuery schema provider that wraps an existing schema provider and augments it with CDC
 * (Change Data Capture) pseudocolumns.
 *
 * <p>BigQuery CDC requires two pseudocolumns to be included in write operations:
 *
 * <ul>
 *   <li>{@code _change_type}: String value of "UPSERT" or "DELETE"
 *   <li>{@code _change_sequence_number}: Hexadecimal string for ordering changes
 * </ul>
 *
 * <p>These pseudocolumns are not stored as table columns but are metadata that BigQuery uses during
 * ingestion to perform upsert/delete operations based on the table's primary key.
 *
 * @see <a href="https://cloud.google.com/bigquery/docs/change-data-capture">BigQuery CDC
 *     Documentation</a>
 */
public class BigQueryCdcSchemaProvider implements BigQuerySchemaProvider {

    /** Name of the CDC change type pseudocolumn. */
    public static final String CDC_CHANGE_TYPE_FIELD = "_change_type";

    /** Name of the CDC sequence number pseudocolumn. */
    public static final String CDC_SEQUENCE_NUMBER_FIELD = "_change_sequence_number";

    private final BigQuerySchemaProvider delegate;
    private final Schema augmentedAvroSchema;
    private final DescriptorProto augmentedDescriptorProto;
    private final Descriptor augmentedDescriptor;

    /**
     * Creates a new CDC schema provider wrapping the given schema provider.
     *
     * @param delegate The underlying schema provider to augment with CDC columns.
     */
    public BigQueryCdcSchemaProvider(BigQuerySchemaProvider delegate) {
        this.delegate = delegate;

        if (delegate.schemaUnknown()) {
            this.augmentedAvroSchema = null;
            this.augmentedDescriptorProto = null;
            this.augmentedDescriptor = null;
        } else {
            this.augmentedAvroSchema = createAugmentedAvroSchema(delegate.getAvroSchema());
            this.augmentedDescriptorProto =
                    createAugmentedDescriptorProto(delegate.getDescriptorProto());
            this.augmentedDescriptor = buildDescriptor(augmentedDescriptorProto);
        }
    }

    @Override
    public DescriptorProto getDescriptorProto() {
        return augmentedDescriptorProto;
    }

    @Override
    public Descriptor getDescriptor() {
        return augmentedDescriptor;
    }

    @Override
    public Schema getAvroSchema() {
        return augmentedAvroSchema;
    }

    @Override
    public boolean schemaUnknown() {
        return delegate.schemaUnknown();
    }

    /**
     * Creates an augmented Avro schema with CDC pseudocolumns added.
     *
     * @param baseSchema The original Avro schema.
     * @return A new schema with CDC fields appended.
     */
    private static Schema createAugmentedAvroSchema(Schema baseSchema) {
        List<Schema.Field> augmentedFields = new ArrayList<>();

        // Copy all existing fields
        for (Schema.Field field : baseSchema.getFields()) {
            augmentedFields.add(
                    new Schema.Field(
                            field.name(),
                            field.schema(),
                            field.doc(),
                            field.defaultVal(),
                            field.order()));
        }

        // Add CDC pseudocolumns as nullable strings
        Schema nullableString =
                Schema.createUnion(
                        Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING));

        augmentedFields.add(
                new Schema.Field(
                        CDC_CHANGE_TYPE_FIELD,
                        nullableString,
                        "CDC change type: UPSERT or DELETE",
                        Schema.Field.NULL_DEFAULT_VALUE));

        augmentedFields.add(
                new Schema.Field(
                        CDC_SEQUENCE_NUMBER_FIELD,
                        nullableString,
                        "CDC sequence number for ordering (hexadecimal string)",
                        Schema.Field.NULL_DEFAULT_VALUE));

        return Schema.createRecord(
                baseSchema.getName(),
                baseSchema.getDoc(),
                baseSchema.getNamespace(),
                baseSchema.isError(),
                augmentedFields);
    }

    /**
     * Creates an augmented DescriptorProto with CDC pseudocolumns added.
     *
     * @param baseDescriptorProto The original descriptor proto.
     * @return A new descriptor proto with CDC fields appended.
     */
    private static DescriptorProto createAugmentedDescriptorProto(
            DescriptorProto baseDescriptorProto) {
        DescriptorProto.Builder builder = baseDescriptorProto.toBuilder();

        // Find the next available field number
        int maxFieldNumber = 0;
        for (FieldDescriptorProto field : baseDescriptorProto.getFieldList()) {
            maxFieldNumber = Math.max(maxFieldNumber, field.getNumber());
        }

        // Add _change_type field
        builder.addField(
                FieldDescriptorProto.newBuilder()
                        .setName(CDC_CHANGE_TYPE_FIELD)
                        .setNumber(maxFieldNumber + 1)
                        .setType(FieldDescriptorProto.Type.TYPE_STRING)
                        .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                        .build());

        // Add _change_sequence_number field
        builder.addField(
                FieldDescriptorProto.newBuilder()
                        .setName(CDC_SEQUENCE_NUMBER_FIELD)
                        .setNumber(maxFieldNumber + 2)
                        .setType(FieldDescriptorProto.Type.TYPE_STRING)
                        .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                        .build());

        return builder.build();
    }

    /**
     * Builds a Descriptor from a DescriptorProto.
     *
     * @param descriptorProto The descriptor proto to convert.
     * @return The built Descriptor.
     * @throws BigQueryConnectorException if descriptor validation fails.
     */
    private static Descriptor buildDescriptor(DescriptorProto descriptorProto) {
        try {
            FileDescriptorProto fileDescriptorProto =
                    FileDescriptorProto.newBuilder().addMessageType(descriptorProto).build();
            FileDescriptor fileDescriptor =
                    FileDescriptor.buildFrom(fileDescriptorProto, new FileDescriptor[0]);
            List<Descriptor> descriptorTypeList = fileDescriptor.getMessageTypes();
            if (descriptorTypeList.size() == 1) {
                return descriptorTypeList.get(0);
            } else {
                throw new IllegalArgumentException(
                        String.format("Expected one element but was %s", descriptorTypeList));
            }
        } catch (DescriptorValidationException | IllegalArgumentException e) {
            throw new BigQueryConnectorException(
                    "Could not build Descriptor for CDC-augmented schema", e);
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(delegate, augmentedAvroSchema);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        BigQueryCdcSchemaProvider other = (BigQueryCdcSchemaProvider) obj;
        return Objects.equals(delegate, other.delegate)
                && Objects.equals(augmentedAvroSchema, other.augmentedAvroSchema);
    }
}
