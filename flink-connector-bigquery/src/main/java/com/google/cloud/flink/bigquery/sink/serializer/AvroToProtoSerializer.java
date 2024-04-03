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

import com.google.api.client.util.Preconditions;
import com.google.cloud.flink.bigquery.sink.exceptions.BigQuerySerializationException;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/** Serializer for converting Avro's {@link GenericRecord} to BigQuery proto. */
public class AvroToProtoSerializer implements BigQueryProtoSerializer<GenericRecord> {

    private Descriptor descriptor;
    private static final Map<Schema.Type, UnaryOperator<Object>> PRIMITIVE_TYPE_ENCODERS;

    /*
     * Map Avro Schema Type to an Encoding function which converts AvroSchema Primitive
     * Type to Dynamic Message.
     *
     * PRIMITIVE_TYPE_ENCODERS: A Map containing mapping from Primitive Avro Schema Type with encoder function.
     */
    static {
        PRIMITIVE_TYPE_ENCODERS = new EnumMap<>(Schema.Type.class);
        PRIMITIVE_TYPE_ENCODERS.put(Schema.Type.INT, UnaryOperator.identity()); // INT -> INT
        PRIMITIVE_TYPE_ENCODERS.put(Schema.Type.LONG, UnaryOperator.identity());
        PRIMITIVE_TYPE_ENCODERS.put(Schema.Type.DOUBLE, UnaryOperator.identity());
        PRIMITIVE_TYPE_ENCODERS.put(Schema.Type.BOOLEAN, UnaryOperator.identity());
        PRIMITIVE_TYPE_ENCODERS.put(
                Schema.Type.FLOAT,
                o -> Float.parseFloat(String.valueOf((float) o))); // FLOAT -> FLOAT
        PRIMITIVE_TYPE_ENCODERS.put(Schema.Type.STRING, Object::toString);
        PRIMITIVE_TYPE_ENCODERS.put(Schema.Type.ENUM, Object::toString);
        PRIMITIVE_TYPE_ENCODERS.put(
                Schema.Type.FIXED, o -> ByteString.copyFrom(((GenericData.Fixed) o).bytes()));
        PRIMITIVE_TYPE_ENCODERS.put(
                Schema.Type.BYTES, o -> ByteString.copyFrom(((ByteBuffer) o).array()));
    }

    /**
     * Initializer for the Serializer.
     *
     * @param bigQuerySchemaProvider BigQuerySchemaProvider for the Sink Table ({@link
     *     BigQuerySchemaProvider} object)
     */
    public void init(BigQuerySchemaProvider bigQuerySchemaProvider) {
        Preconditions.checkNotNull(
                bigQuerySchemaProvider,
                "BigQuerySchemaProvider not found while initializing AvroToProtoSerializer");
        Descriptor descriptor = bigQuerySchemaProvider.getDescriptor();
        Preconditions.checkNotNull(
                descriptor, "Destination BigQuery table's Proto Schema could not be found.");
        this.descriptor = descriptor;
    }

    @Override
    public ByteString serialize(GenericRecord record) throws BigQuerySerializationException {
        try {
            return getDynamicMessageFromGenericRecord(record, this.descriptor).toByteString();
        } catch (Exception e) {
            throw new BigQuerySerializationException(e.getMessage(), e);
        }
    }

    /**
     * Function to convert a Generic Avro Record to Dynamic Message to write using the Storage Write
     * API.
     *
     * @param element {@link GenericRecord} Object to convert to {@link DynamicMessage}
     * @param descriptor {@link Descriptor} describing the schema of the sink table.
     * @return {@link DynamicMessage} Object converted from the Generic Avro Record.
     */
    public static DynamicMessage getDynamicMessageFromGenericRecord(
            GenericRecord element, Descriptor descriptor) {
        Schema schema = element.getSchema();
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);
        // Get the record's schema and find the field descriptor for each field one by one.
        for (Schema.Field field : schema.getFields()) {
            // In case no field descriptor exists for the field, throw an error as we have
            // incompatible schemas.
            FieldDescriptor fieldDescriptor =
                    Preconditions.checkNotNull(
                            descriptor.findFieldByName(field.name().toLowerCase()));
            // Get the value for a field.
            // Check if the value is null.
            @Nullable Object value = element.get(field.name());
            if (value == null) {
                // If the field required, throw an error.
                if (fieldDescriptor.isRequired()) {
                    throw new IllegalArgumentException(
                            "Received null value for non-nullable field "
                                    + fieldDescriptor.getName());
                }
            } else {
                // Convert to Dynamic Message.
                value = toProtoValue(fieldDescriptor, field.schema(), value);
                builder.setField(fieldDescriptor, value);
            }
        }
        return builder.build();
    }

    /**
     * Function to convert a value of an AvroSchemaField value to required DynamicMessage value.
     *
     * @param fieldDescriptor {@link com.google.protobuf.Descriptors.FieldDescriptor} Object
     *     describing the sink table field to which given value needs to be converted to.
     * @param avroSchema {@link Schema} Object describing the value of Avro Schema Field.
     * @param value Value of the Avro Schema Field.
     * @return Converted Object.
     * @throws UnsupportedOperationException In case schema is of type MAP.
     */
    private static Object toProtoValue(
            FieldDescriptor fieldDescriptor, Schema avroSchema, Object value)
            throws UnsupportedOperationException {
        switch (avroSchema.getType()) {
            case RECORD:
                // Recursion
                return getDynamicMessageFromGenericRecord(
                        (GenericRecord) value, fieldDescriptor.getMessageType());
            case ARRAY:
                // Get an Iterable of all the values in the array.
                Iterable<Object> iterable = (Iterable<Object>) value;
                // Get the inner element type.
                @Nullable Schema arrayElementType = avroSchema.getElementType();
                if (arrayElementType.isNullable()) {
                    throw new IllegalArgumentException("Array cannot have NULLABLE datatype");
                }
                if (arrayElementType.isUnion()) {
                    throw new IllegalArgumentException(
                            "ARRAY cannot have multiple datatypes in BigQuery.");
                }
                // Convert each value one by one.
                return StreamSupport.stream(iterable.spliterator(), false)
                        .map(v -> toProtoValue(fieldDescriptor, arrayElementType, v))
                        .collect(Collectors.toList());
            case UNION:
                Schema type = BigQuerySchemaProviderImpl.handleUnionSchema(avroSchema).getLeft();
                // Get the schema of the field.
                return toProtoValue(fieldDescriptor, type, value);
            case MAP:
                throw new UnsupportedOperationException("Not supported yet");
            default:
                return scalarToProtoValue(avroSchema, value);
        }
    }

    /**
     * Function to convert Avro Schema Field value to Dynamic Message value (for Primitive and
     * Logical Types).
     *
     * @param fieldSchema Avro Schema describing the schema for the value.
     * @param value Avro Schema Field value to convert to Dynamic Message value.
     * @return Converted Dynamic Message value.
     */
    private static Object scalarToProtoValue(Schema fieldSchema, Object value) {
        String logicalTypeString = fieldSchema.getProp(LogicalType.LOGICAL_TYPE_PROP);
        @Nullable UnaryOperator<Object> encoder;
        String errorMessage;
        if (logicalTypeString != null) {
            // 1. In case, the Schema has a Logical Type.
            encoder = getLogicalEncoder(logicalTypeString);
            errorMessage = "Unsupported logical type " + logicalTypeString;
        } else {
            // 2. For all the other Primitive types.
            encoder = PRIMITIVE_TYPE_ENCODERS.get(fieldSchema.getType());
            errorMessage = "Unexpected Avro type " + fieldSchema;
        }
        if (encoder == null) {
            throw new IllegalArgumentException(errorMessage);
        }
        return encoder.apply(value);
    }

    /**
     * Function to obtain the Encoder Function responsible for encoding AvroSchemaField to
     * DynamicMessage.
     *
     * @param logicalTypeString String containing the name for Logical Schema Type.
     * @return Encoder Function which converts AvroSchemaField to DynamicMessage
     */
    private static UnaryOperator<Object> getLogicalEncoder(String logicalTypeString) {
        Map<String, UnaryOperator<Object>> mapping = new HashMap<>();
        return mapping.get(logicalTypeString);
    }
}
