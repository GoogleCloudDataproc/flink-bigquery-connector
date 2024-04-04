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
import java.util.HashMap;
import java.util.Map;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/** Serializer for converting Avro's {@link GenericRecord} to BigQuery proto. */
public class AvroToProtoSerializer implements BigQueryProtoSerializer<GenericRecord> {

    private Descriptor descriptor;

    /**
     * Prepares the serializer before its serialize method can be called. It allows contextual
     * preprocessing after constructor and before serialize. The Sink will internally call this
     * method when initializing itself.
     *
     * @param bigQuerySchemaProvider {@link BigQuerySchemaProvider} for the destination table.
     */
    @Override
    public void init(BigQuerySchemaProvider bigQuerySchemaProvider) {
        Preconditions.checkNotNull(
                bigQuerySchemaProvider,
                "BigQuerySchemaProvider not found while initializing AvroToProtoSerializer");
        Descriptor derivedDescriptor = bigQuerySchemaProvider.getDescriptor();
        Preconditions.checkNotNull(
                derivedDescriptor, "Destination BigQuery table's Proto Schema could not be found.");
        this.descriptor = derivedDescriptor;
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
        Schema recordSchema = element.getSchema();
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);
        // Get the record's schema and find the field descriptor for each field one by one.
        for (Schema.Field field : recordSchema.getFields()) {
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
     * Function to convert a value of an AvroSchemaField value to required Dynamic Message value.
     *
     * @param fieldDescriptor {@link com.google.protobuf.Descriptors.FieldDescriptor} Object
     *     describing the sink table field to which given value needs to be converted to.
     * @param avroSchema {@link Schema} Object describing the value of Avro Schema Field.
     * @param value Value of the Avro Schema Field.
     * @return Converted Object.
     */
    private static Object toProtoValue(
            FieldDescriptor fieldDescriptor, Schema avroSchema, Object value) {
        switch (avroSchema.getType()) {
            case RECORD:
                return getDynamicMessageFromGenericRecord(
                        (GenericRecord) value, fieldDescriptor.getMessageType());
            case ARRAY:
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
            case STRING:
                /*
                Logical Types currently supported are of the following underlying types:
                    DATE - INT
                    DECIMAL - BYTES
                    TIMESTAMP-MICROS - LONG
                    TIMESTAMP-MILLIS - LONG
                    UUID - STRING
                    TIME-MILLIS - INT/STRING
                    TIME-MICROS - LONG/STRING
                    LOCAL-TIMESTAMP-MICROS - LONG/STRING
                    LOCAL-TIMESTAMP-MILLIS - LONG/STRING
                    GEOGRAPHY - STRING
                    JSON - STRING
                */
            case LONG:
            case INT:
                // Return the converted value.
                return handleLogicalTypeSchema(avroSchema, value);
            case BYTES:
                // Find if it is of decimal Type.
                Object convertedValue = handleLogicalTypeSchema(avroSchema, value);
                if (convertedValue != value) {
                    return convertedValue;
                }
                return ByteString.copyFrom(((ByteBuffer) value).array());
            case ENUM:
                return value.toString();
            case FIXED:
                return ByteString.copyFrom(((GenericData.Fixed) value).bytes());
            case BOOLEAN:
                return (boolean) value;
            case FLOAT:
                return Float.parseFloat(String.valueOf((float) value));
            case DOUBLE:
                return (double) value;
            case NULL:
                throw new IllegalArgumentException("Null Type Field not supported in BigQuery!");
            default:
                throw new IllegalArgumentException("Unexpected Avro type" + avroSchema);
        }
    }

    /**
     * Function to convert Avro Schema Field value to Dynamic Message value (for Logical Types).
     *
     * @param fieldSchema Avro Schema describing the schema for the value.
     * @param value Avro Schema Field value to convert to {@link DynamicMessage} value.
     * @return Converted {@link DynamicMessage} value if a supported logical types exists, param
     *     value otherwise.
     */
    private static Object handleLogicalTypeSchema(Schema fieldSchema, Object value) {
        String logicalTypeString = fieldSchema.getProp(LogicalType.LOGICAL_TYPE_PROP);
        if (logicalTypeString != null) {
            // 1. In case, the Schema has a Logical Type.
            @Nullable UnaryOperator<Object> encoder = getLogicalEncoder(logicalTypeString);
            // 2. Check if this is supported, Return the value
            if (encoder != null) {
                return encoder.apply(value);
            }
        }
        // Otherwise, return the value as it is.
        return value;
    }

    /**
     * Function to obtain the Encoder Function responsible for encoding AvroSchemaField to Dynamic
     * Message.
     *
     * @param logicalTypeString String containing the name for Logical Schema Type.
     * @return Encoder Function which converts AvroSchemaField to {@link DynamicMessage}
     */
    private static UnaryOperator<Object> getLogicalEncoder(String logicalTypeString) {
        Map<String, UnaryOperator<Object>> mapping = new HashMap<>();
        return mapping.get(logicalTypeString);
    }
}
