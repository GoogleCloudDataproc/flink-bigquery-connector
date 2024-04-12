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
import org.apache.commons.lang3.tuple.ImmutablePair;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
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
            throw new BigQuerySerializationException(e.getMessage());
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
        // Get a record's field schema and find the field descriptor for each field one by one.
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
                // Do nothing in case value == null and fieldDescriptor != "REQUIRED"
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
                return AvroSchemaHandler.handleArraySchema(fieldDescriptor, avroSchema, value);
            case UNION:
                Schema type = AvroSchemaHandler.handleUnionSchema(avroSchema).getLeft();
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
                Object convertedValue =
                        AvroSchemaHandler.handleLogicalTypeSchema(avroSchema, value);
                if (convertedValue != value) {
                    return convertedValue;
                }
                return value.toString();
            case LONG:
                convertedValue = AvroSchemaHandler.handleLogicalTypeSchema(avroSchema, value);
                if (convertedValue != value) {
                    return convertedValue;
                }
                return Long.parseLong(value.toString());
            case INT:
                // Return the converted value.
                convertedValue = AvroSchemaHandler.handleLogicalTypeSchema(avroSchema, value);
                if (convertedValue != value) {
                    return convertedValue;
                }
                return Integer.parseInt(value.toString());
            case BYTES:
                // Find if it is of decimal Type.
                convertedValue = AvroSchemaHandler.handleLogicalTypeSchema(avroSchema, value);
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

    /** Class to handle Specific Avro Proto Schema Types (Logical and Union). */
    static class AvroSchemaHandler {
        private AvroSchemaHandler() {}

        /**
         * Function to convert a value of an <b>ARRAY</b> Type AvroSchemaField value to required
         * Dynamic Message value.
         *
         * @param fieldDescriptor {@link com.google.protobuf.Descriptors.FieldDescriptor} Object
         *     describes the destination field in the sink table. Given value must be converted to a
         *     format compatible with this field.
         * @param avroSchema {@link Schema} Object describing the value of Avro Schema Field.
         * @param value Value of the Avro Schema Field.
         * @return Converted List Object
         */
        public static List<Object> handleArraySchema(
                FieldDescriptor fieldDescriptor, Schema avroSchema, Object value) {
            Iterable<Object> iterable;
            if (value instanceof Iterable) {
                iterable = (Iterable<Object>) value;
            } else {
                throw new IllegalArgumentException(
                        String.format(
                                "Expected an Iterable,%n Found %s instead.", value.getClass()));
            }
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
        }

        /**
         * Helper function to handle the UNION Schema Type. We only consider the union schema valid
         * when it is of the form ["null", datatype]. All other forms such as ["null"],["null",
         * datatype1, datatype2, ...], and [datatype1, datatype2, ...] Are considered as invalid (as
         * there is no such support in BQ) So we throw an error in all such cases. For the valid
         * case of ["null", datatype] or [datatype] we set the Schema as the schema of the <b>not
         * null</b> datatype.
         *
         * @param schema of type UNION to check and derive.
         * @return Schema of the OPTIONAL field.
         * @throws IllegalArgumentException If multiple non-null datatypes or only null is observed.
         */
        public static ImmutablePair<Schema, Boolean> handleUnionSchema(Schema schema)
                throws IllegalArgumentException {
            Schema elementType = schema;
            boolean isNullable = true;
            List<Schema> types = elementType.getTypes();
            // don't need recursion because nested unions aren't supported in AVRO
            // Extract all the nonNull Datatypes.
            List<Schema> nonNullSchemaTypes =
                    types.stream()
                            .filter(schemaType -> schemaType.getType() != Schema.Type.NULL)
                            .collect(Collectors.toList());

            int nonNullSchemaTypesSize = nonNullSchemaTypes.size();

            if (nonNullSchemaTypesSize == 1) {
                elementType = nonNullSchemaTypes.get(0);
                if (nonNullSchemaTypesSize == types.size()) {
                    // Case, when there is only a single type in UNION.
                    // Then it is essentially the same as not having a UNION.
                    isNullable = false;
                }
                return new ImmutablePair<>(elementType, isNullable);
            }

            throw new IllegalArgumentException("Multiple non-null union types are not supported.");
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
         * Function to obtain the Encoder Function responsible for encoding AvroSchemaField to
         * Dynamic Message.
         *
         * @param logicalTypeString String containing the name for Logical Schema Type.
         * @return Encoder Function which converts AvroSchemaField to {@link DynamicMessage}
         */
        private static UnaryOperator<Object> getLogicalEncoder(String logicalTypeString) {
            Map<String, UnaryOperator<Object>> mapping = new HashMap<>();
            return mapping.get(logicalTypeString);
        }
    }
}
