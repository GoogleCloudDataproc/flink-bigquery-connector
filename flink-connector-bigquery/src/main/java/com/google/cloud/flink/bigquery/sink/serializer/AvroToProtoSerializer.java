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
import com.google.cloud.flink.bigquery.common.utils.SchemaTransform;
import com.google.cloud.flink.bigquery.services.BigQueryUtils;
import com.google.cloud.flink.bigquery.sink.exceptions.BigQuerySerializationException;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/** Serializer for converting Avro's {@link GenericRecord} to BigQuery proto. */
public class AvroToProtoSerializer implements BigQueryProtoSerializer<GenericRecord> {

    private final DescriptorProto descriptorProto;
    private final Descriptors.Descriptor descriptor;

    private static final Map<Schema.Type, UnaryOperator<Object>> PRIMITIVE_TYPE_ENCODERS =
            initializePrimitiveEncoderFunction();

    private static final Map<Schema.Type, FieldDescriptorProto.Type> AVRO_TYPES_TO_PROTO =
            initializeAvroFieldToFieldDescriptorTypes();

    private static final Map<String, FieldDescriptorProto.Type> LOGICAL_AVRO_TYPES_TO_PROTO =
            initializeLogicalAvroFieldToFieldDescriptorTypes();

    /**
     * Function to map Avro Schema Type to FieldDescriptorProto Type which converts AvroSchema
     * Primitive Type to Dynamic Message.
     *
     * @return Map containing mapping from Primitive Avro Schema Type to FieldDescriptorProto.
     */
    private static EnumMap<Schema.Type, FieldDescriptorProto.Type>
            initializeAvroFieldToFieldDescriptorTypes() {
        EnumMap<Schema.Type, FieldDescriptorProto.Type> mapping = new EnumMap<>(Schema.Type.class);
        mapping.put(Schema.Type.INT, FieldDescriptorProto.Type.TYPE_INT64);
        mapping.put(Schema.Type.FIXED, FieldDescriptorProto.Type.TYPE_BYTES);
        mapping.put(Schema.Type.LONG, FieldDescriptorProto.Type.TYPE_INT64);
        mapping.put(Schema.Type.FLOAT, FieldDescriptorProto.Type.TYPE_FLOAT);
        mapping.put(Schema.Type.DOUBLE, FieldDescriptorProto.Type.TYPE_DOUBLE);
        mapping.put(Schema.Type.STRING, FieldDescriptorProto.Type.TYPE_STRING);
        mapping.put(Schema.Type.BOOLEAN, FieldDescriptorProto.Type.TYPE_BOOL);
        mapping.put(Schema.Type.ENUM, FieldDescriptorProto.Type.TYPE_STRING);
        mapping.put(Schema.Type.BYTES, FieldDescriptorProto.Type.TYPE_BYTES);
        return mapping;
    }

    /**
     * Function to map Logical Avro Schema Type to FieldDescriptorProto Type which converts
     * AvroSchema Primitive Type to Dynamic Message.
     *
     * @return Map containing mapping from Primitive Avro Schema Type to FieldDescriptorProto.
     */
    private static Map<String, FieldDescriptorProto.Type>
            initializeLogicalAvroFieldToFieldDescriptorTypes() {
        Map<String, FieldDescriptorProto.Type> mapping = new HashMap<>();
        mapping.put(LogicalTypes.date().getName(), FieldDescriptorProto.Type.TYPE_INT32);
        mapping.put(LogicalTypes.decimal(1).getName(), FieldDescriptorProto.Type.TYPE_BYTES);
        mapping.put(LogicalTypes.timestampMicros().getName(), FieldDescriptorProto.Type.TYPE_INT64);
        mapping.put(LogicalTypes.timestampMillis().getName(), FieldDescriptorProto.Type.TYPE_INT64);
        mapping.put(LogicalTypes.uuid().getName(), FieldDescriptorProto.Type.TYPE_STRING);
        // These are newly added.
        mapping.put(LogicalTypes.timeMillis().getName(), FieldDescriptorProto.Type.TYPE_STRING);
        mapping.put(LogicalTypes.timeMicros().getName(), FieldDescriptorProto.Type.TYPE_STRING);
        mapping.put(
                LogicalTypes.localTimestampMillis().getName(),
                FieldDescriptorProto.Type.TYPE_STRING);
        mapping.put(
                LogicalTypes.localTimestampMicros().getName(),
                FieldDescriptorProto.Type.TYPE_STRING);
        mapping.put("geography_wkt", FieldDescriptorProto.Type.TYPE_STRING);
        mapping.put("Json", FieldDescriptorProto.Type.TYPE_STRING);
        return mapping;
    }

    /**
     * Function to map Avro Schema Type to an Encoding function which converts AvroSchema Primitive
     * Type to Dynamic Message.
     *
     * @return Map containing mapping from Primitive Avro Schema Type with encoder function.
     */
    private static EnumMap<Schema.Type, UnaryOperator<Object>>
            initializePrimitiveEncoderFunction() {
        EnumMap<Schema.Type, UnaryOperator<Object>> mapping = new EnumMap<>(Schema.Type.class);
        mapping.put(Schema.Type.INT, o -> (long) (int) o); // INT -> long
        mapping.put(Schema.Type.LONG, UnaryOperator.identity());
        mapping.put(Schema.Type.DOUBLE, UnaryOperator.identity());
        mapping.put(Schema.Type.BOOLEAN, UnaryOperator.identity());
        mapping.put(
                Schema.Type.FLOAT,
                o -> Float.parseFloat(String.valueOf((float) o))); // FLOAT -> FLOAT
        mapping.put(Schema.Type.STRING, Object::toString);
        mapping.put(Schema.Type.ENUM, Object::toString);
        mapping.put(Schema.Type.FIXED, o -> ByteString.copyFrom(((GenericData.Fixed) o).bytes()));
        mapping.put(Schema.Type.BYTES, o -> ByteString.copyFrom(((ByteBuffer) o).array()));
        return mapping;
    }

    /**
     * Function to convert TableSchema to Avro Schema.
     *
     * @param tableSchema A {@link com.google.api.services.bigquery.model.TableSchema} object to
     *     cast to {@link Schema}
     * @return Converted Avro Schema
     */
    private Schema getAvroSchema(com.google.api.services.bigquery.model.TableSchema tableSchema) {
        return SchemaTransform.toGenericAvroSchema("root", tableSchema.getFields());
    }

    /**
     * Helper function to handle the UNION Schema Type. We only consider the union schema valid when
     * it is of the form ["null", datatype]. All other forms such as ["null"],["null", datatype1,
     * datatype2, ...], and [datatype1, datatype2, ...] Are considered as invalid (as there is no
     * such support in BQ) So we throw an error in all such cases. For the valid case of ["null",
     * datatype] we set the Schema as the schema of the <b>not null</b> datatype.
     *
     * @param schema of type UNION to check and derive.
     * @return Schema of the OPTIONAL field.
     */
    private static Schema handleUnionSchema(Schema schema) {
        Schema elementType = schema;
        List<Schema> types = elementType.getTypes();
        // don't need recursion because nested unions aren't supported in AVRO
        // Extract all the nonNull Datatypes.
        List<org.apache.avro.Schema> nonNullSchemaTypes =
                types.stream()
                        .filter(schemaType -> schemaType.getType() != Schema.Type.NULL)
                        .collect(Collectors.toList());

        int nonNullSchemaTypesSize = nonNullSchemaTypes.size();

        if (nonNullSchemaTypesSize == 1) {
            elementType = nonNullSchemaTypes.get(0);
            return elementType;
        } else {
            throw new IllegalArgumentException("Multiple non-null union types are not supported.");
        }
    }

    /**
     * Function to obtain the FieldDescriptorProto from a AvroSchemaField and then append it to
     * DescriptorProto builder.
     *
     * @param field {@link Schema.Field} object to obtain the FieldDescriptorProto from.
     * @param fieldNumber index at which the obtained FieldDescriptorProto is appended in the
     *     Descriptor.
     * @param descriptorProtoBuilder {@link DescriptorProto.Builder} object to add the obtained
     *     FieldDescriptorProto to.
     */
    private static void fieldDescriptorFromSchemaField(
            Schema.Field field, int fieldNumber, DescriptorProto.Builder descriptorProtoBuilder) {

        @Nullable Schema schema = field.schema();
        Preconditions.checkNotNull(schema, "Unexpected null schema!");

        FieldDescriptorProto.Builder fieldDescriptorBuilder = FieldDescriptorProto.newBuilder();
        fieldDescriptorBuilder = fieldDescriptorBuilder.setName(field.name().toLowerCase());
        fieldDescriptorBuilder = fieldDescriptorBuilder.setNumber(fieldNumber);

        Schema elementType = schema;
        boolean isNullable = false;

        switch (schema.getType()) {
            case RECORD:
                Preconditions.checkState(!schema.getFields().isEmpty());
                // Check if this is right.
                DescriptorProto nested = getDescriptorSchemaFromAvroSchema(schema);
                descriptorProtoBuilder.addNestedType(nested);
                fieldDescriptorBuilder =
                        fieldDescriptorBuilder
                                .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                                .setTypeName(nested.getName());
                break;
            case ARRAY:
                elementType = schema.getElementType();
                if (elementType == null) {
                    throw new IllegalArgumentException("Unexpected null element type!");
                }
                Preconditions.checkState(
                        elementType.getType() != Schema.Type.ARRAY,
                        "Nested arrays not supported by BigQuery.");
                if (elementType.getType() == Schema.Type.MAP) {
                    // Note this case would be covered in the check which is performed later,
                    // but just in case the support for this is provided in the future,
                    // it is explicitly mentioned.
                    throw new UnsupportedOperationException("Array of Type MAP not supported yet.");
                }
                DescriptorProto.Builder arrayFieldBuilder = DescriptorProto.newBuilder();
                fieldDescriptorFromSchemaField(
                        new Schema.Field(
                                field.name(), elementType, field.doc(), field.defaultVal()),
                        fieldNumber,
                        arrayFieldBuilder);

                FieldDescriptorProto.Builder arrayFieldElementBuilder =
                        arrayFieldBuilder.getFieldBuilder(0);
                // Check if the inner field is optional without any default value.
                if (arrayFieldElementBuilder.getLabel()
                        != FieldDescriptorProto.Label.LABEL_REQUIRED) {
                    throw new IllegalArgumentException("Array cannot have a NULLABLE element");
                }
                // Default value derived from inner layers should not be the default value of array
                // field.
                fieldDescriptorBuilder =
                        arrayFieldElementBuilder
                                .setLabel(FieldDescriptorProto.Label.LABEL_REPEATED)
                                .clearDefaultValue();
                // Add any nested types.
                descriptorProtoBuilder.addAllNestedType(arrayFieldBuilder.getNestedTypeList());
                break;
            case MAP:
                // A MAP is converted to an array of structs.
                Schema keyType = Schema.create(Schema.Type.STRING);
                Schema valueType = elementType.getValueType();
                if (valueType == null) {
                    throw new IllegalArgumentException("Unexpected null element type!");
                }
                // Create a new field of type RECORD.
                Schema.Field keyField = new Schema.Field("key", keyType, "key of the map entry");
                Schema.Field valueField =
                        new Schema.Field("value", valueType, "value of the map entry");
                Schema mapFieldSchema =
                        Schema.createRecord(
                                schema.getName(),
                                schema.getDoc(),
                                "com.google.flink.bigquery",
                                true,
                                Arrays.asList(keyField, valueField));

                DescriptorProto.Builder mapFieldBuilder = DescriptorProto.newBuilder();
                fieldDescriptorFromSchemaField(
                        new Schema.Field(
                                field.name(), mapFieldSchema, field.doc(), field.defaultVal()),
                        fieldNumber,
                        mapFieldBuilder);

                FieldDescriptorProto.Builder mapFieldElementBuilder =
                        mapFieldBuilder.getFieldBuilder(0);
                // Check if the inner field is optional without any default value.
                // This should not be the case since we explicitly create the STRUCT.
                if (mapFieldElementBuilder.getLabel()
                        != FieldDescriptorProto.Label.LABEL_REQUIRED) {
                    throw new IllegalArgumentException("MAP cannot have a null element");
                }
                fieldDescriptorBuilder =
                        mapFieldElementBuilder
                                .setLabel(FieldDescriptorProto.Label.LABEL_REPEATED)
                                .clearDefaultValue();

                // Add the nested types.
                descriptorProtoBuilder.addAllNestedType(mapFieldBuilder.getNestedTypeList());
                break;
            case UNION:
                // Types can be ["null"] - Not supported in BigQuery.
                // ["null", something] -
                // Bigquery Field of type something with mode NULLABLE
                // ["null", something1, something2], [something1, something2] -
                // Are invalid types
                // not supported in BQ
                elementType = handleUnionSchema(schema);
                // either the field is nullable or error would be thrown.
                isNullable = true;
                if (elementType == null) {
                    throw new IllegalArgumentException("Unexpected null element type!");
                }
                if (elementType.getType() == Schema.Type.MAP
                        || elementType.getType() == Schema.Type.ARRAY) {
                    throw new UnsupportedOperationException(
                            "MAP/ARRAYS in UNION types are not supported");
                }
                DescriptorProto.Builder unionFieldBuilder = DescriptorProto.newBuilder();
                fieldDescriptorFromSchemaField(
                        new Schema.Field(
                                field.name(), elementType, field.doc(), field.defaultVal()),
                        fieldNumber,
                        unionFieldBuilder);
                fieldDescriptorBuilder = unionFieldBuilder.getFieldBuilder(0);
                descriptorProtoBuilder.addAllNestedType(unionFieldBuilder.getNestedTypeList());
                break;
            default:
                @Nullable
                FieldDescriptorProto.Type type =
                        Optional.ofNullable(elementType.getProp(LogicalType.LOGICAL_TYPE_PROP))
                                .map(LOGICAL_AVRO_TYPES_TO_PROTO::get)
                                .orElse(AVRO_TYPES_TO_PROTO.get(elementType.getType()));
                if (type == null) {
                    throw new UnsupportedOperationException(
                            "Converting AVRO type "
                                    + elementType.getType()
                                    + " to Storage API Proto type is unsupported");
                }
                fieldDescriptorBuilder = fieldDescriptorBuilder.setType(type);
                if (field.hasDefaultValue()) {
                    fieldDescriptorBuilder =
                            fieldDescriptorBuilder.setDefaultValue(field.defaultVal().toString());
                }
        }
        // Set the Labels for different Modes - REPEATED, REQUIRED, NULLABLE.
        if (fieldDescriptorBuilder.getLabel() != FieldDescriptorProto.Label.LABEL_REPEATED) {
            if (isNullable) {
                fieldDescriptorBuilder =
                        fieldDescriptorBuilder.setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL);
            } else {
                // The Default Value is specified only in the case of scalar non-repeated fields.
                // If it was a scalar type, the default value would already have been set.
                fieldDescriptorBuilder =
                        fieldDescriptorBuilder.setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED);
            }
        }
        descriptorProtoBuilder.addField(fieldDescriptorBuilder.build());
    }

    /**
     * Obtains a Descriptor Proto by obtaining Descriptor Proto field by field.
     *
     * <p>Iterates over Avro Schema to obtain FieldDescriptorProto for it.
     *
     * @param schema Avro Schema, for which descriptor is needed.
     * @return DescriptorProto describing the Schema.
     */
    public static DescriptorProto getDescriptorSchemaFromAvroSchema(Schema schema) {
        // Iterate over each table field and add them to the schema.
        Preconditions.checkState(!schema.getFields().isEmpty());

        DescriptorProto.Builder descriptorBuilder = DescriptorProto.newBuilder();
        // Create a unique name for the descriptor ('-' characters cannot be used).
        // Replace with "_" and prepend "D".
        descriptorBuilder.setName(BigQueryUtils.bqSanitizedRandomUUIDForDescriptor());
        int i = 1;
        for (Schema.Field field : schema.getFields()) {
            fieldDescriptorFromSchemaField(field, i++, descriptorBuilder);
        }
        return descriptorBuilder.build();
    }

    /**
     * Constructor for the Serializer.
     *
     * @param tableSchema Table Schema for the Sink Table ({@link
     *     com.google.api.services.bigquery.model.TableSchema} object )
     */
    public AvroToProtoSerializer(com.google.api.services.bigquery.model.TableSchema tableSchema)
            throws Descriptors.DescriptorValidationException {
        Schema avroSchema = getAvroSchema(tableSchema);
        this.descriptorProto = getDescriptorSchemaFromAvroSchema(avroSchema);
        this.descriptor = BigQueryProtoSerializer.getDescriptorFromDescriptorProto(descriptorProto);
    }

    /**
     * Constructor for the Serializer.
     *
     * @param avroSchema Table Schema for the Sink Table ({@link Schema} object )
     */
    public AvroToProtoSerializer(Schema avroSchema)
            throws Descriptors.DescriptorValidationException {
        this.descriptorProto = getDescriptorSchemaFromAvroSchema(avroSchema);
        this.descriptor = BigQueryProtoSerializer.getDescriptorFromDescriptorProto(descriptorProto);
    }

    @Override
    public ByteString serialize(GenericRecord record) throws BigQuerySerializationException {
        return getDynamicMessageFromGenericRecord(record, this.descriptor).toByteString();
    }

    @Override
    public DescriptorProto getDescriptorProto() {
        return this.descriptorProto;
    }

    /**
     * Function to convert a Generic Avro Record to Dynamic Message to write using the Storage Write
     * API.
     *
     * @param element {@link GenericRecord} Object to convert to {@link DynamicMessage}
     * @param descriptor {@link Descriptors.Descriptor} describing the schema of the sink table.
     * @return {@link DynamicMessage} Object converted from the Generic Avro Record.
     */
    public static DynamicMessage getDynamicMessageFromGenericRecord(
            GenericRecord element, Descriptors.Descriptor descriptor) {
        Schema schema = element.getSchema();
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);
        // Get the record's schema and find the field descriptor for each field one by one.
        for (Schema.Field field : schema.getFields()) {
            // In case no field descriptor exists for the field, throw an error as we have
            // incompatible schemas.
            Descriptors.FieldDescriptor fieldDescriptor =
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
     */
    private static Object toProtoValue(
            Descriptors.FieldDescriptor fieldDescriptor, Schema avroSchema, Object value) {
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
                if (arrayElementType == null) {
                    throw new IllegalArgumentException("Unexpected null element type!");
                }
                // Convert each value one by one.
                return StreamSupport.stream(iterable.spliterator(), false)
                        .map(v -> toProtoValue(fieldDescriptor, arrayElementType, v))
                        .collect(Collectors.toList());
            case UNION:
                Schema type = handleUnionSchema(avroSchema);
                // Get the schema of the field.
                return toProtoValue(fieldDescriptor, type, value);
            case MAP:
                return new UnsupportedOperationException("Not supported yet");
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
        mapping.put(LogicalTypes.date().getName(), AvroToProtoSerializerUtils::convertDate);
        mapping.put(
                LogicalTypes.decimal(1).getName(),
                value -> AvroToProtoSerializerUtils.convertDecimal(LogicalTypes.decimal(1), value));
        mapping.put(
                LogicalTypes.timestampMicros().getName(),
                value -> AvroToProtoSerializerUtils.convertTimestamp(value, true, "Timestamp(micros/millis)"));
        mapping.put(
                LogicalTypes.timestampMillis().getName(),
                value -> AvroToProtoSerializerUtils.convertTimestamp(value, false, "Timestamp(micros/millis)"));
        mapping.put(LogicalTypes.uuid().getName(), AvroToProtoSerializerUtils::convertUUID);
        mapping.put(
                LogicalTypes.timeMillis().getName(),
                value -> AvroToProtoSerializerUtils.convertTime(value, false, "Time(micros/millis)"));
        mapping.put(
                LogicalTypes.timeMicros().getName(),
                value -> AvroToProtoSerializerUtils.convertTime(value, true, "Time(micros/millis)"));
        mapping.put(
                LogicalTypes.localTimestampMillis().getName(),
                value -> AvroToProtoSerializerUtils.convertDateTime(value, false, "Local Timestamp(micros/millis)"));
        mapping.put(
                LogicalTypes.localTimestampMicros().getName(),
                value -> AvroToProtoSerializerUtils.convertDateTime(value, true, "Local Timestamp(micros/millis)"));
        mapping.put("geography_wkt", AvroToProtoSerializerUtils::convertGeography);
        mapping.put("Json", AvroToProtoSerializerUtils::convertJson);
        return mapping.get(logicalTypeString);
    }
}
