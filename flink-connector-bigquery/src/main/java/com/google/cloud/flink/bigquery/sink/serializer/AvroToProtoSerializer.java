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
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.flink.bigquery.common.utils.SchemaTransform;
import com.google.cloud.flink.bigquery.services.BigQueryUtils;
import com.google.cloud.flink.bigquery.sink.exceptions.BigQuerySerializationException;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import javax.annotation.Nullable;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/** Serializer for converting Avro's {@link GenericRecord} to BigQuery proto. */
public class AvroToProtoSerializer implements BigQueryProtoSerializer<GenericRecord> {

    private final DescriptorProto descriptorProto;
    private final Descriptor descriptor;

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
     * Function to convert TableSchema to Avro Schema.
     *
     * @param tableSchema A {@link TableSchema} object to cast to {@link Schema}
     * @return Converted Avro Schema
     */
    private Schema getAvroSchema(TableSchema tableSchema) {
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
                throw new UnsupportedOperationException("ARRAY type not supported yet.");
            case MAP:
                throw new UnsupportedOperationException("MAP type not supported yet.");
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
     * @param tableSchema Table Schema for the Sink Table ({@link TableSchema} object )
     */
    public AvroToProtoSerializer(TableSchema tableSchema) throws DescriptorValidationException {
        Schema avroSchema = getAvroSchema(tableSchema);
        this.descriptorProto = getDescriptorSchemaFromAvroSchema(avroSchema);
        this.descriptor = BigQueryProtoSerializer.getDescriptorFromDescriptorProto(descriptorProto);
    }

    /**
     * Constructor for the Serializer.
     *
     * @param avroSchema Table Schema for the Sink Table ({@link Schema} object )
     */
    public AvroToProtoSerializer(Schema avroSchema) throws DescriptorValidationException {
        this.descriptorProto = getDescriptorSchemaFromAvroSchema(avroSchema);
        this.descriptor = BigQueryProtoSerializer.getDescriptorFromDescriptorProto(descriptorProto);
    }

    @Override
    public ByteString serialize(GenericRecord record) throws BigQuerySerializationException {
        throw new UnsupportedOperationException("serialize method is not supported");
    }

    @Override
    public DescriptorProto getDescriptorProto() {
        return this.descriptorProto;
    }

    @Override
    public Descriptor getDescriptor() {
        return this.descriptor;
    }
}
