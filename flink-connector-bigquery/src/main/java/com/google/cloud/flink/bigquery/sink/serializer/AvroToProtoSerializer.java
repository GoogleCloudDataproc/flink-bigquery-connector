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

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableMap;

import com.google.api.client.util.Preconditions;
import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.cloud.flink.bigquery.common.utils.SchemaTransform;
import com.google.cloud.flink.bigquery.services.BigQueryUtils;
import com.google.cloud.flink.bigquery.sink.exceptions.BigQuerySerializationException;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

/** Serializer for converting Avro's {@link GenericRecord} to BigQuery proto. */
public class AvroToProtoSerializer implements BigQueryProtoSerializer<GenericRecord> {

    private final DescriptorProto descriptorProto;

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
        mapping.put(Schema.Type.FIXED, FieldDescriptorProto.Type.TYPE_FIXED64);
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
        mapping.put(LogicalTypes.timeMillis().getName(), FieldDescriptorProto.Type.TYPE_INT64);
        mapping.put(LogicalTypes.timeMicros().getName(), FieldDescriptorProto.Type.TYPE_INT64);
        mapping.put(
                LogicalTypes.localTimestampMicros().getName(),
                FieldDescriptorProto.Type.TYPE_INT64);
        mapping.put(
                LogicalTypes.localTimestampMicros().getName(),
                FieldDescriptorProto.Type.TYPE_INT64);
        mapping.put("geography_wkt", FieldDescriptorProto.Type.TYPE_STRING);
        mapping.put("{range, DATE}", FieldDescriptorProto.Type.TYPE_STRING);
        mapping.put("{range, TIME}", FieldDescriptorProto.Type.TYPE_STRING);
        mapping.put("{range, TIMESTAMP}", FieldDescriptorProto.Type.TYPE_STRING);
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
     * datatype2, ...], and [datatype1, datatype2, ...] are considered as invalid (as there is no
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
                DescriptorProto.Builder arrayFieldBuilder = DescriptorProto.newBuilder();
                fieldDescriptorFromSchemaField(
                        new Schema.Field(
                                field.name(), elementType, field.doc(), field.defaultVal()),
                        fieldNumber,
                        arrayFieldBuilder);
                fieldDescriptorBuilder =
                        arrayFieldBuilder
                                .getFieldBuilder(0)
                                .setLabel(FieldDescriptorProto.Label.LABEL_REPEATED);
                // Add any nested types.
                descriptorProtoBuilder.addAllNestedType(arrayFieldBuilder.getNestedTypeList());
                break;
            case MAP:
                // Map is converted to an array of structs.
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
                fieldDescriptorBuilder =
                        mapFieldBuilder
                                .getFieldBuilder(0)
                                .setLabel(FieldDescriptorProto.Label.LABEL_REPEATED);

                // Add the nested types.
                descriptorProtoBuilder.addAllNestedType(mapFieldBuilder.getNestedTypeList());
                break;
            case UNION:
                // Types can be ["null"] - Not supported in BigQuery.
                // ["null", something]  - Bigquery Field of type something with mode NULLABLE
                // ["null", something1, something2], [something1, something2] - Are invalid types
                // not supported in BQ
                elementType = handleUnionSchema(schema);
                // either the field is nullable or error would be thrown.
                isNullable = true;
                if (elementType == null) {
                    throw new IllegalArgumentException("Unexpected null element type!");
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
        }
        // Set the Labels for different Modes - REPEATED, REQUIRED, NULLABLE.
        if (fieldDescriptorBuilder.getLabel() != FieldDescriptorProto.Label.LABEL_REPEATED) {
            if (isNullable) {
                fieldDescriptorBuilder =
                        fieldDescriptorBuilder.setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL);
            } else {
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
     * @param schema Avro Schema for which descriptor is needed.
     * @return DescriptorProto describing the Schema.
     */
    public static DescriptorProto getDescriptorSchemaFromAvroSchema(Schema schema) {
        // Iterate over each table fields and add them to schema.
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
    public AvroToProtoSerializer(com.google.api.services.bigquery.model.TableSchema tableSchema) {
        Schema avroSchema = getAvroSchema(tableSchema);
        this.descriptorProto = getDescriptorSchemaFromAvroSchema(avroSchema);
    }

    public AvroToProtoSerializer() {}

    @Override
    public ByteString serialize(GenericRecord record) throws BigQuerySerializationException {
        throw new UnsupportedOperationException("serialize method is not supported");
    }

    @Override
    public DescriptorProto getDescriptorProto() {
        return this.descriptorProto;
    }

    /**
     * DELETE EVERYTHING BELOW AND INCLUDING THIS COMMENT.
     * ------------------------------------------------------------------------ TODO: DELETE
     * EVERYTHING BELOW AND INCLUDING THIS COMMENT
     */
    //  ------------------------------------------------------------------------
    public static DescriptorProto getBeamResult(Schema avroSchema) {
        TableSchema protoTableSchema = protoTableSchemaFromAvroSchema(avroSchema);

        return descriptorSchemaFromTableFieldSchemas(protoTableSchema.getFieldsList());
    }

    /**
     * Given an Avro Schema, returns a protocol-buffer TableSchema that can be used to write data
     * through BigQuery Storage API.
     *
     * @param schema An Avro Schema
     * @return Returns the TableSchema created from the provided Schema
     */
    public static TableSchema protoTableSchemaFromAvroSchema(Schema schema) {
        Preconditions.checkState(!schema.getFields().isEmpty());

        TableSchema.Builder builder = TableSchema.newBuilder();
        for (Schema.Field field : schema.getFields()) {
            builder.addFields(fieldDescriptorFromAvroField(field));
        }
        return builder.build();
    }

    private static TableFieldSchema fieldDescriptorFromAvroField(Schema.Field field) {
        @Nullable Schema schema = field.schema();
        Preconditions.checkNotNull(schema, "Unexpected null schema!");
        TableFieldSchema.Builder builder =
                TableFieldSchema.newBuilder().setName(field.name().toLowerCase());
        Schema elementType = null;
        switch (schema.getType()) {
            case RECORD:
                Preconditions.checkState(!schema.getFields().isEmpty());
                builder = builder.setType(TableFieldSchema.Type.STRUCT);
                for (Schema.Field recordField : schema.getFields()) {
                    builder = builder.addFields(fieldDescriptorFromAvroField(recordField));
                }
                break;
            case ARRAY:
                elementType = TypeWithNullability.create(schema.getElementType()).getType();
                if (elementType == null) {
                    throw new RuntimeException("Unexpected null element type!");
                }
                Preconditions.checkState(
                        elementType.getType() != Schema.Type.ARRAY,
                        "Nested arrays not supported by BigQuery.");

                TableFieldSchema elementFieldSchema =
                        fieldDescriptorFromAvroField(
                                new Schema.Field(
                                        field.name(),
                                        elementType,
                                        field.doc(),
                                        field.defaultVal()));
                builder = builder.setType(elementFieldSchema.getType());
                builder.addAllFields(elementFieldSchema.getFieldsList());
                builder = builder.setMode(TableFieldSchema.Mode.REPEATED);
                break;
            case MAP:
                Schema keyType = Schema.create(Schema.Type.STRING);
                Schema valueType = TypeWithNullability.create(schema.getElementType()).getType();
                if (valueType == null) {
                    throw new RuntimeException("Unexpected null element type!");
                }
                TableFieldSchema keyFieldSchema =
                        fieldDescriptorFromAvroField(
                                new Schema.Field(
                                        "key",
                                        keyType,
                                        "key of the map entry",
                                        Schema.Field.NULL_VALUE));
                TableFieldSchema valueFieldSchema =
                        fieldDescriptorFromAvroField(
                                new Schema.Field(
                                        "value",
                                        valueType,
                                        "value of the map entry",
                                        Schema.Field.NULL_VALUE));
                builder =
                        builder.setType(TableFieldSchema.Type.STRUCT)
                                .addFields(keyFieldSchema)
                                .addFields(valueFieldSchema)
                                .setMode(TableFieldSchema.Mode.REPEATED);
                break;
            case UNION:
                elementType = TypeWithNullability.create(schema).getType();
                if (elementType == null) {
                    throw new RuntimeException("Unexpected null element type!");
                }
                // check to see if more than one non-null type is defined in the union
                Preconditions.checkState(
                        elementType.getType() != Schema.Type.UNION,
                        "Multiple non-null union types are not supported.");
                TableFieldSchema unionFieldSchema =
                        fieldDescriptorFromAvroField(
                                new Schema.Field(
                                        field.name(),
                                        elementType,
                                        field.doc(),
                                        field.defaultVal()));
                builder =
                        builder.setType(unionFieldSchema.getType())
                                .addAllFields(unionFieldSchema.getFieldsList());
                break;
            default:
                elementType = TypeWithNullability.create(schema).getType();
                @Nullable
                TableFieldSchema.Type primitiveType =
                        Optional.ofNullable(LogicalTypes.fromSchema(elementType))
                                .map(logicalType -> LOGICAL_TYPES.get(logicalType.getName()))
                                .orElse(PRIMITIVE_TYPES.get(elementType.getType()));
                if (primitiveType == null) {
                    throw new RuntimeException("Unsupported type " + elementType.getType());
                }
                // a scalar will be required by default, if defined as part of union then
                // caller will set nullability requirements
                builder = builder.setType(primitiveType);
        }
        if (builder.getMode() != TableFieldSchema.Mode.REPEATED) {
            if (TypeWithNullability.create(schema).isNullable()) {
                builder = builder.setMode(TableFieldSchema.Mode.NULLABLE);
            } else {
                builder = builder.setMode(TableFieldSchema.Mode.REQUIRED);
            }
        }
        if (field.doc() != null) {
            builder = builder.setDescription(field.doc());
        }
        return builder.build();
    }

    /** javadoc. */
    public static class TypeWithNullability {
        public final org.apache.avro.Schema type;
        public final boolean nullable;

        public static TypeWithNullability create(org.apache.avro.Schema avroSchema) {
            return new TypeWithNullability(avroSchema);
        }

        TypeWithNullability(org.apache.avro.Schema avroSchema) {
            if (avroSchema.getType() == Schema.Type.UNION) {
                List<org.apache.avro.Schema> types = avroSchema.getTypes();

                // optional fields in AVRO have form of:
                // {"name": "foo", "type": ["null", "something"]}

                // don't need recursion because nested unions aren't supported in AVRO
                // Extract all the nonNull Datatypes.
                List<org.apache.avro.Schema> nonNullTypes =
                        types.stream()
                                .filter(x -> x.getType() != Schema.Type.NULL)
                                .collect(Collectors.toList());

                if (nonNullTypes.size() == types.size() || nonNullTypes.isEmpty()) {
                    // union without `null` or all 'null' union, keep as is.
                    // If all the allowed fields types are null/not null
                    // Keep the type as it is.
                    type = avroSchema;
                    nullable = false;
                } else if (nonNullTypes.size() > 1) {
                    //
                    type = org.apache.avro.Schema.createUnion(nonNullTypes);
                    nullable = true;
                } else {
                    // One non-null type.
                    type = nonNullTypes.get(0);
                    nullable = true;
                }
            } else {
                type = avroSchema;
                nullable = false;
            }
        }

        public Boolean isNullable() {
            return nullable;
        }

        public org.apache.avro.Schema getType() {
            return type;
        }
    }

    static final Map<String, TableFieldSchema.Type> LOGICAL_TYPES =
            ImmutableMap.<String, TableFieldSchema.Type>builder()
                    .put(LogicalTypes.date().getName(), TableFieldSchema.Type.DATE)
                    .put(LogicalTypes.decimal(1).getName(), TableFieldSchema.Type.BIGNUMERIC)
                    .put(LogicalTypes.timestampMicros().getName(), TableFieldSchema.Type.TIMESTAMP)
                    .put(LogicalTypes.timestampMillis().getName(), TableFieldSchema.Type.TIMESTAMP)
                    .put(LogicalTypes.uuid().getName(), TableFieldSchema.Type.STRING)
                    // These are newly added.
                    .put(LogicalTypes.timeMillis().getName(), TableFieldSchema.Type.TIME)
                    .put(LogicalTypes.timeMicros().getName(), TableFieldSchema.Type.TIME)
                    .build();

    static final Map<Schema.Type, TableFieldSchema.Type> PRIMITIVE_TYPES =
            ImmutableMap.<Schema.Type, TableFieldSchema.Type>builder()
                    // BQ has only INTEGER.
                    .put(Schema.Type.INT, TableFieldSchema.Type.INT64)
                    .put(Schema.Type.FIXED, TableFieldSchema.Type.BYTES)
                    .put(Schema.Type.LONG, TableFieldSchema.Type.INT64)
                    .put(Schema.Type.FLOAT, TableFieldSchema.Type.DOUBLE)
                    .put(Schema.Type.DOUBLE, TableFieldSchema.Type.DOUBLE)
                    .put(Schema.Type.STRING, TableFieldSchema.Type.STRING)
                    .put(Schema.Type.BOOLEAN, TableFieldSchema.Type.BOOL)
                    .put(Schema.Type.ENUM, TableFieldSchema.Type.STRING)
                    .put(Schema.Type.BYTES, TableFieldSchema.Type.BYTES)
                    .build();

    private static DescriptorProto descriptorSchemaFromTableFieldSchemas(
            Iterable<TableFieldSchema> tableFieldSchemas) {
        DescriptorProto.Builder descriptorBuilder = DescriptorProto.newBuilder();
        // Create a unique name for the descriptor ('-' characters cannot be used).
        descriptorBuilder.setName("D" + UUID.randomUUID().toString().replace("-", "_"));
        int i = 1;
        for (TableFieldSchema fieldSchema : tableFieldSchemas) {
            fieldDescriptorFromTableField(fieldSchema, i++, descriptorBuilder);
        }
        return descriptorBuilder.build();
    }

    private static void fieldDescriptorFromTableField(
            TableFieldSchema fieldSchema,
            int fieldNumber,
            DescriptorProto.Builder descriptorBuilder) {
        FieldDescriptorProto.Builder fieldDescriptorBuilder = FieldDescriptorProto.newBuilder();
        fieldDescriptorBuilder =
                fieldDescriptorBuilder.setName(fieldSchema.getName().toLowerCase());
        fieldDescriptorBuilder = fieldDescriptorBuilder.setNumber(fieldNumber);
        switch (fieldSchema.getType()) {
            case STRUCT:
                DescriptorProto nested =
                        descriptorSchemaFromTableFieldSchemas(fieldSchema.getFieldsList());
                descriptorBuilder.addNestedType(nested);
                fieldDescriptorBuilder =
                        fieldDescriptorBuilder
                                .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                                .setTypeName(nested.getName());
                break;
            default:
                @Nullable
                DescriptorProtos.FieldDescriptorProto.Type type =
                        PRIMITIVE_TYPES_BQ_TO_PROTO.get(fieldSchema.getType());
                if (type == null) {
                    throw new UnsupportedOperationException(
                            "Converting BigQuery type "
                                    + fieldSchema.getType()
                                    + " to Beam type is unsupported");
                }
                fieldDescriptorBuilder = fieldDescriptorBuilder.setType(type);
        }

        if (fieldSchema.getMode() == TableFieldSchema.Mode.REPEATED) {
            fieldDescriptorBuilder =
                    fieldDescriptorBuilder.setLabel(FieldDescriptorProto.Label.LABEL_REPEATED);
        } else if (fieldSchema.getMode() != TableFieldSchema.Mode.REQUIRED) {
            fieldDescriptorBuilder =
                    fieldDescriptorBuilder.setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL);
        } else {
            fieldDescriptorBuilder =
                    fieldDescriptorBuilder.setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED);
        }
        descriptorBuilder.addField(fieldDescriptorBuilder.build());
    }

    static final Map<TableFieldSchema.Type, FieldDescriptorProto.Type> PRIMITIVE_TYPES_BQ_TO_PROTO =
            ImmutableMap.<TableFieldSchema.Type, FieldDescriptorProto.Type>builder()
                    .put(TableFieldSchema.Type.INT64, FieldDescriptorProto.Type.TYPE_INT64)
                    .put(TableFieldSchema.Type.DOUBLE, FieldDescriptorProto.Type.TYPE_DOUBLE)
                    .put(TableFieldSchema.Type.STRING, FieldDescriptorProto.Type.TYPE_STRING)
                    .put(TableFieldSchema.Type.BOOL, FieldDescriptorProto.Type.TYPE_BOOL)
                    .put(TableFieldSchema.Type.BYTES, FieldDescriptorProto.Type.TYPE_BYTES)
                    .put(TableFieldSchema.Type.NUMERIC, FieldDescriptorProto.Type.TYPE_BYTES)
                    .put(TableFieldSchema.Type.BIGNUMERIC, FieldDescriptorProto.Type.TYPE_BYTES)
                    .put(
                            TableFieldSchema.Type.GEOGRAPHY,
                            FieldDescriptorProto.Type
                                    .TYPE_STRING) // Pass through the JSON encoding.
                    .put(TableFieldSchema.Type.DATE, FieldDescriptorProto.Type.TYPE_INT32)
                    .put(TableFieldSchema.Type.TIME, FieldDescriptorProto.Type.TYPE_INT64)
                    .put(TableFieldSchema.Type.DATETIME, FieldDescriptorProto.Type.TYPE_INT64)
                    .put(TableFieldSchema.Type.TIMESTAMP, FieldDescriptorProto.Type.TYPE_INT64)
                    .put(TableFieldSchema.Type.JSON, FieldDescriptorProto.Type.TYPE_STRING)
                    .build();
}

    // ------------------------------------------------------------------------
    //  ------------------------------------------------------------------------
