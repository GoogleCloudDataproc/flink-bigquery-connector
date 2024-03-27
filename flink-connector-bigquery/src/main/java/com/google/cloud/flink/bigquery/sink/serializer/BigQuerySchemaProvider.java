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
import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;
import com.google.cloud.flink.bigquery.common.exceptions.BigQueryConnectorException;
import com.google.cloud.flink.bigquery.common.utils.SchemaTransform;
import com.google.cloud.flink.bigquery.services.BigQueryServices.QueryDataClient;
import com.google.cloud.flink.bigquery.services.BigQueryServicesFactory;
import com.google.cloud.flink.bigquery.services.BigQueryUtils;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FileDescriptor;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.commons.lang3.tuple.ImmutablePair;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Class to obtain Descriptor ({@link Descriptor}) required for serialization of Generic Records.
 * This class has essential methods to obtain bigQuery Table Schema ({@link TableSchema}) from the
 * provided Table Connection Options ({@link BigQueryConnectOptions}). It is also responsible for
 * converting this Table Schema to Avro Schema ({@link Schema}) which is used to further convert to
 * DescriptorProto({@link DescriptorProto}). The obtained Descriptor Proto is required for the
 * formation of the Descriptors which is essential for Proto Rows formation.
 */
public class BigQuerySchemaProvider implements Serializable {

    private Schema avroSchema;
    private DescriptorProto descriptorProto;

    private static final Map<Schema.Type, FieldDescriptorProto.Type> AVRO_TYPES_TO_PROTO;
    private static final Map<String, FieldDescriptorProto.Type> LOGICAL_AVRO_TYPES_TO_PROTO;

    public DescriptorProto getDescriptorProto() {
        return descriptorProto;
    }

    public Descriptor getDescriptor() {
        Preconditions.checkNotNull(
                getDescriptorProto(),
                "DescriptorProto not initialized before obtaining Descriptor!");
        try {
            return getDescriptorFromDescriptorProto(descriptorProto);
        } catch (DescriptorValidationException e) {
            throw new BigQueryConnectorException(
                    "Error obtaining Descriptor from Descriptor Proto", e.getCause());
        }
    }

    public Schema getSchema() {
        return this.avroSchema;
    }

    public BigQuerySchemaProvider(BigQueryConnectOptions connectOptions) {
        this.getBigQuerySchemaProvider(connectOptions);
    }

    public BigQuerySchemaProvider(TableSchema avroSchema) {
        this.getBigQuerySchemaProvider(avroSchema);
    }

    public BigQuerySchemaProvider(Schema avroSchema) {
        this.getBigQuerySchemaProvider(avroSchema);
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
    public static ImmutablePair<Schema, Boolean> handleUnionSchema(Schema schema) {
        Schema elementType = schema;
        boolean isNullable = true;
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
            if (nonNullSchemaTypesSize == types.size()) {
                // Case, when there is only a single type in UNION.
                // Then it is essentially the same as not having a UNION.
                isNullable = false;
            }
            return new ImmutablePair<>(elementType, isNullable);
        } else {
            throw new IllegalArgumentException("Multiple non-null union types are not supported.");
        }
    }

    // ----------- Initialise Maps between Avro Schema to Descriptor Proto schema -------------
    static {
        /*
         * Map Avro Schema Type to FieldDescriptorProto Type which converts AvroSchema
         * Primitive Type to Dynamic Message.
         * AVRO_TYPES_TO_PROTO: containing mapping from Primitive Avro Schema Type to FieldDescriptorProto.
         */
        AVRO_TYPES_TO_PROTO = new EnumMap<>(Schema.Type.class);
        AVRO_TYPES_TO_PROTO.put(Schema.Type.INT, FieldDescriptorProto.Type.TYPE_INT32);
        AVRO_TYPES_TO_PROTO.put(Schema.Type.FIXED, FieldDescriptorProto.Type.TYPE_BYTES);
        AVRO_TYPES_TO_PROTO.put(Schema.Type.LONG, FieldDescriptorProto.Type.TYPE_INT64);
        AVRO_TYPES_TO_PROTO.put(Schema.Type.FLOAT, FieldDescriptorProto.Type.TYPE_FLOAT);
        AVRO_TYPES_TO_PROTO.put(Schema.Type.DOUBLE, FieldDescriptorProto.Type.TYPE_DOUBLE);
        AVRO_TYPES_TO_PROTO.put(Schema.Type.STRING, FieldDescriptorProto.Type.TYPE_STRING);
        AVRO_TYPES_TO_PROTO.put(Schema.Type.BOOLEAN, FieldDescriptorProto.Type.TYPE_BOOL);
        AVRO_TYPES_TO_PROTO.put(Schema.Type.ENUM, FieldDescriptorProto.Type.TYPE_STRING);
        AVRO_TYPES_TO_PROTO.put(Schema.Type.BYTES, FieldDescriptorProto.Type.TYPE_BYTES);

        /*
         * Map Logical Avro Schema Type to FieldDescriptorProto Type, which converts
         * AvroSchema Primitive Type to Dynamic Message.
         * LOGICAL_AVRO_TYPES_TO_PROTO: Map containing mapping from Primitive Avro Schema Type to FieldDescriptorProto.
         */
        LOGICAL_AVRO_TYPES_TO_PROTO = new HashMap<>();
        LOGICAL_AVRO_TYPES_TO_PROTO.put(
                LogicalTypes.date().getName(), FieldDescriptorProto.Type.TYPE_INT32);
        LOGICAL_AVRO_TYPES_TO_PROTO.put(
                LogicalTypes.decimal(1).getName(), FieldDescriptorProto.Type.TYPE_BYTES);
        LOGICAL_AVRO_TYPES_TO_PROTO.put(
                LogicalTypes.timestampMicros().getName(), FieldDescriptorProto.Type.TYPE_INT64);
        LOGICAL_AVRO_TYPES_TO_PROTO.put(
                LogicalTypes.timestampMillis().getName(), FieldDescriptorProto.Type.TYPE_INT64);
        LOGICAL_AVRO_TYPES_TO_PROTO.put(
                LogicalTypes.uuid().getName(), FieldDescriptorProto.Type.TYPE_STRING);
        // These are newly added.
        LOGICAL_AVRO_TYPES_TO_PROTO.put(
                LogicalTypes.timeMillis().getName(), FieldDescriptorProto.Type.TYPE_STRING);
        LOGICAL_AVRO_TYPES_TO_PROTO.put(
                LogicalTypes.timeMicros().getName(), FieldDescriptorProto.Type.TYPE_STRING);
        LOGICAL_AVRO_TYPES_TO_PROTO.put(
                LogicalTypes.localTimestampMillis().getName(),
                FieldDescriptorProto.Type.TYPE_STRING);
        LOGICAL_AVRO_TYPES_TO_PROTO.put(
                LogicalTypes.localTimestampMicros().getName(),
                FieldDescriptorProto.Type.TYPE_STRING);
        LOGICAL_AVRO_TYPES_TO_PROTO.put("geography_wkt", FieldDescriptorProto.Type.TYPE_STRING);
        LOGICAL_AVRO_TYPES_TO_PROTO.put("Json", FieldDescriptorProto.Type.TYPE_STRING);
    }

    // ---------------  Factory Methods ---------------------

    /**
     * Factory Method for the BigQuerySchemaProvider.
     *
     * @param connectOptions BigQueryConnectOptions for the Sink Table ({@link
     *     BigQueryConnectOptions} object)
     */
    private BigQuerySchemaProvider getBigQuerySchemaProvider(
            BigQueryConnectOptions connectOptions) {
        QueryDataClient queryDataClient =
                BigQueryServicesFactory.instance(connectOptions).queryClient();
        TableSchema tableSchema =
                queryDataClient.getTableSchema(
                        connectOptions.getProjectId(),
                        connectOptions.getDataset(),
                        connectOptions.getTable());
        return this.getBigQuerySchemaProvider(tableSchema);
    }

    /**
     * Factory Method for the BigQuerySchemaProvider.
     *
     * @param tableSchema Table Schema for the Sink Table ({@link TableSchema} object)
     */
    private BigQuerySchemaProvider getBigQuerySchemaProvider(TableSchema tableSchema) {
        this.avroSchema = getAvroSchema(tableSchema);
        return this.getBigQuerySchemaProvider(avroSchema);
    }

    /**
     * Factory Method for the BigQuerySchemaProvider.
     *
     * @param avroSchema Table Schema for the Sink Table ({@link Schema} object)
     */
    private BigQuerySchemaProvider getBigQuerySchemaProvider(Schema avroSchema) {
        this.descriptorProto = getDescriptorSchemaFromAvroSchema(avroSchema);
        return this;
    }

    // --------------- Obtain AvroSchema from TableSchema -----------------
    /**
     * Function to convert TableSchema to Avro Schema.
     *
     * @param tableSchema A {@link TableSchema} object to cast to {@link Schema}
     * @return Converted Avro Schema
     */
    private static Schema getAvroSchema(TableSchema tableSchema) {
        return SchemaTransform.toGenericAvroSchema("root", tableSchema.getFields());
    }

    // --------------- Obtain Descriptor Proto from Avro Schema  ---------------
    /**
     * Obtains a Descriptor Proto by obtaining Descriptor Proto field by field.
     *
     * <p>Iterates over Avro Schema to obtain FieldDescriptorProto for it.
     *
     * @param schema Avro Schema, for which descriptor is needed.
     * @return DescriptorProto describing the Schema.
     */
    private static DescriptorProto getDescriptorSchemaFromAvroSchema(Schema schema) {
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
                ImmutablePair<Schema, Boolean> handleUnionSchemaResult = handleUnionSchema(schema);
                elementType = handleUnionSchemaResult.getLeft();
                // either the field is nullable or error would be thrown.
                isNullable = handleUnionSchemaResult.getRight();
                if (elementType == null) {
                    throw new IllegalArgumentException("Unexpected null element type!");
                }
                if (isNullable
                        && (elementType.getType() == Schema.Type.MAP
                                || elementType.getType() == Schema.Type.ARRAY)) {
                    throw new UnsupportedOperationException(
                            "NULLABLE MAP/ARRAYS in UNION types are not supported");
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

    // --------------- Obtain Descriptor from DescriptorProto  ---------------
    /**
     * Function to convert the {@link DescriptorProto} Type to {@link Descriptor}.This is necessary
     * as a Descriptor is needed for DynamicMessage (used to write to Storage API).
     *
     * @param descriptorProto input which needs to be converted to a Descriptor.
     * @return Descriptor obtained form the input DescriptorProto
     * @throws DescriptorValidationException in case the conversion is not possible.
     */
    private static Descriptor getDescriptorFromDescriptorProto(DescriptorProto descriptorProto)
            throws DescriptorValidationException {
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
    }
}
