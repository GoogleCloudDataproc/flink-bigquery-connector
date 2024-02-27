package com.google.cloud.flink.bigquery.sink.serializer;

import org.apache.flink.shaded.guava30.com.google.common.collect.Iterables;

import com.google.api.client.util.Preconditions;
import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.cloud.flink.bigquery.common.utils.SchemaTransform;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.Descriptor;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

/** Javadoc. */
public class SerialiseAvroRecordsToStorageApiProtos extends SerialiseRecordsToStorageApiProto {

    static final Map<String, TableFieldSchema.Type> LOGICAL_TYPES =
            initializeLogicalAvroToTableFieldTypes();
    static final Map<Schema.Type, TableFieldSchema.Type> PRIMITIVE_TYPES =
            initializePrimitiveAvroToTableFieldTypes();

    static final Map<TableFieldSchema.Type, FieldDescriptorProto.Type> PRIMITIVE_TYPES_BQ_TO_PROTO =
            initializeProtoFieldToFieldDescriptorTypes();

    private static Map<TableFieldSchema.Type, FieldDescriptorProto.Type>
            initializeProtoFieldToFieldDescriptorTypes() {

        Map<TableFieldSchema.Type, FieldDescriptorProto.Type> mapping = new HashMap<>();

        mapping.put(TableFieldSchema.Type.INT64, FieldDescriptorProto.Type.TYPE_INT64);
        mapping.put(TableFieldSchema.Type.DOUBLE, FieldDescriptorProto.Type.TYPE_DOUBLE);
        mapping.put(TableFieldSchema.Type.STRING, FieldDescriptorProto.Type.TYPE_STRING);
        mapping.put(TableFieldSchema.Type.BOOL, FieldDescriptorProto.Type.TYPE_BOOL);
        mapping.put(TableFieldSchema.Type.BYTES, FieldDescriptorProto.Type.TYPE_BYTES);

        // Is this ever obtained??
        mapping.put(TableFieldSchema.Type.NUMERIC, FieldDescriptorProto.Type.TYPE_BYTES);
        mapping.put(TableFieldSchema.Type.JSON, FieldDescriptorProto.Type.TYPE_STRING);

        mapping.put(TableFieldSchema.Type.BIGNUMERIC, FieldDescriptorProto.Type.TYPE_BYTES);
        mapping.put(
                TableFieldSchema.Type.GEOGRAPHY,
                FieldDescriptorProto.Type.TYPE_STRING); // Pass through the JSON encoding.
        mapping.put(TableFieldSchema.Type.DATE, FieldDescriptorProto.Type.TYPE_INT32);
        mapping.put(TableFieldSchema.Type.TIME, FieldDescriptorProto.Type.TYPE_INT64);
        mapping.put(TableFieldSchema.Type.DATETIME, FieldDescriptorProto.Type.TYPE_INT64);
        mapping.put(TableFieldSchema.Type.TIMESTAMP, FieldDescriptorProto.Type.TYPE_INT64); // Check with String as Time-Zone has to be present.
        mapping.put(TableFieldSchema.Type.INTERVAL, FieldDescriptorProto.Type.TYPE_STRING);
        return mapping;
    }

    /**
     * Function to initialize the conversion Map which converts LogicalType to TableFieldSchema
     * Type.
     *
     * @return Hashmap containing the mapping for conversion.
     */
    private static Map<String, TableFieldSchema.Type> initializeLogicalAvroToTableFieldTypes() {
        Map<String, TableFieldSchema.Type> mapping = new HashMap<>();
        mapping.put(LogicalTypes.date().getName(), TableFieldSchema.Type.DATE);

        mapping.put(LogicalTypes.decimal(1).getName(), TableFieldSchema.Type.BIGNUMERIC);
        mapping.put(LogicalTypes.timestampMicros().getName(), TableFieldSchema.Type.TIMESTAMP);
        mapping.put(LogicalTypes.timestampMillis().getName(), TableFieldSchema.Type.TIMESTAMP);
        mapping.put(LogicalTypes.uuid().getName(), TableFieldSchema.Type.STRING);
        // These are newly added.
        mapping.put(LogicalTypes.timeMillis().getName(), TableFieldSchema.Type.TIME);
        mapping.put(LogicalTypes.timeMicros().getName(), TableFieldSchema.Type.TIME);
        mapping.put(LogicalTypes.localTimestampMicros().getName(), TableFieldSchema.Type.DATETIME);
        mapping.put(LogicalTypes.localTimestampMicros().getName(), TableFieldSchema.Type.DATETIME);
        mapping.put("geography_wkt", TableFieldSchema.Type.GEOGRAPHY);
        mapping.put("{range, DATE}", TableFieldSchema.Type.INTERVAL);
        mapping.put("{range, TIME}", TableFieldSchema.Type.INTERVAL);
        mapping.put("{range, TIMESTAMP}", TableFieldSchema.Type.INTERVAL);
        return mapping;
    }

    /**
     * Function to initialize the conversion Map which converts Primitive Datatypes to
     * TableFieldSchema Type.
     *
     * @return Hashmap containing the mapping for conversion.
     */
    private static Map<Schema.Type, TableFieldSchema.Type>
            initializePrimitiveAvroToTableFieldTypes() {
        Map<Schema.Type, TableFieldSchema.Type> mapping = new HashMap<>();
        mapping.put(Schema.Type.INT, TableFieldSchema.Type.INT64);
        mapping.put(Schema.Type.FIXED, TableFieldSchema.Type.BYTES);
        mapping.put(Schema.Type.LONG, TableFieldSchema.Type.INT64);
        mapping.put(Schema.Type.FLOAT, TableFieldSchema.Type.DOUBLE);
        mapping.put(Schema.Type.DOUBLE, TableFieldSchema.Type.DOUBLE);
        mapping.put(Schema.Type.STRING, TableFieldSchema.Type.STRING);
        mapping.put(Schema.Type.BOOLEAN, TableFieldSchema.Type.BOOL);
        mapping.put(Schema.Type.ENUM, TableFieldSchema.Type.STRING);
        mapping.put(Schema.Type.BYTES, TableFieldSchema.Type.BYTES);
        return mapping;
    }

    private Schema getAvroSchema(com.google.api.services.bigquery.model.TableSchema tableSchema) {

        return SchemaTransform.toGenericAvroSchema("root", tableSchema.getFields());
    }

    private Schema getAvroSchema(com.google.cloud.bigquery.Schema bigQuerySchema) {

        // Convert to Table Schema.
        // And then Avro Schema by calling getAvroSchema(TableSchema)
        return getAvroSchema(SchemaTransform.bigQuerySchemaToTableSchema(bigQuerySchema));
    }

    /**
     * Given an Avro Schema, returns a protocol-buffer TableSchema that can be used to write data
     * through BigQuery Storage API.
     *
     * @param schema An Avro Schema
     * @return Returns the TableSchema created from the provided Schema
     */
    public static TableSchema getProtoSchemaFromAvroSchema(Schema schema) {

        // Iterate over each table fields and add them to schema.
        Preconditions.checkState(!schema.getFields().isEmpty());

        TableSchema.Builder builder = TableSchema.newBuilder();
        for (Schema.Field field : schema.getFields()) {
            builder.addFields(getTableFieldFromAvroField(field));
        }
        return builder.build();
    }

    /**
     * Given an Avro Schema Field, returns a protocol-buffer TableFieldSchema that can be used to
     * create the TableSchema.
     *
     * @param field An Avro Schema Field
     * @return Returns the TableFieldSchema created from the provided Field
     */
    private static TableFieldSchema getTableFieldFromAvroField(Schema.Field field) {
        @Nullable Schema schema = field.schema();

        Preconditions.checkNotNull(schema, "Unexpected null schema!");
        TableFieldSchema.Builder builder =
                TableFieldSchema.newBuilder().setName(field.name().toLowerCase());
        Schema elementType = null;
        boolean isNullable = false;

        switch (schema.getType()) {
            case RECORD:
                elementType = schema;

                throw new UnsupportedOperationException("Operation is not supported yet.");
            case ARRAY:
                elementType = schema.getElementType();
                if (elementType == null) {
                    throw new RuntimeException("Unexpected null element type!");
                }
                Preconditions.checkState(
                        elementType.getType() != Schema.Type.ARRAY,
                        "Nested arrays not supported by BigQuery.");

                TableFieldSchema elementFieldSchema =
                        getTableFieldFromAvroField(
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
                Schema valueType = schema.getElementType();
                if (valueType == null) {
                    throw new RuntimeException("Unexpected null element type!");
                }
                TableFieldSchema keyFieldSchema =
                        getTableFieldFromAvroField(
                                new Schema.Field(
                                        "key", keyType, " Map entry key", Schema.Field.NULL_VALUE));
                TableFieldSchema valueFieldSchema =
                        getTableFieldFromAvroField(
                                new Schema.Field(
                                        "value",
                                        valueType,
                                        "Map entry value",
                                        Schema.Field.NULL_VALUE));
                builder =
                        builder.setType(TableFieldSchema.Type.STRUCT)
                                .addFields(keyFieldSchema)
                                .addFields(valueFieldSchema)
                                .setMode(TableFieldSchema.Mode.REPEATED);
                break;
            case UNION:
                // Types can be ["null"] - Not supported in BigQuery.
                // ["null", something]  - Bigquery Field of type something with mode NULLABLE
                // ["null", something1, something2], [something1, something2] - Are invalid types
                // not supported in BQ
                elementType = schema;
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
                    isNullable = true;
                } else {
                    throw new IllegalArgumentException(
                            "Multiple non-null union types are not supported.");
                }
                if (elementType == null) {
                    throw new RuntimeException("Unexpected null element type!");
                }
                TableFieldSchema unionFieldSchema =
                        getTableFieldFromAvroField(
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
                elementType = schema;
                // Handling of the Logical or Primitive Type.
                @Nullable
                TableFieldSchema.Type primitiveType =
                        Optional.ofNullable(elementType.getProp(LogicalType.LOGICAL_TYPE_PROP))
                                .map(LOGICAL_TYPES::get)
                                .orElse(PRIMITIVE_TYPES.get(elementType.getType()));
                if (primitiveType == null) {
                    throw new RuntimeException("Unsupported type " + elementType.getType());
                }
                builder = builder.setType(primitiveType);
        }
        if (builder.getMode() != TableFieldSchema.Mode.REPEATED) {
            if (isNullable) {
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

    public static Descriptor wrapDescriptorProto(DescriptorProto descriptorProto)
            throws Descriptors.DescriptorValidationException {
        FileDescriptorProto fileDescriptorProto =
                FileDescriptorProto.newBuilder().addMessageType(descriptorProto).build();
        Descriptors.FileDescriptor fileDescriptor =
                Descriptors.FileDescriptor.buildFrom(
                        fileDescriptorProto, new Descriptors.FileDescriptor[0]);

        return Iterables.getOnlyElement(fileDescriptor.getMessageTypes());
    }

    /**
     * Defines descriptor for the Table Schema.
     *
     * @param tableSchema for which descriptor needs to be defined.
     * @return Descriptor that describes the TableSchema
     * @throws Descriptors.DescriptorValidationException
     */
    public static Descriptor getDescriptorFromTableSchema(TableSchema tableSchema)
            throws Descriptors.DescriptorValidationException {
        return wrapDescriptorProto(descriptorSchemaFromTableSchema(tableSchema));
    }

    /**
     * Obtains a Descriptor Proto by obtaining Descriptor Proto field by field.
     *
     * @param tableSchema Table Schema for which descriptor is needed.
     * @return DescriptorProto describing the Schema.
     */
    static DescriptorProto descriptorSchemaFromTableSchema(TableSchema tableSchema) {
        return descriptorSchemaFromTableFieldSchemas(tableSchema.getFieldsList());
    }

    /**
     * Obtains a Descriptor Proto by obtaining Descriptor Proto field by field.
     *
     * <p>Iterates over TableSchemaField to obtain FieldDescriptorProto for it.
     *
     * @param tableFieldSchemas List of Table Schema Fields for which descriptor is needed.
     * @return DescriptorProto describing the Schema.
     */
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
                throw new UnsupportedOperationException("Operation is not supported yet.");
            default:
                @Nullable
                FieldDescriptorProto.Type type =
                        PRIMITIVE_TYPES_BQ_TO_PROTO.get(fieldSchema.getType());
                if (type == null) {
                    throw new UnsupportedOperationException(
                            "Converting BigQuery type "
                                    + fieldSchema.getType()
                                    + " to Storage API Proto type is unsupported");
                }
                fieldDescriptorBuilder = fieldDescriptorBuilder.setType(type);
        }

        // Set the Labels for different Modes - REPEATED, REQUIRED, NULLABLE.
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

    /** Uncomment for Approach 2. */

    //    public SerialiseAvroRecordsToStorageApiProtos(com.google.cloud.bigquery.Schema schema) {
    //        Schema avroSchema = getAvroSchema(schema);
    //
    //        TableSchema protoTableSchema = getProtoSchemaFromAvroSchema(avroSchema);
    //    }

    public SerialiseAvroRecordsToStorageApiProtos(
            com.google.api.services.bigquery.model.TableSchema tableSchema) {

        Schema avroSchema = getAvroSchema(tableSchema);

        TableSchema protoTableSchema = getProtoSchemaFromAvroSchema(avroSchema);

        // TODO: 3. Proto Table Schema -> Descriptor.
        //        Descriptors.Descriptor descriptor =
        // getDescriptorFromTableSchema(protoTableSchema);

        // TODO: 5. .toMessage() {NOT IN THE CONSTRUCTOR}
        // messageFromGenericRecord(descriptor, record)
        // Check if descriptor has the same fields as that in the record.
        // messageValueFromGenericRecordValue() -> toProtoValue()

    }
}
