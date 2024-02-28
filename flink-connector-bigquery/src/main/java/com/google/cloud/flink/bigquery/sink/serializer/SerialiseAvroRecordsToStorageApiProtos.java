package com.google.cloud.flink.bigquery.sink.serializer;

import org.apache.flink.shaded.guava30.com.google.common.collect.Iterables;

import com.google.api.client.util.Preconditions;
import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.cloud.flink.bigquery.common.utils.SchemaTransform;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.DynamicMessage;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.ImmutablePair;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/** Javadoc. */
public class SerialiseAvroRecordsToStorageApiProtos extends SerialiseRecordsToStorageApiProto {

    private static final Map<String, TableFieldSchema.Type> LOGICAL_TYPES =
            initializeLogicalAvroToTableFieldTypes();
    private static final Map<Schema.Type, TableFieldSchema.Type> PRIMITIVE_TYPES =
            initializePrimitiveAvroToTableFieldTypes();

    private static final Map<TableFieldSchema.Type, FieldDescriptorProto.Type>
            PRIMITIVE_TYPES_BQ_TO_PROTO = initializeProtoFieldToFieldDescriptorTypes();

    private static final Map<Schema.Type, Function<Object, Object>> PRIMITIVE_ENCODERS =
            initalizePrimitiveEncoderFunction();

    /**
     * Function to initialize the conversion Map which converts TableFieldSchema to
     * FieldDescriptorProto Type.
     *
     * @return Hashmap containing the mapping for conversion.
     */
    private static Map<TableFieldSchema.Type, FieldDescriptorProto.Type>
            initializeProtoFieldToFieldDescriptorTypes() {

        Map<TableFieldSchema.Type, FieldDescriptorProto.Type> mapping = new HashMap<>();

        mapping.put(TableFieldSchema.Type.INT64, FieldDescriptorProto.Type.TYPE_INT64);
        mapping.put(TableFieldSchema.Type.DOUBLE, FieldDescriptorProto.Type.TYPE_DOUBLE);
        mapping.put(TableFieldSchema.Type.STRING, FieldDescriptorProto.Type.TYPE_STRING);
        mapping.put(TableFieldSchema.Type.BOOL, FieldDescriptorProto.Type.TYPE_BOOL);
        mapping.put(TableFieldSchema.Type.BYTES, FieldDescriptorProto.Type.TYPE_BYTES);

        mapping.put(TableFieldSchema.Type.DATE, FieldDescriptorProto.Type.TYPE_INT32);
        mapping.put(TableFieldSchema.Type.TIME, FieldDescriptorProto.Type.TYPE_INT64);
        mapping.put(TableFieldSchema.Type.DATETIME, FieldDescriptorProto.Type.TYPE_INT64);
        mapping.put(
                TableFieldSchema.Type.TIMESTAMP,
                FieldDescriptorProto.Type
                        .TYPE_INT64); // Check with String as Time-Zone has to be present.

        // Are these ever obtained??
        mapping.put(TableFieldSchema.Type.NUMERIC, FieldDescriptorProto.Type.TYPE_BYTES);
        mapping.put(TableFieldSchema.Type.JSON, FieldDescriptorProto.Type.TYPE_STRING);

        mapping.put(TableFieldSchema.Type.BIGNUMERIC, FieldDescriptorProto.Type.TYPE_BYTES);
        mapping.put(
                TableFieldSchema.Type.GEOGRAPHY,
                FieldDescriptorProto.Type.TYPE_STRING); // Pass through the JSON encoding.
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
        // Is this ever obtained ?
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

    /**
     * Function to map Avro Schema Type to an Encoding function which converts AvroSchema Primitive
     * Type to Dynamic Message.
     *
     * @return Map containing mapping from Primitive Avro Schema Type with encoder function.
     */
    private static Map<Schema.Type, Function<Object, Object>> initalizePrimitiveEncoderFunction() {

        Map<Schema.Type, Function<Object, Object>> mapping = new HashMap<>();
        mapping.put(Schema.Type.INT, o -> Long.valueOf((int) o));
        mapping.put(Schema.Type.FIXED, o -> ByteString.copyFrom(((GenericData.Fixed) o).bytes()));
        mapping.put(Schema.Type.LONG, Function.identity());
        mapping.put(
                Schema.Type.FLOAT, o -> Double.parseDouble(Float.valueOf((float) o).toString()));
        mapping.put(Schema.Type.DOUBLE, Function.identity());
        mapping.put(Schema.Type.STRING, Object::toString);
        mapping.put(Schema.Type.BOOLEAN, Function.identity());
        mapping.put(Schema.Type.ENUM, o -> o.toString());
        mapping.put(Schema.Type.BYTES, o -> ByteString.copyFrom((byte[]) o));
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
     * Function to convert BigQuerySchema to Avro Schema. First Converts {@link
     * com.google.cloud.bigquery.Schema} to {@link
     * com.google.api.services.bigquery.model.TableSchema} and then calls getAvroSchema().
     *
     * @param bigQuerySchema A {@link com.google.cloud.bigquery.Schema} object to cast to {@link
     *     Schema}
     * @return Converted Avro Schema
     */
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
    private static TableSchema getProtoSchemaFromAvroSchema(Schema schema) {

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
                ImmutablePair<Schema, Boolean> pair = handleUnionSchema(schema);
                elementType = pair.getLeft();
                isNullable = pair.getRight();

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
                    throw new UnsupportedOperationException(
                            "Unsupported type " + elementType.getType());
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

    /**
     * Function to convert the {@link DescriptorProto} Type to {@link Descriptor}. This is necessary
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
        Descriptors.FileDescriptor fileDescriptor =
                Descriptors.FileDescriptor.buildFrom(
                        fileDescriptorProto, new Descriptors.FileDescriptor[0]);

        // TODO: Remove dependency on guava.
        return Iterables.getOnlyElement(fileDescriptor.getMessageTypes());
    }

    /**
     * Helper function to handle the UNION Schema Type. We only consider the union schema valid when
     * it is of the form ["null", datatype]. All other forms such as ["null"],["null", datatype1,
     * datatype2, ...], and [datatype1, datatype2, ...] are considered as invalid (as there is no
     * such support in BQ) So we throw an error in all such cases. For the valid case of ["null",
     * datatype] we set the field `isNullable` as True and the Schema as the schema of the <b>not
     * null</b> datatype.
     *
     * @param schema of type UNION to check and derive.
     * @return ImmutablePair containing (Schema, Boolean isNullable)
     */
    private static ImmutablePair<Schema, Boolean> handleUnionSchema(Schema schema) {
        Schema elementType = schema;
        Boolean isNullable = false;
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
            throw new IllegalArgumentException("Multiple non-null union types are not supported.");
        }
        return new ImmutablePair<>(elementType, isNullable);
    }

    /**
     * Obtains descriptor for the Table Schema.
     *
     * @param tableSchema for which descriptor needs to be defined.
     * @return Descriptor that describes the TableSchema
     * @throws DescriptorValidationException in case Descriptor cannot be created from the input.
     */
    private static Descriptor getDescriptorFromTableSchema(TableSchema tableSchema)
            throws Descriptors.DescriptorValidationException {
        return getDescriptorFromDescriptorProto(descriptorSchemaFromTableSchema(tableSchema));
    }

    /**
     * Obtains a Descriptor Proto by obtaining Descriptor Proto field by field.
     *
     * @param tableSchema Table Schema for which descriptor is needed.
     * @return DescriptorProto describing the Schema.
     */
    private static DescriptorProto descriptorSchemaFromTableSchema(TableSchema tableSchema) {
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

    /**
     * Function to obtain the FieldDescriptorProto from a TableFieldSchema and then append it to
     * DescriptorProto builder.
     *
     * @param fieldSchema {@link TableFieldSchema} object to obtain the FieldDescriptorProto from.
     * @param fieldNumber index at which the obtained FieldDescriptorProto is appended in the
     *     Descriptor.
     * @param descriptorBuilder {@link DescriptorProto.Builder} object to add the obtained
     *     FieldDescriptorProto to.
     */
    private static void fieldDescriptorFromTableField(
            TableFieldSchema fieldSchema,
            int fieldNumber,
            DescriptorProto.Builder descriptorBuilder) {

        FieldDescriptorProto.Builder fieldDescriptorBuilder = FieldDescriptorProto.newBuilder();
        fieldDescriptorBuilder =
                fieldDescriptorBuilder.setName(fieldSchema.getName().toLowerCase());
        fieldDescriptorBuilder = fieldDescriptorBuilder.setNumber(fieldNumber);

        TableFieldSchema.Type fieldSchemaType = fieldSchema.getType();
        if (fieldSchemaType.equals(TableFieldSchema.Type.STRUCT)) {
            throw new UnsupportedOperationException("Operation is not supported yet.");
        } else {
            @Nullable
            FieldDescriptorProto.Type type = PRIMITIVE_TYPES_BQ_TO_PROTO.get(fieldSchema.getType());
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

    @VisibleForTesting
    public DescriptorProto getDescriptorProto() {
        return descriptorProto;
    }

    @VisibleForTesting
    public Descriptor getDescriptor() {
        return descriptor;
    }

    private final DescriptorProto descriptorProto;

    private final Descriptors.Descriptor descriptor;

    /**
     * Constructor for the Serializer.
     *
     * @param tableSchema Table Schema for the Sink Table ({@link
     *     com.google.api.services.bigquery.model.TableSchema} object )
     * @throws Exception In case DescriptorProto could not be converted to Descriptor or there is
     *     any error in the conversion.
     */
    public SerialiseAvroRecordsToStorageApiProtos(
            com.google.api.services.bigquery.model.TableSchema tableSchema) throws Exception {
        Schema avroSchema = getAvroSchema(tableSchema);
        TableSchema protoTableSchema = getProtoSchemaFromAvroSchema(avroSchema);
        descriptorProto = descriptorSchemaFromTableFieldSchemas(protoTableSchema.getFieldsList());
        descriptor = getDescriptorFromDescriptorProto(descriptorProto);
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
            Descriptors.FieldDescriptor fieldDescriptor =
                    Preconditions.checkNotNull(
                            descriptor.findFieldByName(field.name().toLowerCase()));
            // Get the value for a field.
            // Check if the value is null.
            @Nullable Object value = element.get(field.name());
            if (value == null) {
                // If the field is not optional, throw error.
                if (!fieldDescriptor.isOptional()) {
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
     * Function to convert a value of a AvroSchemaField value to required DynamicMessage value.
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
                ImmutablePair<Schema, Boolean> pair = handleUnionSchema(avroSchema);
                Schema type = pair.getLeft();
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
        @Nullable Function<Object, Object> encoder;
        String errorMessage;
        if (logicalTypeString != null) {
            // 1. In case the Schema has a Logical Type.
            encoder = getLogicalEncoder(logicalTypeString);
            errorMessage = "Unsupported logical type " + logicalTypeString;
        } else {
            // 2. For all the other Primitive types.
            encoder = PRIMITIVE_ENCODERS.get(fieldSchema.getType());
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
    private static Function<Object, Object> getLogicalEncoder(String logicalTypeString) {

        Map<String, Function<Object, Object>> mapping = new HashMap<>();
        //        mapping.put(LogicalTypes.date().getName(),
        //                SerialiseAvroRecordsToStorageApiProtos::convertDate);
        //        mapping.put(LogicalTypes.decimal(1).getName(), (value) ->  );
        //        mapping.put(LogicalTypes.timestampMicros().getName(), (value) ->  );
        //        mapping.put(LogicalTypes.timestampMillis().getName(), (value) -> );
        //        mapping.put(LogicalTypes.uuid().getName(), (value) -> );
        //        mapping.put(LogicalTypes.timeMillis().getName(), (value) -> );
        //        mapping.put(LogicalTypes.timeMicros().getName(), (value) -> );
        //        mapping.put(LogicalTypes.localTimestampMicros().getName(), (value) -> );
        //        mapping.put(LogicalTypes.localTimestampMicros().getName(), (value) -> );
        //        mapping.put("geography_wkt", (value) -> );
        //        mapping.put("{range, DATE}", (value) -> );
        //        mapping.put("{range, TIME}", (value) -> );
        //        mapping.put("{range, TIMESTAMP}", (value) -> );

        return mapping.get(logicalTypeString);
    }
}
