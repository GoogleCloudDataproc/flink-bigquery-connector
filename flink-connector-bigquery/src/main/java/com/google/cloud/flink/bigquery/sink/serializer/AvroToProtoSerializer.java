package com.google.cloud.flink.bigquery.sink.serializer;

import com.google.api.client.util.Preconditions;
import com.google.cloud.flink.bigquery.common.utils.SchemaTransform;
import com.google.cloud.flink.bigquery.services.BigQueryUtils;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.ImmutablePair;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/** Class to Serialise Avro Generic Records to Storage API protos. */
public class AvroToProtoSerializer extends BigQueryProtoSerializer {

    private static final Map<Schema.Type, FieldDescriptorProto.Type> AVRO_TYPES_TO_PROTO =
            initializeAvroFieldToFieldDescriptorTypes();

    private static final Map<String, FieldDescriptorProto.Type> LOGICAL_AVRO_TYPES_TO_PROTO =
            initializeLogicalAvroFieldToFieldDescriptorTypes();

    private static final Map<Schema.Type, UnaryOperator<Object>> PRIMITIVE_ENCODERS =
            initializePrimitiveEncoderFunction();

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
                o -> Double.parseDouble(String.valueOf((float) o))); // FLOAT -> Double
        mapping.put(Schema.Type.STRING, Object::toString);
        mapping.put(Schema.Type.ENUM, Object::toString);
        mapping.put(Schema.Type.FIXED, o -> ByteString.copyFrom(((GenericData.Fixed) o).bytes()));
        mapping.put(Schema.Type.BYTES, o -> ByteString.copyFrom(((ByteBuffer) o).array()));
        return mapping;
    }

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

    private static Map<String, FieldDescriptorProto.Type>
            initializeLogicalAvroFieldToFieldDescriptorTypes() {
        //        Map<Schema.Type, FieldDescriptorProto.Type> mapping = new HashMap<>();
        //        return mapping;
        return new HashMap<>();
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
     * datatype] we set the field `isNullable` as True and the Schema as the schema of the <b>not
     * null</b> datatype.
     *
     * @param schema of type UNION to check and derive.
     * @return ImmutablePair containing (Schema, Boolean isNullable)
     */
    private static ImmutablePair<Schema, Boolean> handleUnionSchema(Schema schema) {
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
            return new ImmutablePair<>(elementType, true);
        } else {
            throw new IllegalArgumentException("Multiple non-null union types are not supported.");
        }
    }

    @Override
    public DescriptorProto getDescriptorProto() {
        return descriptorProto;
    }

    private final DescriptorProto descriptorProto;

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
    public void fieldDescriptorFromSchemaField(
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
                throw new UnsupportedOperationException("Operation is not supported yet.");
            case UNION:
                // Types can be ["null"] - Not supported in BigQuery.
                // ["null", something]  - Bigquery Field of type something with mode NULLABLE
                // ["null", something1, something2], [something1, something2] - Are invalid types
                // not supported in BQ
                ImmutablePair<Schema, Boolean> pair = handleUnionSchema(schema);
                elementType = pair.getLeft();
                isNullable = pair.getRight();

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
    public DescriptorProto getDescriptorSchemaFromAvroSchema(Schema schema) {
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
        descriptorProto = getDescriptorSchemaFromAvroSchema(avroSchema);
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
        @Nullable UnaryOperator<Object> encoder;
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
    private static UnaryOperator<Object> getLogicalEncoder(String logicalTypeString) {
        throw new UnsupportedOperationException(
                String.format("Logical Type '%s' is not supported.", logicalTypeString));
    }
}
