package com.google.cloud.flink.bigquery.sink.serializer;

import com.google.api.client.util.Preconditions;
import com.google.cloud.flink.bigquery.common.utils.SchemaTransform;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.util.EnumMap;
import java.util.Map;
import java.util.function.UnaryOperator;

/** Class to Serialise Avro Generic Records to Storage API protos. */
public class AvroToProtoSerializer extends BigQueryProtoSerializer {

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

    @Override
    public DescriptorProto getDescriptorProto() {
        return descriptorProto;
    }

    private final DescriptorProto descriptorProto;

    /**
     * Constructor for the Serializer.
     *
     * @param tableSchema Table Schema for the Sink Table ({@link
     *     com.google.api.services.bigquery.model.TableSchema} object )
     */
    public AvroToProtoSerializer(com.google.api.services.bigquery.model.TableSchema tableSchema) {
        Schema avroSchema = getAvroSchema(tableSchema);
        // TODO: Decide on approach and obtain descriptorProto.
        descriptorProto = null;
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
                return new UnsupportedOperationException("Not supported yet");
            case UNION:
                return new UnsupportedOperationException("Not supported yet");
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
