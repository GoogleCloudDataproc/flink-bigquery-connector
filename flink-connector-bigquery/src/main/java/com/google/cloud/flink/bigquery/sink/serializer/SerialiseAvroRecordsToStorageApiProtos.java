package com.google.cloud.flink.bigquery.sink.serializer;

import org.apache.flink.shaded.guava30.com.google.common.collect.Iterables;

import com.google.api.client.util.BackOff;
import com.google.api.client.util.BackOffUtils;
import com.google.api.client.util.Preconditions;
import com.google.api.client.util.Sleeper;
import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.cloud.flink.bigquery.common.utils.SchemaTransform;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/** Javadoc. */
public class SerialiseAvroRecordsToStorageApiProtos extends SerialiseRecordsToStorageApiProto {

    static final Map<String, TableFieldSchema.Type> LOGICAL_TYPES =
            initializeLogicalAvroToTableFieldTypes();
    static final Map<Schema.Type, TableFieldSchema.Type> PRIMITIVE_TYPES =
            initializePrimitiveAvroToTableFieldTypes();

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
        mapping.put(LogicalTypes.localTimestampMillis().getName(), TableFieldSchema.Type.DATETIME);
        mapping.put(LogicalTypes.localTimestampMicros().getName(), TableFieldSchema.Type.DATETIME);
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

    private static TableFieldSchema getTableFieldFromAvroField(Schema.Field field) {
        @Nullable Schema schema = field.schema();
        // Check if schema is not null
        Preconditions.checkNotNull(schema, "Unexpected null schema!");

        // TODO: Check if the field name is a CDC Column.

        TableFieldSchema.Builder builder =
                TableFieldSchema.newBuilder().setName(field.name().toLowerCase());
        Schema elementType = schema;
        switch (schema.getType()) {
            case RECORD:
                throw new UnsupportedOperationException("Operation is not supported yet.");
            case ARRAY:
                throw new UnsupportedOperationException("Operation is not supported yet.");
            case MAP:
                throw new UnsupportedOperationException("Operation is not supported yet.");
            case UNION:
                throw new UnsupportedOperationException("Operation is not supported yet.");
            default:
                // Handling of the Logical or Primitive Type.
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
            //            if (TypeWithNullability.create(schema).isNullable()) {
            //                builder =
            // builder.setMode(TableFieldSchema.Mode.NULLABLE);
            //            } else {
            //            TODO: Implement TypeWithNullability() here.
            builder = builder.setMode(TableFieldSchema.Mode.REQUIRED);
            //            }
        }
        if (field.doc() != null) {
            builder = builder.setDescription(field.doc());
        }
        return builder.build();
    }

    //    static final Map<TableFieldSchema.Type, DescriptorProtos.FieldDescriptorProto.Type>
    //            PRIMITIVE_TYPES_BQ_TO_PROTO =
    //                    ImmutableMap
    //                            .<TableFieldSchema.Type,
    // DescriptorProtos.FieldDescriptorProto.Type>
    //                                    builder()
    //                            .put(
    //                                    TableFieldSchema.Type.INT64,
    //                                    DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64)
    //                            .put(
    //                                    TableFieldSchema.Type.DOUBLE,
    //                                    DescriptorProtos.FieldDescriptorProto.Type.TYPE_DOUBLE)
    //                            .put(
    //                                    TableFieldSchema.Type.STRING,
    //                                    DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)
    //                            .put(
    //                                    TableFieldSchema.Type.BOOL,
    //                                    DescriptorProtos.FieldDescriptorProto.Type.TYPE_BOOL)
    //                            .put(
    //                                    TableFieldSchema.Type.BYTES,
    //                                    DescriptorProtos.FieldDescriptorProto.Type.TYPE_BYTES)
    //                            .put(
    //                                    TableFieldSchema.Type.NUMERIC,
    //                                    DescriptorProtos.FieldDescriptorProto.Type.TYPE_BYTES)
    //                            .put(
    //                                    TableFieldSchema.Type.BIGNUMERIC,
    //                                    DescriptorProtos.FieldDescriptorProto.Type.TYPE_BYTES)
    //                            .put(
    //                                    TableFieldSchema.Type.GEOGRAPHY,
    //                                    DescriptorProtos.FieldDescriptorProto.Type
    //                                            .TYPE_STRING) // Pass through the JSON encoding.
    //                            .put(
    //                                    TableFieldSchema.Type.DATE,
    //                                    DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32)
    //                            .put(
    //                                    TableFieldSchema.Type.TIME,
    //                                    DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64)
    //                            .put(
    //                                    TableFieldSchema.Type.DATETIME,
    //                                    DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64)
    //                            .put(
    //                                    TableFieldSchema.Type.TIMESTAMP,
    //                                    DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64)
    //                            .put(
    //                                    TableFieldSchema.Type.JSON,
    //                                    DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)
    //                            .build();

    private static boolean nextBackOff(Sleeper sleeper, BackOff backoff)
            throws InterruptedException {
        try {
            return BackOffUtils.next(sleeper, backoff);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Descriptors.Descriptor wrapDescriptorProto(
            DescriptorProtos.DescriptorProto descriptorProto)
            throws Descriptors.DescriptorValidationException {
        FileDescriptorProto fileDescriptorProto =
                FileDescriptorProto.newBuilder().addMessageType(descriptorProto).build();
        Descriptors.FileDescriptor fileDescriptor =
                Descriptors.FileDescriptor.buildFrom(
                        fileDescriptorProto, new Descriptors.FileDescriptor[0]);

        return Iterables.getOnlyElement(fileDescriptor.getMessageTypes());
    }

    private static DescriptorProtos.DescriptorProto descriptorSchemaFromTableFieldSchemas(
            Iterable<TableFieldSchema> tableFieldSchemas,
            boolean respectRequired,
            boolean includeCdcColumns) {
        DescriptorProtos.DescriptorProto.Builder descriptorBuilder =
                DescriptorProtos.DescriptorProto.newBuilder();
        // Create a unique name for the descriptor ('-' characters cannot be used).
        descriptorBuilder.setName("D" + UUID.randomUUID().toString().replace("-", "_"));
        int i = 1;
        for (TableFieldSchema fieldSchema : tableFieldSchemas) {
            fieldDescriptorFromTableField(fieldSchema, i++, descriptorBuilder, respectRequired);
        }
        if (includeCdcColumns) {
            DescriptorProtos.FieldDescriptorProto.Builder fieldDescriptorBuilder =
                    DescriptorProtos.FieldDescriptorProto.newBuilder();
            //           TODO:  fieldDescriptorBuilder =
            // fieldDescriptorBuilder.setName(StorageApiCDC.CHANGE_TYPE_COLUMN);
            fieldDescriptorBuilder = fieldDescriptorBuilder.setNumber(i++);
            fieldDescriptorBuilder =
                    fieldDescriptorBuilder.setType(
                            DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING);
            fieldDescriptorBuilder =
                    fieldDescriptorBuilder.setLabel(
                            DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL);
            descriptorBuilder.addField(fieldDescriptorBuilder.build());

            fieldDescriptorBuilder = DescriptorProtos.FieldDescriptorProto.newBuilder();
            //          TODO:  fieldDescriptorBuilder =
            // fieldDescriptorBuilder.setName(StorageApiCDC.CHANGE_SQN_COLUMN);
            fieldDescriptorBuilder = fieldDescriptorBuilder.setNumber(i++);
            fieldDescriptorBuilder =
                    fieldDescriptorBuilder.setType(
                            DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64);
            fieldDescriptorBuilder =
                    fieldDescriptorBuilder.setLabel(
                            DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL);
            descriptorBuilder.addField(fieldDescriptorBuilder.build());
        }
        return descriptorBuilder.build();
    }

    private static void fieldDescriptorFromTableField(
            TableFieldSchema fieldSchema,
            int fieldNumber,
            DescriptorProtos.DescriptorProto.Builder descriptorBuilder,
            boolean respectRequired) {
        // TODO: Throw an error if CDC column
        //        if (StorageApiCDC.COLUMNS.contains(fieldSchema.getName())) {
        //            throw new RuntimeException(
        //                    "Reserved field name " + fieldSchema.getName() + " in user schema.");
        //        }
        DescriptorProtos.FieldDescriptorProto.Builder fieldDescriptorBuilder =
                DescriptorProtos.FieldDescriptorProto.newBuilder();
        fieldDescriptorBuilder =
                fieldDescriptorBuilder.setName(fieldSchema.getName().toLowerCase());
        fieldDescriptorBuilder = fieldDescriptorBuilder.setNumber(fieldNumber);
        switch (fieldSchema.getType()) {
            case STRUCT:
                DescriptorProtos.DescriptorProto nested =
                        descriptorSchemaFromTableFieldSchemas(
                                fieldSchema.getFieldsList(), respectRequired, false);
                descriptorBuilder.addNestedType(nested);
                fieldDescriptorBuilder =
                        fieldDescriptorBuilder
                                .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)
                                .setTypeName(nested.getName());
                break;
            default:
                @Nullable DescriptorProtos.FieldDescriptorProto.Type type = null;
                //                        PRIMITIVE_TYPES_BQ_TO_PROTO.get(fieldSchema.getType());
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
                    fieldDescriptorBuilder.setLabel(
                            DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED);
        } else if (!respectRequired || fieldSchema.getMode() != TableFieldSchema.Mode.REQUIRED) {
            fieldDescriptorBuilder =
                    fieldDescriptorBuilder.setLabel(
                            DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL);
        } else {
            fieldDescriptorBuilder =
                    fieldDescriptorBuilder.setLabel(
                            DescriptorProtos.FieldDescriptorProto.Label.LABEL_REQUIRED);
        }
        descriptorBuilder.addField(fieldDescriptorBuilder.build());
    }

    public static Descriptors.Descriptor getDescriptorFromTableSchema(
            TableSchema tableSchema, boolean respectRequired, boolean includeCdcColumns)
            throws Descriptors.DescriptorValidationException {
        return wrapDescriptorProto(
                descriptorSchemaFromTableSchema(tableSchema, respectRequired, includeCdcColumns));
    }

    static DescriptorProtos.DescriptorProto descriptorSchemaFromTableSchema(
            TableSchema tableSchema, boolean respectRequired, boolean includeCdcColumns) {
        return descriptorSchemaFromTableFieldSchemas(
                tableSchema.getFieldsList(), respectRequired, includeCdcColumns);
    }

    public SerialiseAvroRecordsToStorageApiProtos(
            com.google.api.services.bigquery.model.TableSchema tableSchema) {

        Schema avroSchema = getAvroSchema(tableSchema);

        TableSchema protoTableSchema = getProtoSchemaFromAvroSchema(avroSchema);

        //        TableSchema storageTableSchema = destination.getStorageTableSchema();

        // TODO: 3. Proto Table Schema -> Descriptor.
        /*

        */
        //        Descriptors.Descriptor descriptor =
        //                getDescriptorFromTableSchema(protoTableSchema, false, false);

        // TODO: 4. Message from GenericRecord()
        /*

        */
        // TODO: 5. .toMessage() {NOT IN THE CONSTRUCTOR}
        // messageFromGenericRecord(descriptor, record)
        // Check if descriptor has the same fields as that in the record.
        // messageValueFromGenericRecordValue() -> toProtoValue()

    }
}
