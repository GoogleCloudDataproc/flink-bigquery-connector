package com.google.cloud.flink.bigquery.sink.serializer;

import com.google.api.client.googleapis.services.AbstractGoogleClientRequest;
import com.google.api.client.util.BackOff;
import com.google.api.client.util.BackOffUtils;
import com.google.api.client.util.Preconditions;
import com.google.api.client.util.Sleeper;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;
import com.google.cloud.flink.bigquery.common.config.CredentialsOptions;
import com.google.cloud.flink.bigquery.common.utils.SchemaTransform;
import com.google.cloud.flink.bigquery.services.BigQueryUtils;
import com.google.cloud.flink.bigquery.sink.BigQuerySink;
import com.google.cloud.flink.bigquery.sink.Destination.Destination;
import com.google.cloud.flink.bigquery.sink.Destination.TableDestination;
import com.google.cloud.flink.bigquery.source.config.BigQueryReadOptions;
import com.google.common.collect.Iterables;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import jdk.internal.org.jline.utils.Log;
import org.apache.avro.Schema;

import org.apache.flink.util.function.SerializableFunction;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.UUID;


public class SerialiseAvroRecordsToStorageApiProtos extends SerialiseRecordsToStorageApiProto {

    public final Destination destination;
    private static final Logger LOG = LoggerFactory.getLogger(BigQuerySink.class);

    private final com.google.api.services.bigquery.model.TableSchema destinationTableSchema;
    private final Schema avroSchema;

    private static TableDestination getTable(Destination destination){
        // TODO: Extract TableDestination from Destination.
        TableReference tableReference = new TableReference();
        tableReference.setTableId("sink_table");
        tableReference.setProjectId("bqrampupprashasti");
        tableReference.setDatasetId("Prashasti");
        return new TableDestination(new TableReference());
    }

    private static boolean nextBackOff(Sleeper sleeper, BackOff backoff) throws InterruptedException {
        try {
            return BackOffUtils.next(sleeper, backoff);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Descriptors.Descriptor wrapDescriptorProto(DescriptorProtos.DescriptorProto descriptorProto)
            throws Descriptors.DescriptorValidationException {
        DescriptorProtos.FileDescriptorProto fileDescriptorProto =
                DescriptorProtos.FileDescriptorProto.newBuilder().addMessageType(descriptorProto).build();
        Descriptors.FileDescriptor fileDescriptor =
                Descriptors.FileDescriptor.buildFrom(fileDescriptorProto, new Descriptors.FileDescriptor[0]);

        return Iterables.getOnlyElement(fileDescriptor.getMessageTypes());
    }

    private static void fieldDescriptorFromTableField(
            com.google.cloud.bigquery.storage.v1.TableFieldSchema fieldSchema,
            int fieldNumber,
            DescriptorProtos.DescriptorProto.Builder descriptorBuilder,
            boolean respectRequired) {
        if (StorageApiCDC.COLUMNS.contains(fieldSchema.getName())) {
            throw new RuntimeException(
                    "Reserved field name " + fieldSchema.getName() + " in user schema.");
        }
        DescriptorProtos.FieldDescriptorProto.Builder fieldDescriptorBuilder = DescriptorProtos.FieldDescriptorProto.newBuilder();
        fieldDescriptorBuilder = fieldDescriptorBuilder.setName(fieldSchema.getName().toLowerCase());
        fieldDescriptorBuilder = fieldDescriptorBuilder.setNumber(fieldNumber);
        switch (fieldSchema.getType()) {
            case STRUCT:
                DescriptorProtos.DescriptorProto nested =
                        descriptorSchemaFromTableFieldSchemas(
                                fieldSchema.getFieldsList(), respectRequired, false);
                descriptorBuilder.addNestedType(nested);
                fieldDescriptorBuilder =
                        fieldDescriptorBuilder.setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE).setTypeName(nested.getName());
                break;
            default:
                @Nullable DescriptorProtos.FieldDescriptorProto.Type type = PRIMITIVE_TYPES_BQ_TO_PROTO.get(fieldSchema.getType());
                if (type == null) {
                    throw new UnsupportedOperationException(
                            "Converting BigQuery type " + fieldSchema.getType() + " to Beam type is unsupported");
                }
                fieldDescriptorBuilder = fieldDescriptorBuilder.setType(type);
        }

        if (fieldSchema.getMode() == com.google.cloud.bigquery.storage.v1.TableFieldSchema.Mode.REPEATED) {
            fieldDescriptorBuilder = fieldDescriptorBuilder.setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED);
        } else if (!respectRequired || fieldSchema.getMode() != com.google.cloud.bigquery.storage.v1.TableFieldSchema.Mode.REQUIRED) {
            fieldDescriptorBuilder = fieldDescriptorBuilder.setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL);
        } else {
            fieldDescriptorBuilder = fieldDescriptorBuilder.setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_REQUIRED);
        }
        descriptorBuilder.addField(fieldDescriptorBuilder.build());
    }

    public static Descriptors.Descriptor getDescriptorFromTableSchema(
            TableSchema tableSchema, boolean respectRequired, boolean includeCdcColumns)
            throws Descriptors.DescriptorValidationException {
        return wrapDescriptorProto(
                descriptorSchemaFromTableSchema(tableSchema, respectRequired, includeCdcColumns));
    }


    private static TableFieldSchema fieldDescriptorFromAvroField(TableFieldSchema tableFieldSchema){
        //Todo: Convert the field here.
    }
    /**
     * Given an Avro Schema, returns a protocol-buffer TableSchema that can be used to write data
     * through BigQuery Storage API.
     *
     * @param schema An Avro Schema
     * @return Returns the TableSchema created from the provided Schema
     */
    public static TableSchema protoTableSchemaFromAvroSchema(Schema schema) {

        // Iterate over each table fields and add them to schema.
        Preconditions.checkState(!schema.getFields().isEmpty());

        TableSchema.Builder builder = TableSchema.newBuilder();
        for (Schema.Field field : schema.getFields()) {
            builder.addFields(fieldDescriptorFromAvroField(field));
        }
        return builder.build();
    }


    static DescriptorProtos.DescriptorProto descriptorSchemaFromTableSchema(
            TableSchema tableSchema, boolean respectRequired, boolean includeCdcColumns) {
        return descriptorSchemaFromTableFieldSchemas(
                tableSchema.getFieldsList(), respectRequired, includeCdcColumns);
    }

    private static Table getTable(
            TableReference ref)
            throws IOException {
        TableReference updatedRef = ref.clone();
        BigQueryReadOptions readOptions = BigQueryReadOptions.builder().
                setBigQueryConnectOptions(BigQueryConnectOptions.builder().setProjectId("bqrampupprashasti").
                        setDataset("Prashasti").
                        setTable("sink_table").build()).
                build();
        CredentialsOptions credentialsOptions = readOptions.getBigQueryConnectOptions().getCredentialsOptions();
        Bigquery client = BigQueryUtils.newBigqueryBuilder(credentialsOptions).build();
        Bigquery.Tables.Get get =
                client
                        .tables()
                        .get(updatedRef.getProjectId(), updatedRef.getDatasetId(), updatedRef.getTableId())
                        .setPrettyPrint(false);

        // TODO: obtain with retries.
        return get.execute();
    }
    private static Table getBigQueryTable(TableReference tableReference) throws IOException {

        // Attempt to obtain the table with exponential backoff.
        // TODO: Add exponential backoff
        // TODO: Add BQWriteOptions with proper builder.
        return getTable(tableReference);
    }

    private static com.google.api.services.bigquery.model.TableSchema getSchema(Destination destination) throws IOException {
        TableDestination wrappedTableDestination = getTable(destination);
        @Nullable Table existingTable = getBigQueryTable(wrappedTableDestination.getTableReference());
        if (existingTable == null
                || existingTable.getSchema() == null
                || existingTable.getSchema().isEmpty()) {
            Log.error("Table does not exist or the schema is null.");
        } else {
            return existingTable.getSchema();
        }
        return null;
    }

    private static final SerializableFunction<TableSchema, Schema> DEFAULT_AVRO_SCHEMA_FACTORY =
            (SerializableFunction<TableSchema, org.apache.avro.Schema>)
                    input -> SchemaTransform.toGenericAvroSchema(
                            String.format("root", input.getfields()));

    public SerialiseAvroRecordsToStorageApiProtos(Destination destination) throws IOException {
//        this.destination = destination;
//        destinationTableSchema = destination.getSchema();
        // TODO: Derive the Proto Descriptor Here.
        // TODO: 1. Get the Table Schema
        // TODO: 1.2 Convert it to Avro Format.
        avroSchema = DEFAULT_AVRO_SCHEMA_FACTORY.apply(getSchema(destination));
        // TODO 2: Avro -> Proto Table Schema
        /*
         Iterate over each of the fields
         Check null field or reserved name.
         Then convert to Proto Table Schema.
         */
        TableSchema protoTableSchema = protoTableSchemaFromAvroSchema(avroSchema);


        // TODO: 3. Proto Table Schema -> Descriptor.
        /*

         */
        Descriptors.Descriptor descriptor = getDescriptorFromTableSchema(protoTableSchema, false,false);

        // TODO: 4. Message from GenericRecord()
        /*

         */
        //TODO: 5. .toMessage() {NOT IN THE CONSTRUCTOR}
        // messageFromGenericRecord(descriptor, record)
        // Check if descriptor has the same fields as that in the record.
        // messageValueFromGenericRecordValue() -> toProtoValue()

    }


}
