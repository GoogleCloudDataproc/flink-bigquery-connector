package com.google.cloud.flink.bigquery.sink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.google.api.gax.retrying.RetrySettings;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.ProtoRows;
import com.google.cloud.bigquery.storage.v1.ProtoSchema;
import com.google.cloud.bigquery.storage.v1.StreamWriter;
import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;
import com.google.cloud.flink.bigquery.common.config.CredentialsOptions;
import com.google.cloud.flink.bigquery.services.BigQueryServices;
import com.google.cloud.flink.bigquery.services.BigQueryServicesFactory;
import com.google.cloud.flink.bigquery.sink.serializer.AvroToProtoSerializer;
import com.google.cloud.flink.bigquery.source.BigQuerySource;
import com.google.cloud.flink.bigquery.source.config.BigQueryReadOptions;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/** Javadoc. */
public class SinkTest {

    public static final String SIMPLE_AVRO_SCHEMA_FIELDS_STRING =
            " \"fields\": [\n"
                    + "   {\"name\": \"name\", \"type\": \"string\"},\n"
                    + "   {\"name\": \"number\", \"type\": \"long\"},\n"
                    + "   {\"name\" : \"ts\", \"type\" : {\"type\" : \"long\",\"logicalType\" : \"timestamp-micros\"}},\n"
                    //                    + "   {\"name\" : \"sample_field_1\", \"type\" :
                    // [\"string\", \"long\"]},\n"
                    + "   {\"name\" : \"sample_field_2\", \"type\" : [\"null\", \"long\"]},\n"
                    //                    + "   {\"name\" : \"sample_field_3\", \"type\" :
                    // [\"null\", \"null\"]}\n"
                    //                    + "   {\"name\" : \"sample_field_4\", \"type\" :
                    // [\"null\"]}\n"
                    + "   {\"name\" : \"sample_array_field\", \"type\" : {\"type\": \"array\",\"items\" : \"string\", \"default\": [] }}\n"

                    //                    + "   {\"name\" : \"sample_field_5\", \"type\" :
                    // [\"null\", \"string\", \"long\"]}\n"
                    + " ]\n";
    public static final String SIMPLE_AVRO_SCHEMA_STRING =
            "{\"namespace\": \"project.dataset\",\n"
                    + " \"type\": \"record\",\n"
                    + " \"name\": \"table\",\n"
                    + " \"doc\": \"Translated Avro Schema for project.dataset.table\",\n"
                    + SIMPLE_AVRO_SCHEMA_FIELDS_STRING
                    + "}";

    BigQueryReadOptions readOptions =
            BigQueryReadOptions.builder()
                    .setBigQueryConnectOptions(
                            BigQueryConnectOptions.builder()
                                    .setProjectId("bqrampupprashasti")
                                    .setDataset("Prashasti")
                                    .setTable("simple_table")
                                    .build())
                    .build();

    BigQueryConnectOptions readConnectOptions = readOptions.getBigQueryConnectOptions();

    CredentialsOptions readCredentialsOptions = readConnectOptions.getCredentialsOptions();

    BigQueryReadOptions writeOptions =
            BigQueryReadOptions.builder()
                    .setBigQueryConnectOptions(
                            BigQueryConnectOptions.builder()
                                    .setProjectId("bqrampupprashasti")
                                    .setDataset("Prashasti")
                                    .setTable("simple_table_write")
                                    .build())
                    .build();

    BigQueryConnectOptions writeConnectOptions = writeOptions.getBigQueryConnectOptions();

    CredentialsOptions writeCredentialsOptions = writeConnectOptions.getCredentialsOptions();

    public SinkTest() throws IOException {}

    //    @Test
    //    public void checkSchemaConversionForUnion() {
    //
    //        //        System.out.println(SIMPLE_AVRO_SCHEMA_STRING.charAt());
    //        System.out.println("@prashastia >>> ");
    //        System.out.println("here 0");
    //
    //        org.apache.avro.Schema simpleAvroSchema =
    //                new org.apache.avro.Schema.Parser().parse(SIMPLE_AVRO_SCHEMA_STRING);
    //
    //        System.out.println("@prashastia >>> ");
    //        System.out.println("here 1");
    //
    //        TableSchema convertedTableSchema =
    //                SerialiseAvroRecordsToStorageApiProtos.getProtoSchemaFromAvroSchema(
    //                        simpleAvroSchema);
    //
    //        System.out.println("@prashastia >>> ");
    //        System.out.println(convertedTableSchema.toString());
    //
    //
    //    }

    //    private com.google.api.services.bigquery.model.TableSchema getTableSchema() throws
    // IOException {
    //
    //        Bigquery bigqueryClient =
    // BigQueryUtils.newBigqueryBuilder(writeCredentialsOptions).build();
    //        com.google.api.services.bigquery.model.Table table =
    //                bigqueryClient
    //                        .tables()
    //                        .get(
    //                                writeConnectOptions.getProjectId(),
    //                                writeConnectOptions.getDataset(),
    //                                writeConnectOptions.getTable())
    //                        .execute();
    //        return table.getSchema();
    //    }
    //

    private List<GenericRecord> readRows() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        System.out.println("env formed");
        BigQuerySource<GenericRecord> source = BigQuerySource.readAvros(readOptions);
        System.out.println("source formed");
        return env.fromSource(source, WatermarkStrategy.noWatermarks(), "BigQuerySource")
                .executeAndCollect(100);
    }

    @Test
    public void checkSerialisation() throws Exception {
        // Create the write stream.
        // write using the Storage Write API.
        String tablePath =
                "projects/"
                        + writeConnectOptions.getProjectId()
                        + "/datasets/"
                        + writeConnectOptions.getDataset()
                        + "/tables/"
                        + writeConnectOptions.getTable();
        BigQueryServices.SinkDataClient sinkDataClient =
                BigQueryServicesFactory.instance(writeConnectOptions).sinkDataClient();
        TableSchema tableSchema =
                sinkDataClient.getBigQueryTableSchema(
                        writeConnectOptions.getProjectId(),
                        writeConnectOptions.getDataset(),
                        writeConnectOptions.getTable());

        System.out.println("@prashastia serializer initiated.");
        AvroToProtoSerializer serialiseAvroRecordsToStorageApiProtos =
                new AvroToProtoSerializer(tableSchema);

        DescriptorProtos.DescriptorProto descriptorProto =
                serialiseAvroRecordsToStorageApiProtos.getDescriptorProto();
        System.out.println("@prashastia [DescriptorProto] " + descriptorProto);

        Descriptors.Descriptor descriptor =
                AvroToProtoSerializer.getDescriptorFromDescriptorProto(descriptorProto);
        System.out.println("@prashastia [Descriptor] " + descriptor);

        ProtoSchema protoSchema =
                ProtoSchema.newBuilder().setProtoDescriptor(descriptorProto).build();

        ProtoRows.Builder protoRowsBuilder = ProtoRows.newBuilder();

        try (BigQueryServices.StorageWriteClient writeClient =
                BigQueryServicesFactory.instance(writeConnectOptions).storageWrite()) {
            System.out.println("@prashastia writeClient formed " + writeClient);
            String writeStreamName = String.format("%s/streams/_default", tablePath);
            StreamWriter streamWriter =
                    writeClient.createStreamWriter(
                            protoSchema, RetrySettings.newBuilder().build(), writeStreamName);
            System.out.println("@prashastia streamWriter formed " + streamWriter);
            System.out.println(
                    "@prashastia streamWriter.getStreamName() formed "
                            + streamWriter.getStreamName());

            System.out.println("@prashastia readRows() Started... ");
            List<GenericRecord> records = readRows();
            System.out.println("@prashastia readRows() Finished. [size] " + records.size());

            for (GenericRecord element : records) {
                System.out.println("@prashastia record write [" + element + "]");
                // Add the serialisation step here.
                protoRowsBuilder.addSerializedRows(
                        AvroToProtoSerializer.getDynamicMessageFromGenericRecord(
                                        (GenericRecord) element, descriptor)
                                .toByteString());
                System.out.println("@prashastia addSerialisedRow completed.");
            }
            ProtoRows rowsToAppend = protoRowsBuilder.build();
            System.out.println("@prashastia append()  Started...");
            AppendRowsResponse response = streamWriter.append(rowsToAppend).get();
            System.out.println("@prashastia: [response]" + response);
            streamWriter

        } catch (Exception e) {
            System.out.println("@prashastia: [exception] " + e.getMessage());
        }
    }

    //    @Test
    //    public void testEnumStateSerializerInitialState() throws IOException, IOException {
    //        BigQueryReadOptions readOptions =
    //                BigQueryReadOptions.builder()
    //                        .setBigQueryConnectOptions(
    //                                BigQueryConnectOptions.builder()
    //                                        .setProjectId("bqrampupprashasti")
    //                                        .setDataset("Prashasti")
    //                                        .setTable("sink_table")
    //                                        .build())
    //                        .build();
    //
    //        CredentialsOptions credentialsOptions =
    //                readOptions.getBigQueryConnectOptions().getCredentialsOptions();
    //        Bigquery bigqueryClient =
    // BigQueryUtils.newBigqueryBuilder(credentialsOptions).build();
    //        com.google.api.services.bigquery.model.Table table1 =
    //                bigqueryClient
    //                        .tables()
    //                        .get("bqrampupprashasti", "Prashasti", "sink_table")
    //                        .execute();
    //        com.google.api.services.bigquery.model.TableSchema tableSchema3 = table1.getSchema();
    //        org.apache.avro.Schema convertedAvroSchema =
    //                SchemaTransform.toGenericAvroSchema("root", tableSchema3.getFields());
    //        System.out.println(
    //                "Schema Another Method "
    //                        + "Converted avro schema is "
    //                        + convertedAvroSchema.toString());
    //
    //        TableSchema convertedProtoSchema =
    //                SerialiseAvroRecordsToStorageApiProtos.getProtoSchemaFromAvroSchema(
    //                        convertedAvroSchema);
    //
    //        System.out.println(
    //                "@prashastia >>>" + "Converted Proto schema is " +
    // convertedProtoSchema.toString());
    //    }

    //    @Test
    //    public void testMethod2() {
    //            BigQueryWriteSettings bigQueryWriteSettings =
    //                    BigQueryWriteSettings.newBuilder()
    //                            .setCredentialsProvider(
    //                                    FixedCredentialsProvider.create(
    //                                            credentialsOptions.getCredentials()))
    //                            .build();
    //            try (BigQueryWriteClient bigQueryWriteClient =
    //                    BigQueryWriteClient.create(bigQueryWriteSettings)) {
    //                System.out.println("@prashastia: here");
    //                WriteStream writeStream =
    //                        bigQueryWriteClient.getWriteStream(
    //
    //     "projects/bqrampupprashasti/datasets/Prashasti/tables/sink_table/streams/_default");
    //                System.out.println("@prashastia: " + writeStream.toString());
    //
    //                TableSchema tableSchema = writeStream.getTableSchema();
    ////                System.out.println("@prashastia: Fields-" +
    // tableSchema.getFieldsList());
    //                //            System.out.println("@prashastia: Fields(0)" +
    //                // tableSchema.getFieldsList().get(0));
    //                //            System.out.println(
    //                //                    "@prashastia: Fields(0).getName()"
    //                //                            +
    // tableSchema.getFieldsList().get(0).getName());
    //
    //                System.out.println("@prashastia: Fields-" + "Explicit creating the
    // stream.");
    //                // ow we create a comitted type stream and check if we gt the schema now,.
    //                WriteStream stream =
    //
    // WriteStream.newBuilder().setType(WriteStream.Type.COMMITTED).build();
    //                TableName parentTable = TableName.of("bqrampupprashasti", "Prashasti",
    //     "sink_table");
    //                CreateWriteStreamRequest createWriteStreamRequest =
    //                        CreateWriteStreamRequest.newBuilder()
    //                                .setParent(parentTable.toString())
    //                                .setWriteStream(stream)
    //                                .build();
    //                writeStream =
    // bigQueryWriteClient.createWriteStream(createWriteStreamRequest);
    //
    //                tableSchema = writeStream.getTableSchema();
    //                System.out.println("@prashastia: Fields-" + tableSchema.getFieldsList());
    //                System.out.println("@prashastia: Fields(0)" +
    // tableSchema.getFieldsList().get(0));
    //                System.out.println(
    //                        "@prashastia: Fields(0).getName()"
    //                                + tableSchema.getFieldsList().get(0).getName());
    //
    //                //            return  tableSchema;
    //
    //                System.out.println("@prashastia: Fields(0): Getting Schema via BQ
    // Client");

    //                         Integer[] numberOfTriesArray = {1};

    //            for (int numberOfTries : numberOfTriesArray) {

    //            Long startTime = System.currentTimeMillis();
    //            for (int i = 0; i < numberOfTries; i++) {
    // Method 1
    // Method 2
    //                BigQuery bigQueryClient =
    //                        BigQueryOptions.newBuilder()
    //                                .setCredentials(
    //                                        readOptions
    //                                                .getBigQueryConnectOptions()
    //                                                .getCredentialsOptions()
    //                                                .getCredentials())
    //                                .build()
    //                                .getService();
    //                TableId tableId = TableId.of("bqrampupprashasti", "Prashasti",
    // "sink_table");
    //                Table table = bigQueryClient.getTable(tableId);
    //                Schema schemaOfTypeSchema = table.getDefinition().getSchema();
    //
    //                com.google.api.services.bigquery.model.TableSchema tableSchemaConverted4 =
    //                        SchemaTransform.bigQuerySchemaToTableSchema(schemaOfTypeSchema);
    //                System.out.println(
    //                        "Schema Another Method "
    //                                + "Converted avro schema is [TableSchema] "
    //                                + tableSchemaConverted4.toString());
    //
    //                org.apache.avro.Schema convertedAvroSchema2 =
    //                        SchemaTransform.toGenericAvroSchema("root",
    //     tableSchemaConverted4.getFields());
    //
    //                // schemaOfTypeSchema);

    //                System.out.println(
    //                        "Schema Another Method "
    //                                + "Converted avro schema is "
    //                                + convertedAvroSchema.toString());
    //                //                }

    //                System.out.println(
    //                        "TYPE For field 'name' is"
    //                                +
    // convertedAvroSchema.getFields().get(0).schema().getType());
    //
    //                assert convertedAvroSchema.equals(convertedAvroSchema2);

    //    }
}
