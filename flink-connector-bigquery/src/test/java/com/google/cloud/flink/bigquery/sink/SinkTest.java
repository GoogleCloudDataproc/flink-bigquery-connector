package com.google.cloud.flink.bigquery.sink;

import com.google.api.services.bigquery.Bigquery;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;
import com.google.cloud.flink.bigquery.common.config.CredentialsOptions;
import com.google.cloud.flink.bigquery.common.utils.SchemaTransform;
import com.google.cloud.flink.bigquery.services.BigQueryUtils;
import com.google.cloud.flink.bigquery.sink.serializer.SerialiseAvroRecordsToStorageApiProtos;
import com.google.cloud.flink.bigquery.source.config.BigQueryReadOptions;
import org.junit.Test;

import java.io.IOException;

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

    @Test
    public void testEnumStateSerializerInitialState() throws IOException, IOException {
        BigQueryReadOptions readOptions =
                BigQueryReadOptions.builder()
                        .setBigQueryConnectOptions(
                                BigQueryConnectOptions.builder()
                                        .setProjectId("bqrampupprashasti")
                                        .setDataset("Prashasti")
                                        .setTable("sink_table")
                                        .build())
                        .build();

        CredentialsOptions credentialsOptions =
                readOptions.getBigQueryConnectOptions().getCredentialsOptions();
        Bigquery bigqueryClient = BigQueryUtils.newBigqueryBuilder(credentialsOptions).build();
        com.google.api.services.bigquery.model.Table table1 =
                bigqueryClient
                        .tables()
                        .get("bqrampupprashasti", "Prashasti", "sink_table")
                        .execute();
        com.google.api.services.bigquery.model.TableSchema tableSchema3 = table1.getSchema();
        org.apache.avro.Schema convertedAvroSchema =
                SchemaTransform.toGenericAvroSchema("root", tableSchema3.getFields());
        System.out.println(
                "Schema Another Method "
                        + "Converted avro schema is "
                        + convertedAvroSchema.toString());

        TableSchema convertedProtoSchema =
                SerialiseAvroRecordsToStorageApiProtos.getProtoSchemaFromAvroSchema(
                        convertedAvroSchema);

        System.out.println(
                "@prashastia >>>" + "Converted Proto schema is " + convertedProtoSchema.toString());
    }

    @Test
    public void testMethod2() {
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

    }
}
