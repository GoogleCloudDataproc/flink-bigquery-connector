package com.google.cloud.flink.bigquery.sink;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteSettings;
import com.google.cloud.bigquery.storage.v1.CreateWriteStreamRequest;
import com.google.cloud.bigquery.storage.v1.TableName;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.cloud.bigquery.storage.v1.WriteStream;
import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;
import com.google.cloud.flink.bigquery.common.config.CredentialsOptions;
import com.google.cloud.flink.bigquery.source.config.BigQueryReadOptions;
import org.junit.Test;

import java.io.IOException;

/** Javadoc. */
public class SinkTest {
    @Test
    public void testEnumStateSerializerInitialState() throws IOException {
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

        BigQueryWriteSettings bigQueryWriteSettings =
                BigQueryWriteSettings.newBuilder()
                        .setCredentialsProvider(
                                FixedCredentialsProvider.create(
                                        credentialsOptions.getCredentials()))
                        .build();

        try (BigQueryWriteClient bigQueryWriteClient =
                BigQueryWriteClient.create(bigQueryWriteSettings)) {
            System.out.println("@prashastia: here");
            WriteStream writeStream =
                    bigQueryWriteClient.getWriteStream(
                            "projects/bqrampupprashasti/datasets/Prashasti/tables/sink_table/streams/_default");
            System.out.println("@prashastia: " + writeStream.toString());

            TableSchema tableSchema = writeStream.getTableSchema();
            System.out.println("@prashastia: Fields-" + tableSchema.getFieldsList());
            //            System.out.println("@prashastia: Fields(0)" +
            // tableSchema.getFieldsList().get(0));
            //            System.out.println(
            //                    "@prashastia: Fields(0).getName()"
            //                            + tableSchema.getFieldsList().get(0).getName());

            System.out.println("@prashastia: Fields-" + "Explicit creating the stream.");
            // ow we create a comitted type stream and check if we gt the schema now,.
            WriteStream stream =
                    WriteStream.newBuilder().setType(WriteStream.Type.COMMITTED).build();
            TableName parentTable = TableName.of("bqrampupprashasti", "Prashasti", "sink_table");
            CreateWriteStreamRequest createWriteStreamRequest =
                    CreateWriteStreamRequest.newBuilder()
                            .setParent(parentTable.toString())
                            .setWriteStream(stream)
                            .build();
            writeStream = bigQueryWriteClient.createWriteStream(createWriteStreamRequest);

            tableSchema = writeStream.getTableSchema();
            System.out.println("@prashastia: Fields-" + tableSchema.getFieldsList());
            System.out.println("@prashastia: Fields(0)" + tableSchema.getFieldsList().get(0));
            System.out.println(
                    "@prashastia: Fields(0).getName()"
                            + tableSchema.getFieldsList().get(0).getName());

            //            return  tableSchema;
        } catch (IOException e) {
            throw new RuntimeException("Write Stream Could not be created");
        }
    }
}
