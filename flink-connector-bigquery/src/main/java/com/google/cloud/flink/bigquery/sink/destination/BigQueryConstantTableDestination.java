package com.google.cloud.flink.bigquery.sink.destination;

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableReference;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;
import com.google.cloud.flink.bigquery.common.config.CredentialsOptions;
import com.google.cloud.flink.bigquery.services.BigQueryUtils;
import com.google.cloud.flink.bigquery.sink.serializer.SerialiseAvroRecordsToStorageApiProtos;
import com.google.cloud.flink.bigquery.sink.serializer.SerialiseRecords;
import com.google.cloud.flink.bigquery.source.config.BigQueryReadOptions;
import com.google.protobuf.Descriptors;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.IOException;

/**
 * Inherits {@link Destination}. It is a wrapper around the actual location to which data is
 * written. This class represents writing to an existing BigQuery Table.
 */
public class BigQueryConstantTableDestination extends Destination {

    com.google.api.services.bigquery.model.TableSchema tableSchema = getSchema();

    SerialiseRecords serialiseRecords;

    {
        try {
            serialiseRecords = new SerialiseAvroRecordsToStorageApiProtos(tableSchema);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public BigQueryConstantTableDestination()
            throws Descriptors.DescriptorValidationException, IOException {}

    private static Table getBigQueryTable(TableReference tableReference) throws IOException {

        // TODO: Add with exponential backoff.
        TableReference updatedRef = tableReference.clone();
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
        Bigquery client = BigQueryUtils.newBigqueryBuilder(credentialsOptions).build();
        Bigquery.Tables.Get get =
                client.tables()
                        .get(
                                updatedRef.getProjectId(),
                                updatedRef.getDatasetId(),
                                updatedRef.getTableId())
                        .setPrettyPrint(false);

        // TODO: obtain with retries and exponential backoff.
        return get.execute();
    }

    private static TableDestination getTable() {
        // TODO: Extract TableDestination from Destination.
        TableReference tableReference = new TableReference();
        tableReference.setTableId("sink_table");
        tableReference.setProjectId("bqrampupprashasti");
        tableReference.setDatasetId("Prashasti");
        return new TableDestination(new TableReference());
    }

    public TableSchema getStorageTableSchema() {

        try (BigQueryWriteClient bigQueryWriteClient = BigQueryWriteClient.create()) {
            TableSchema tableSchema =
                    bigQueryWriteClient.getWriteStream("_default").getTableSchema();
            return tableSchema;
        } catch (IOException e) {
            throw new RuntimeException("Write Stream Could not be created");
        }
    }

    @Override
    public com.google.api.services.bigquery.model.TableSchema getSchema() throws IOException {

        TableDestination wrappedDestination = getTable();

        @Nullable Table existingTable = getBigQueryTable(wrappedDestination.getTableReference());

        // If the table does not exist, or the schema of the existing table is null or empty.
        if (existingTable == null
                || existingTable.getSchema() == null
                || existingTable.getSchema().isEmpty()) {
            throw new RuntimeException("The specified table does not exist.");
        } else {
            return existingTable.getSchema();
        }
    }
}
