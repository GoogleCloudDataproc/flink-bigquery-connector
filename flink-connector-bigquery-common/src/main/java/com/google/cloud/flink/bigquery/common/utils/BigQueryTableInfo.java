package com.google.cloud.flink.bigquery.common.utils;

import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;

import java.util.Optional;

/** Class to obtain information about BigQuery Table. */
public class BigQueryTableInfo {

    // Make the constructor private so that it cannot be instantiated.
    private BigQueryTableInfo() {}

    /**
     * Function to Obtain a Bigquery Table Schema.
     *
     * @param client {@link BigQuery} Object containing the BigQuery Client.
     * @param project Project ID containing the Table.
     * @param dataset Dataset ID containing the Table.
     * @param table Table Name.
     * @return {@link TableSchema} Object containing the Table Schema requested.
     */
    public static TableSchema getSchema(
            BigQuery client, String project, String dataset, String table) {
        return Optional.ofNullable(client.getTable(TableId.of(project, dataset, table)))
                .map(t -> t.getDefinition().getSchema())
                .map(SchemaTransform::bigQuerySchemaToTableSchema)
                .orElseThrow(
                        () ->
                                new IllegalArgumentException(
                                        String.format(
                                                "The provided table %s.%s.%s does not exists.",
                                                project, dataset, table)));
    }
    /**
     * Function to identify if a BigQuery table exists.
     *
     * @param client {@link BigQuery} Object containing the BigQuery Client.
     * @param dataset Dataset ID containing the Table.
     * @param tableName Table Name.
     * @return Boolean {@code TRUE} if the table exists or {@code FALSE} if it does not.
     */
    public static Boolean tableExists(BigQuery client, String dataset, String tableName) {
        try {
            Table table = client.getTable(TableId.of(dataset, tableName));
            return (table != null && table.exists());
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    String.format(
                            "The provided Bigquery table %s.%s not found with exception: %s",
                            dataset, tableName, e.toString()));
        }
    }
}
