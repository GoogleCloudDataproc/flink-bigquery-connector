package com.google.cloud.flink.bigquery.common.utils;

import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.flink.bigquery.common.exceptions.BigQueryConnectorException;

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
     * @param projectName Project name of the BigQuery dataset.
     * @param datasetName Dataset ID containing the Table.
     * @param tableName Table Name.
     * @return Boolean {@code TRUE} if the table exists or {@code FALSE} if it does not.
     */
    public static Boolean tableExists(
            BigQuery client, String projectName, String datasetName, String tableName) {
        try {
            Table table = client.getTable(TableId.of(projectName, datasetName, tableName));
            return (table != null && table.exists());
        } catch (Exception e) {
            throw new BigQueryConnectorException(
                    String.format(
                            "Could not determine existence of BigQuery table %s.%s.%s",
                            projectName, datasetName, tableName),
                    e);
        }
    }
}
