package com.google.cloud.flink.bigquery.common.utils;

import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.TableId;

import java.util.Optional;

/** Class to obtain BigQuery Table Schema. */
public class BigQueryGetTableSchema {

    // Make the constructor private so that it cannot be instantiated.
    private BigQueryGetTableSchema() {}

    /**
     * Function to Obtain a Bigquery Table Schema.
     *
     * @param client {@link BigQuery} Object containing the BigQuery Client.
     * @param project Project ID containing the Table.
     * @param dataset Dataset ID containing the Table.
     * @param table Table Name.
     * @return {@link TableSchema} Object containing the Table Schema requested.
     */
    public static TableSchema get(BigQuery client, String project, String dataset, String table) {
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
}
