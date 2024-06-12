package com.google.cloud.flink.bigquery.table.config;

/**
 * Configurations for a BigQuery Table API Write.
 *
 * <p>Inherits {@link BigQueryTableConfig} for general options and defines sink specific options.
 *
 * <p>Uses static inner builder to initialize new instances.
 */
public class BigQuerySinkTableConfig extends BigQueryTableConfig {

    BigQuerySinkTableConfig(
            String project,
            String dataset,
            String table,
            String credentialAccessToken,
            String credentialFile,
            String credentialKey,
            boolean testMode) {
        super(
                project,
                dataset,
                table,
                credentialAccessToken,
                credentialFile,
                credentialKey,
                testMode);
    }
}
