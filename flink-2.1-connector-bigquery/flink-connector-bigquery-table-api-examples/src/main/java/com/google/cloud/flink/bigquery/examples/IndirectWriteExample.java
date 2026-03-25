package com.google.cloud.flink.bigquery.examples;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.ParameterTool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A BigQuery indirect write example using Flink SQL.
 *
 * <p>Generates 100 synthetic rows and writes them to BigQuery via indirect writes (GCS staging +
 * load jobs) in batch mode.
 *
 * <ul>
 *   <li>Flink command line format is: <br>
 *       <code>flink run {additional runtime params} {path to this jar}/IndirectWriteExample.jar
 *       </code> <br>
 *       --project {required; GCP project ID} <br>
 *       --dataset {required; BigQuery dataset name} <br>
 *       --table {required; BigQuery table name} <br>
 *       --gcs-temp-path {required; GCS path for staging Avro files}
 * </ul>
 *
 * <p>Requires the GCS Hadoop connector plugin ({@code flink-gs-fs-hadoop}) to be installed in the
 * Flink cluster's {@code plugins/} directory.
 */
public class IndirectWriteExample {

    private static final Logger LOG = LoggerFactory.getLogger(IndirectWriteExample.class);

    public static void main(String[] args) {
        final ParameterTool params = ParameterTool.fromArgs(args);

        if (params.getNumberOfParameters() < 4) {
            LOG.error(
                    "Missing parameters!\n"
                            + "Usage: IndirectWriteExample"
                            + " --project <GCP project>"
                            + " --dataset <BigQuery dataset>"
                            + " --table <BigQuery table>"
                            + " --gcs-temp-path <GCS path for staging files>");
            return;
        }

        String project = params.getRequired("project");
        String dataset = params.getRequired("dataset");
        String table = params.getRequired("table");
        String gcsTempPath = params.getRequired("gcs-temp-path");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        tEnv.executeSql(
                "CREATE TABLE source_data ("
                        + "    id BIGINT,"
                        + "    name STRING"
                        + ") WITH ("
                        + "    'connector' = 'datagen',"
                        + "    'number-of-rows' = '100'"
                        + ")");

        tEnv.executeSql(
                String.format(
                        "CREATE TABLE bigquery_sink ("
                                + "    id BIGINT,"
                                + "    name STRING"
                                + ") WITH ("
                                + "    'connector' = 'bigquery',"
                                + "    'project' = '%s',"
                                + "    'dataset' = '%s',"
                                + "    'table' = '%s',"
                                + "    'write.mode' = 'INDIRECT',"
                                + "    'write.gcs-temp-path' = '%s'"
                                + ")",
                        project, dataset, table, gcsTempPath));

        String insertSql = "INSERT INTO bigquery_sink SELECT * FROM source_data";
        System.out.println(tEnv.explainSql(insertSql, ExplainDetail.JSON_EXECUTION_PLAN));
        tEnv.executeSql(insertSql);
    }
}
