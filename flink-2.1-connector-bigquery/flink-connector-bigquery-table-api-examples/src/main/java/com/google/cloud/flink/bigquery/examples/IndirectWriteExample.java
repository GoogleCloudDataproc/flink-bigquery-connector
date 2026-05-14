package com.google.cloud.flink.bigquery.examples;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.ParameterTool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A BigQuery indirect write example using Flink SQL that exercises every supported BigQuery
 * datatype.
 *
 * <p>Inserts a small fixed set of rows with distinctive values for each column so that round-trip
 * fidelity (especially microsecond-precision TIMESTAMP / DATETIME) can be verified by querying the
 * destination table after the job finishes.
 *
 * <p>Expected target schema (created in BigQuery before running):
 *
 * <pre>
 * CREATE TABLE `&lt;project&gt;.&lt;dataset&gt;.&lt;table&gt;` (
 *   bool_col       BOOL,
 *   bytes_col      BYTES,
 *   string_col     STRING,
 *   int64_col      INT64,
 *   float64_col    FLOAT64,
 *   numeric_col    NUMERIC,
 *   bignumeric_col BIGNUMERIC,
 *   date_col       DATE,
 *   timestamp_col  TIMESTAMP,
 *   timestamp3_col TIMESTAMP,
 *   array_col      ARRAY&lt;INT64&gt;,
 *   struct_col     STRUCT&lt;a INT64, b STRING&gt;
 * );
 * </pre>
 *
 * <p>Note: Parquet staging can't distinguish DATETIME from TIMESTAMP (Flink's
 * ParquetSchemaConverter emits the same {@code timestamp(isAdjustedToUTC=false, MICROS)} for both
 * {@code TIMESTAMP_WITHOUT_TIME_ZONE} and {@code TIMESTAMP_WITH_LOCAL_TIME_ZONE}), so a target
 * DATETIME column would fail with a schema mismatch at load time.
 *
 * <ul>
 *   <li>Flink command line format is: <br>
 *       <code>flink run {additional runtime params} {path to this jar}/IndirectWriteExample.jar
 *       </code> <br>
 *       --project {required; GCP project ID} <br>
 *       --dataset {required; BigQuery dataset name} <br>
 *       --table {required; BigQuery table name} <br>
 *       --temp-gcs-path {required; GCS path for staging files} <br>
 *       --temp-project {required; GCP project that owns --temp-dataset} <br>
 *       --temp-dataset {required; BigQuery dataset for temporary tables created during
 *       multi-partition loads. Must exist in --temp-project. Recommended: configure a default
 *       tableExpirationMs on this dataset so any temp tables left behind by failed jobs are
 *       auto-deleted by BigQuery.}
 * </ul>
 *
 * <p>Requires the GCS Hadoop connector plugin ({@code flink-gs-fs-hadoop}) to be installed in the
 * Flink cluster's {@code plugins/} directory.
 */
public class IndirectWriteExample {

    private static final Logger LOG = LoggerFactory.getLogger(IndirectWriteExample.class);

    public static void main(String[] args) {
        final ParameterTool params = ParameterTool.fromArgs(args);

        if (params.getNumberOfParameters() < 6) {
            LOG.error(
                    "Missing parameters!\n"
                            + "Usage: IndirectWriteExample"
                            + " --project <GCP project>"
                            + " --dataset <BigQuery dataset>"
                            + " --table <BigQuery table>"
                            + " --temp-gcs-path <GCS path for staging files>"
                            + " --temp-project <GCP project for temp tables>"
                            + " --temp-dataset <BigQuery dataset for temp tables>");
            return;
        }

        String project = params.getRequired("project");
        String dataset = params.getRequired("dataset");
        String table = params.getRequired("table");
        String tempGcsPath = params.getRequired("temp-gcs-path");
        String tempProject = params.getRequired("temp-project");
        String tempDataset = params.getRequired("temp-dataset");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(2);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        tEnv.executeSql(
                String.format(
                        "CREATE TABLE bigquery_sink ("
                                + "    bool_col       BOOLEAN,"
                                + "    bytes_col      BYTES,"
                                + "    string_col     STRING,"
                                + "    int64_col      BIGINT,"
                                + "    float64_col    DOUBLE,"
                                + "    numeric_col    DECIMAL(38, 9),"
                                + "    bignumeric_col DECIMAL(38, 38),"
                                + "    date_col       DATE,"
                                + "    timestamp_col  TIMESTAMP_LTZ(6),"
                                + "    timestamp3_col TIMESTAMP_LTZ(3),"
                                + "    array_col      ARRAY<BIGINT>,"
                                + "    struct_col     ROW<a BIGINT, b STRING>"
                                + ") WITH ("
                                + "    'connector' = 'bigquery',"
                                + "    'project' = '%s',"
                                + "    'dataset' = '%s',"
                                + "    'table' = '%s',"
                                + "    'write.mode' = 'INDIRECT',"
                                + "    'write.indirect.temp-gcs-path' = '%s',"
                                + "    'write.indirect.temp-bigquery-project' = '%s',"
                                + "    'write.indirect.temp-bigquery-dataset' = '%s'"
                                + ")",
                        project, dataset, table, tempGcsPath, tempProject, tempDataset));

        String insertSql =
                "INSERT INTO bigquery_sink VALUES"
                        + "  (TRUE, X'cafebabe', 'hello world', 9223372036854775807,"
                        + "   3.141592653589793,"
                        + "   CAST('12345678901234567890123456789.123456789' AS DECIMAL(38, 9)),"
                        + "   CAST('0.12345678901234567890123456789012345678' AS DECIMAL(38, 38)),"
                        + "   DATE '2025-04-26',"
                        + "   CAST(TIMESTAMP '2025-04-26 12:34:56.123456' AS TIMESTAMP_LTZ(6)),"
                        + "   CAST(TIMESTAMP '2025-04-26 12:34:56.123' AS TIMESTAMP_LTZ(3)),"
                        + "   ARRAY[CAST(1 AS BIGINT), CAST(2 AS BIGINT), CAST(3 AS BIGINT)],"
                        + "   CAST(ROW(42, 'forty-two') AS ROW<a BIGINT, b STRING>)),"
                        + "  (FALSE, X'00', '', 0, 0.0,"
                        + "   CAST('0' AS DECIMAL(38, 9)),"
                        + "   CAST('-0.99999999999999999999999999999999999999' AS DECIMAL(38, 38)),"
                        + "   DATE '1970-01-01',"
                        + "   CAST(TIMESTAMP '1970-01-01 00:00:00' AS TIMESTAMP_LTZ(6)),"
                        + "   CAST(TIMESTAMP '1970-01-01 00:00:00' AS TIMESTAMP_LTZ(3)),"
                        + "   CAST(NULL AS ARRAY<BIGINT>),"
                        + "   CAST(NULL AS ROW<a BIGINT, b STRING>))";

        tEnv.executeSql(insertSql);
    }
}
