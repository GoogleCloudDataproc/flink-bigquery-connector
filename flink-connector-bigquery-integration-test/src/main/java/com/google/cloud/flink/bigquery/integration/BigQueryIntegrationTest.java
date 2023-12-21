/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.flink.bigquery.integration;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;
import com.google.cloud.flink.bigquery.source.BigQuerySource;
import com.google.cloud.flink.bigquery.source.config.BigQueryReadOptions;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * The Integration Test pipeline will try to read the specified BigQuery table according to the
 * command line arguments, returning {@link GenericRecord} representing the rows, perform certain
 * operations and then log the total number of records read. <br>
 * This module checks the following cases of BigQuery Table read.
 *
 * <ol>
 *   <li>Bounded Jobs: Involve reading a BigQuery Table in the <i> bounded </i> mode.<br>
 *       The arguments given in this case would be:
 *       <ul>
 *         <li>--gcp-project {required; project ID which contains the BigQuery table}
 *         <li>--bq-dataset {required; name of BigQuery dataset containing the desired table} <br>
 *         <li>--bq-table {required; name of BigQuery table to read} <br>
 *         <li>--agg-prop {required; record property to aggregate in Flink job} <br>
 *         <li>--query {optional; SQL query to fetch data from BigQuery table}
 *       </ul>
 *       The sequence of operations in this pipeline is: <i>source > flatMap > keyBy > sum </i> <br>
 *       A counter counts the total number of records read (the number of records observed by keyBy
 *       operation) and logs this count at the end. <br>
 *       Command to run bounded tests on Dataproc Cluster is: <br>
 *       {@code gcloud dataproc jobs submit flink --id {JOB_ID} --jar= {GCS_JAR_LOCATION}
 *       --cluster={CLUSTER_NAME} --region={REGION} -- --gcp-project {GCP_PROJECT_ID} --bq-dataset
 *       {BigQuery Dataset Name} --bq-table {BigQuery Table Name} --agg-prop
 *       {PROPERTY_TO_AGGREGATE_ON} --query {QUERY} } <br>
 *       The following cases are tested:
 *       <ol>
 *         <li>Reading a Simple Table: This test reads a simple table of 40,000 rows having size 900
 *             KBs.
 *         <li>Reading a Table with Complex Schema: This test reads a table with 15 levels (maximum
 *             number of levels allowed by BigQuery). The table contains 100,000 rows and has a size
 *             of 2.96 MB.
 *         <li>Reading a Large Table: This test reads a large table. The table contains __ rows and
 *             has a size of about 200 GBs.
 *         <li>Reading a Table with Large Row: This test reads a table with a large row. The table
 *             contains 100 rows each fo size 45 MB and has a size of about 450 GB.
 *         <li>Testing a BigQuery Query Run: This tests a BigQuery Query run. The query filters
 *             certain rows based on a condition, groups the records and finds the AVG of value of a
 *             column.
 *       </ol>
 *   <li>Unbounded Source Job: Involve reading a BigQuery Table in the <i> unbounded </i> mode.<br>
 *       This test requires some additional arguments besides the ones mentioned in the bounded
 *       mode.
 *       <ul>
 *         <li>--ts-prop {property record for timestamp}
 *         <li>--partition-discovery-interval {optional; minutes between polling table for new data.
 *             Used in unbounded/hybrid mode} <br>
 *         <li>--mode {unbounded in this case}.
 *       </ul>
 *       The sequence of operations in this pipeline is the same as bounded one. This job is run
 *       asynchronously. The test appends newer partitions to check the read correctness. Hence,
 *       after the job is created new partitions are added. <br>
 *       Command to run unbounded tests on Dataproc Cluster is: <br>
 *       {@code gcloud dataproc jobs submit flink --id {JOB_ID} --jar= {GCS_JAR_LOCATION}
 *       --cluster={CLUSTER_NAME} --region={REGION} --async -- --gcp-project {GCP_PROJECT_ID}
 *       --bq-dataset {BigQuery Dataset Name} --bq-table {BigQuery Table Name} --agg-prop
 *       {PROPERTY_TO_AGGREGATE_ON} --mode unbounded --ts-prop {TIMESTAMP_PROPERTY}
 *       --partition-discovery-interval {PARTITION_DISCOVERY_INTERVAL} }
 * </ol>
 */
public class BigQueryIntegrationTest {

    private static final Logger LOG = LoggerFactory.getLogger(BigQueryIntegrationTest.class);
    private static final Integer RECORD_LIMIT = -1;
    private static final Long CHECKPOINT_INTERVAL = 60000L;
    private static final String ROW_RESTRICTION = "";
    private static final Integer MAX_OUT_OF_ORDER = 10;
    private static final Integer MAX_IDLENESS = 20;
    private static final Integer WINDOW_SIZE = 1;
    private static final String OLDEST_PARTITION = "";

    public static void main(String[] args) throws Exception {
        // parse input arguments
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);

        if (parameterTool.getNumberOfParameters() < 1) {
            LOG.error(
                    "Missing parameters!\n"
                            + "Usage: flink run <additional runtime params> <jar>"
                            + " --gcp-project <gcp project id>"
                            + " --bq-dataset <dataset name>"
                            + " --bq-table <table name>"
                            + " --agg-prop <record property to aggregate>"
                            + " --mode <source type>"
                            + " --query <SQL query to get data from BQ table>"
                            + " --ts-prop <timestamp property>"
                            + " --partition-discovery-interval <minutes between checking new data>");
            return;
        }
        String projectName = parameterTool.getRequired("gcp-project");
        String query = parameterTool.get("query", "");

        if (!query.isEmpty()) {
            runQueryFlinkJob(projectName, query);
            return;
        }
        String datasetName = parameterTool.getRequired("bq-dataset");
        String tableName = parameterTool.getRequired("bq-table");
        String mode = parameterTool.get("mode", "bounded");
        String recordPropertyToAggregate = parameterTool.getRequired("agg-prop");

        Integer partitionDiscoveryInterval =
                parameterTool.getInt("partition-discovery-interval", 10);

        String recordPropertyForTimestamps;
        switch (mode) {
            case "bounded":
                runBoundedFlinkJob(projectName, datasetName, tableName, recordPropertyToAggregate);
                break;
            case "unbounded":
                recordPropertyForTimestamps = parameterTool.getRequired("ts-prop");
                runStreamingFlinkJob(
                        projectName,
                        datasetName,
                        tableName,
                        recordPropertyToAggregate,
                        recordPropertyForTimestamps,
                        partitionDiscoveryInterval);
                break;
            default:
                throw new IllegalArgumentException(
                        "Allowed values for mode are bounded, unbounded. Found " + mode);
        }
    }

    private static void runQueryFlinkJob(String projectName, String query) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(CHECKPOINT_INTERVAL);

        BigQuerySource<GenericRecord> bqSource =
                BigQuerySource.readAvrosFromQuery(query, projectName, RECORD_LIMIT);

        env.fromSource(bqSource, WatermarkStrategy.noWatermarks(), "BigQueryQuerySource")
                .map(new Mapper())
                .print();

        env.execute("Flink BigQuery Query Integration Test");
    }

    private static void runJob(
            Source<GenericRecord, ?, ?> source,
            TypeInformation<GenericRecord> typeInfo,
            String recordPropertyToAggregate,
            String recordPropertyForTimestamps)
            throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(CHECKPOINT_INTERVAL);

        env.fromSource(
                        source,
                        WatermarkStrategy.<GenericRecord>forBoundedOutOfOrderness(
                                        Duration.ofMinutes(MAX_OUT_OF_ORDER))
                                .withTimestampAssigner(
                                        (event, timestamp) ->
                                                (Long) event.get(recordPropertyForTimestamps))
                                .withIdleness(Duration.ofMinutes(MAX_IDLENESS)),
                        "BigQueryStreamingSource",
                        typeInfo)
                .flatMap(new FlatMapper(recordPropertyToAggregate))
                .keyBy(mappedTuple -> mappedTuple.f0)
                .window(TumblingEventTimeWindows.of(Time.minutes(WINDOW_SIZE)))
                .sum("f1");

        String jobName = "Flink BigQuery Unbounded Read Integration Test";
        env.execute(jobName);
    }

    private static void runBoundedFlinkJob(
            String projectName,
            String datasetName,
            String tableName,
            String recordPropertyToAggregate)
            throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(CHECKPOINT_INTERVAL);

        BigQuerySource<GenericRecord> source =
                BigQuerySource.readAvros(
                        BigQueryReadOptions.builder()
                                .setBigQueryConnectOptions(
                                        BigQueryConnectOptions.builder()
                                                .setProjectId(projectName)
                                                .setDataset(datasetName)
                                                .setTable(tableName)
                                                .build())
                                .setRowRestriction(ROW_RESTRICTION)
                                .setLimit(RECORD_LIMIT)
                                .build());

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "BigQueryQuerySource")
                .flatMap(new FlatMapper(recordPropertyToAggregate))
                .keyBy(mappedTuple -> mappedTuple.f0)
                .sum("f1");

        env.execute("Flink BigQuery Bounded Read Integration Test");
    }

    private static void runStreamingFlinkJob(
            String projectName,
            String datasetName,
            String tableName,
            String recordPropertyToAggregate,
            String recordPropertyForTimestamps,
            Integer partitionDiscoveryInterval)
            throws Exception {

        BigQuerySource<GenericRecord> source =
                BigQuerySource.streamAvros(
                        BigQueryReadOptions.builder()
                                .setBigQueryConnectOptions(
                                        BigQueryConnectOptions.builder()
                                                .setProjectId(projectName)
                                                .setDataset(datasetName)
                                                .setTable(tableName)
                                                .build())
                                .setRowRestriction(ROW_RESTRICTION)
                                .setLimit(RECORD_LIMIT)
                                .setOldestPartitionId(OLDEST_PARTITION)
                                .setPartitionDiscoveryRefreshIntervalInMinutes(
                                        partitionDiscoveryInterval)
                                .build());

        runJob(
                source,
                source.getProducedType(),
                recordPropertyToAggregate,
                recordPropertyForTimestamps);
    }

    static class FlatMapper extends RichFlatMapFunction<GenericRecord, Tuple2<String, Integer>> {

        private final String recordPropertyToAggregate;

        private transient Counter counter;

        @Override
        public void open(Configuration config) {
            this.counter =
                    getRuntimeContext().getMetricGroup().counter("number_of_records_counter_map");
        }

        public FlatMapper(String recordPropertyToAggregate) {
            this.recordPropertyToAggregate = recordPropertyToAggregate;
        }

        @Override
        public void flatMap(GenericRecord readRecord, Collector<Tuple2<String, Integer>> out)
                throws Exception {
            this.counter.inc();

            out.collect(
                    Tuple2.of(
                            String.valueOf(
                                    (readRecord.get(recordPropertyToAggregate).toString())
                                            .hashCode()
                                            % 1000),
                            1));
        }

        @Override
        public void close() throws Exception {
            LOG.info("Number of records read: {} ;", this.counter.getCount());
        }
    }

    static class Mapper extends RichMapFunction<GenericRecord, String> {

        private transient Counter counter;

        @Override
        public void open(Configuration config) {
            this.counter =
                    getRuntimeContext()
                            .getMetricGroup()
                            .counter("number_of_records_counter_query_map");
        }

        @Override
        public void close() throws Exception {
            LOG.info("Number of records read: {} ;", this.counter.getCount());
        }

        @Override
        public String map(GenericRecord value) throws Exception {
            this.counter.inc();
            return "[ " + value.get("HOUR") + ", " + value.get("DAY") + " ]";
        }
    }
}
