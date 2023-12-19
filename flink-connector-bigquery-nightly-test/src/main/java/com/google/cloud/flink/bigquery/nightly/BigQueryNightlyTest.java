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

package com.google.cloud.flink.bigquery.nightly;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.hybrid.HybridSource;
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
 * A nightly job to test BigQuery table read with Flink's DataStream API.
 *
 * <p>The Flink pipeline will try to read the specified BigQuery table according to given the
 * command line arguments, returning {@link GenericRecord} representing the rows, perform certain
 * operations and then log the total number of records read.
 *
 * <p>This module checks the following cases of BigQuery Table read.
 *
 * <ol>
 *   <li> Bounded Jobs: Involve reading a BigQuery Table in the <i> bounded </i> mode.<br>
 *   The arguments given in this case would be:
 *     <ul>
 *       <li>
 *         --gcp-project {required; project ID which contains the BigQuery table}
 *       </li>
 *       <li>
 *         --bq-dataset {required; name of BigQuery dataset containing the desired table} <br>
 *       </li>
 *       <li>
 *         --bq-table {required; name of BigQuery table to read} <br>
 *       </li>
 *       <li>
 *         --agg-prop {required; record property to aggregate in Flink job} <br>
 *       </li>
 *       <li>
 *         --query {optional; SQL query to fetch data from BigQuery table}
 *       </li>
 *     </ul>
 *     The sequence of operations in this pipeline is: <i>source > flatMap > keyBy > sum </i> <br>
 *       A counter counts the total number of records read
 *       (the number of records observed by keyBy operation) and logs this count at the end. <br>
 *     Command to run bounded tests on Dataproc Cluster is: <br>
 *  {@code gcloud dataproc jobs submit flink --id {JOB_ID} --jar= {GCS_JAR_LOCATION} --cluster={CLUSTER_NAME}
 *      --region={REGION} -- --gcp-project {GCP_PROJECT_ID} --bq-dataset {BigQuery Dataset Name}
 *      --bq-table {BigQuery Table Name} --agg-prop {PROPERTY_TO_AGGREGATE_ON}  --query {QUERY} }
 *   <p>
 *   The following cases are tested:
 *   <ol>
 *       <li>
 *          Reading a Simple Table: This test reads a simple table of 40,000 rows having size 900 KBs.
 *       </li>
 *       <li>
 *          Reading a Table with Complex Schema: This test reads a table with 15 levels (maximum number of levels allowed by BigQuery). The table contains 100,000 rows and has a size of 2.96 MB.
 *       </li>
 *       <li>
 *          Reading a Large Table: This test reads a large table. The table contains __ rows and has a size of about 200 GBs.
 *       </li>
 *       <li>
 *         Reading a Table with Large Row: This test reads a table with a large row. The table contains 100 rows each fo size 45 MB and has a size of about 450 GB.
 *       </li>
 *       <li>
 *        Testing a BigQuery Query Run: This tests a BigQuery Query run. The query filters certain rows based on a condition, groups the records and finds the AVG of value of a column.
 *       </li>
 *   </ol>
 *   </li>
 *   <li>
 *     Unbounded Source Job: Involve reading a BigQuery Table in the <i> unbounded </i> mode.<br>
 *     This test requires some additional arguments besides the ones mentioned in the bounded mode.
 *      <ul>
 *      <li>
 *        --ts-prop {property record for timestamp}
 *      </li>
 *      <li>
 *        --partition-discovery-interval {optional; minutes between polling table for new data. Used
 *        in unbounded/hybrid mode} <br>
 *      </li>
 *      <li>
 *          --mode {unbounded in this case}.
 *      </li>
 *    </ul>
 *    The sequence of operations in this pipeline is the same as bounded one.
 *      This job is run asynchronously. The test appends newer partitions to check the read
 *      correctness. Hence, after the job is created new partitions are added. <br>
 *    Command to run unbounded tests on Dataproc Cluster is: <br>
 * {@code gcloud dataproc jobs submit flink --id {JOB_ID} --jar= {GCS_JAR_LOCATION} --cluster={CLUSTER_NAME}
 *     --region={REGION} --async -- --gcp-project {GCP_PROJECT_ID} --bq-dataset {BigQuery Dataset Name}
 *     --bq-table {BigQuery Table Name} --agg-prop {PROPERTY_TO_AGGREGATE_ON} --mode unbounded --ts-prop {TIMESTAMP_PROPERTY}  --partition-discovery-interval {PARTITION_DISCOVERY_INTERVAL} }
 *  <p>
 *   </li>
 *
 */
public class BigQueryNightlyTest {

    private static final Logger LOG = LoggerFactory.getLogger(BigQueryNightlyTest.class);

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
                            + " --restriction <row filter predicate>"
                            + " --limit <limit on records returned>"
                            + " --checkpoint-interval <milliseconds between state checkpoints>"
                            + " --query <SQL query to get data from BQ table>"
                            + " --ts-prop <timestamp property>"
                            + " --oldest-partition-id <oldest partition to read>"
                            + " --partition-discovery-interval <minutes between checking new data>"
                            + " --out-of-order-tolerance <minutes to accept out of order records>"
                            + " --max-idleness <maximum idle minutes for read stream>"
                            + " --window-size <Flink's window size in minutes>");
            return;
        }
        /**
         * we will be reading avro generic records from BigQuery, and in this case we are assuming
         * the GOOGLE_APPLICATION_CREDENTIALS env variable will be present in the execution runtime.
         * In case of needing authenticate differently, the credentials builder (part of the
         * BigQueryConnectOptions) should enable capturing the credentials from various sources.
         */
        String projectName = parameterTool.getRequired("gcp-project");
        String query = parameterTool.get("query", "");
        Integer recordLimit = parameterTool.getInt("limit", -1);
        Long checkpointInterval = parameterTool.getLong("checkpoint-interval", 60000L);
				String recordPropertyToAggregate = parameterTool.getRequired("agg-prop");

			if (!query.isEmpty()) {
            runQueryFlinkJob(projectName, query, recordLimit, checkpointInterval, recordPropertyToAggregate);
            return;
        }
        String datasetName = parameterTool.getRequired("bq-dataset");
        String tableName = parameterTool.getRequired("bq-table");
        String rowRestriction = parameterTool.get("restriction", "").replace("\\u0027", "'");
        String mode = parameterTool.get("mode", "bounded");
        String oldestPartition = parameterTool.get("oldest-partition-id", "");
        Integer partitionDiscoveryInterval =
                parameterTool.getInt("partition-discovery-interval", 10);
        Integer maxOutOfOrder = parameterTool.getInt("out-of-order-tolerance", 10);
        Integer maxIdleness = parameterTool.getInt("max-idleness", 20);
        Integer windowSize = parameterTool.getInt("window-size", 1);

        String recordPropertyForTimestamps;
        switch (mode) {
            case "bounded":
                runBoundedFlinkJob(
                        projectName,
                        datasetName,
                        tableName,
                        recordPropertyToAggregate,
                        rowRestriction,
                        recordLimit,
                        checkpointInterval);
                break;
            case "unbounded":
                recordPropertyForTimestamps = parameterTool.getRequired("ts-prop");
                runStreamingFlinkJob(
                        projectName,
                        datasetName,
                        tableName,
                        recordPropertyToAggregate,
                        recordPropertyForTimestamps,
                        rowRestriction,
                        recordLimit,
                        checkpointInterval,
                        oldestPartition,
                        partitionDiscoveryInterval,
                        maxOutOfOrder,
                        maxIdleness,
                        windowSize);
                break;
            case "hybrid":
                recordPropertyForTimestamps = parameterTool.getRequired("ts-prop");
                runHybridFlinkJob(
                        projectName,
                        datasetName,
                        tableName,
                        recordPropertyToAggregate,
                        recordPropertyForTimestamps,
                        rowRestriction,
                        checkpointInterval,
                        oldestPartition,
                        partitionDiscoveryInterval,
                        maxOutOfOrder,
                        maxIdleness,
                        windowSize);
                break;
            default:
                throw new IllegalArgumentException(
                        "Allowed values for mode are bounded, unbounded or hybrid. Found " + mode);
        }
    }

    private static void runQueryFlinkJob(
            String projectName, String query, Integer limit, Long checkpointInterval, String recordPropertyToAggregate)
            throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(checkpointInterval);

        BigQuerySource<GenericRecord> bqSource =
                BigQuerySource.readAvrosFromQuery(query, projectName, limit);

        env.fromSource(bqSource, WatermarkStrategy.noWatermarks(), "BigQueryQuerySource").flatMap(new FlatMapper(recordPropertyToAggregate))
								.keyBy(mappedTuple -> mappedTuple.f0)
								.sum("f1");

        env.execute("Flink BigQuery Query Nightly Test");
    }

    private static void runJob(
            Source<GenericRecord, ?, ?> source,
            TypeInformation<GenericRecord> typeInfo,
            String sourceName,
            String jobName,
            String recordPropertyToAggregate,
            String recordPropertyForTimestamps,
            Long checkpointInterval,
            Integer maxOutOfOrder,
            Integer maxIdleness,
            Integer windowSize)
            throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(checkpointInterval);

        env.fromSource(
                        source,
                        WatermarkStrategy.<GenericRecord>forBoundedOutOfOrderness(
                                        Duration.ofMinutes(maxOutOfOrder))
                                .withTimestampAssigner(
                                        (event, timestamp) ->
                                                (Long) event.get(recordPropertyForTimestamps))
                                .withIdleness(Duration.ofMinutes(maxIdleness)),
                        sourceName,
                        typeInfo)
                .flatMap(new FlatMapper(recordPropertyToAggregate))
                .keyBy(mappedTuple -> mappedTuple.f0)
                .window(TumblingEventTimeWindows.of(Time.minutes(windowSize)))
                .sum("f1");

        env.execute(jobName);
    }

    private static void runBoundedFlinkJob(
            String projectName,
            String datasetName,
            String tableName,
            String recordPropertyToAggregate,
            String rowRestriction,
            Integer limit,
            Long checkpointInterval)
            throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(checkpointInterval);

        BigQuerySource<GenericRecord> source =
                BigQuerySource.readAvros(
                        BigQueryReadOptions.builder()
                                .setBigQueryConnectOptions(
                                        BigQueryConnectOptions.builder()
                                                .setProjectId(projectName)
                                                .setDataset(datasetName)
                                                .setTable(tableName)
                                                .build())
                                .setRowRestriction(rowRestriction)
                                .setLimit(limit)
                                .build());
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "BigQuerySource")
                .flatMap(new FlatMapper(recordPropertyToAggregate))
                .keyBy(mappedTuple -> mappedTuple.f0)
                .sum("f1");

        env.execute("Flink BigQuery Bounded Read Nightly Test");
    }

    private static void runStreamingFlinkJob(
            String projectName,
            String datasetName,
            String tableName,
            String recordPropertyToAggregate,
            String recordPropertyForTimestamps,
            String rowRestriction,
            Integer limit,
            Long checkpointInterval,
            String oldestPartition,
            Integer partitionDiscoveryInterval,
            Integer maxOutOfOrder,
            Integer maxIdleness,
            Integer windowSize)
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
                                .setRowRestriction(rowRestriction)
                                .setLimit(limit)
                                .setOldestPartitionId(oldestPartition)
                                .setPartitionDiscoveryRefreshIntervalInMinutes(
                                        partitionDiscoveryInterval)
                                .build());

        runJob(
                source,
                source.getProducedType(),
                "BigQueryStreamingSource",
                "Flink BigQuery Unbounded Read Nightly Test",
                recordPropertyToAggregate,
                recordPropertyForTimestamps,
                checkpointInterval,
                maxOutOfOrder,
                maxIdleness,
                windowSize);
    }

    private static void runHybridFlinkJob(
            String projectName,
            String datasetName,
            String tableName,
            String recordPropertyToAggregate,
            String recordPropertyForTimestamps,
            String rowRestrictionForBatch,
            Long checkpointInterval,
            String oldestPartitionForStreaming,
            Integer partitionDiscoveryInterval,
            Integer maxOutOfOrder,
            Integer maxIdleness,
            Integer windowSize)
            throws Exception {

        // we will be reading the historical batch data as the restriction shared from command line.
        BigQuerySource<GenericRecord> batchSource =
                BigQuerySource.readAvros(
                        BigQueryReadOptions.builder()
                                .setBigQueryConnectOptions(
                                        BigQueryConnectOptions.builder()
                                                .setProjectId(projectName)
                                                .setDataset(datasetName)
                                                .setTable(tableName)
                                                .build())
                                .setRowRestriction(rowRestrictionForBatch)
                                .build());

        // and then reading the new data from the streaming source, as it gets available from the
        // underlying BigQuery table.
        BigQuerySource<GenericRecord> streamingSource =
                BigQuerySource.streamAvros(
                        BigQueryReadOptions.builder()
                                .setBigQueryConnectOptions(
                                        BigQueryConnectOptions.builder()
                                                .setProjectId(projectName)
                                                .setDataset(datasetName)
                                                .setTable(tableName)
                                                .build())
                                .setOldestPartitionId(oldestPartitionForStreaming)
                                .setPartitionDiscoveryRefreshIntervalInMinutes(
                                        partitionDiscoveryInterval)
                                .build());

        // create an hybrid source with both batch and streaming flavors of the BigQuery source.
        HybridSource<GenericRecord> hybridSource =
                HybridSource.builder(batchSource).addSource(streamingSource).build();

        runJob(
                hybridSource,
                streamingSource.getProducedType(),
                "BigQueryHybridSource",
                "Flink BigQuery Hybrid Read Nightly Test",
                recordPropertyToAggregate,
                recordPropertyForTimestamps,
                checkpointInterval,
                maxOutOfOrder,
                maxIdleness,
                windowSize);
    }

    static class FlatMapper extends RichFlatMapFunction<GenericRecord, Tuple2<String, Integer>> {

        private final String recordPropertyToAggregate;

        private transient Counter counter;

        @Override
        public void open(Configuration config) {
               this.counter = getRuntimeContext()
                      .getMetricGroup()
                      .counter("number_of_records_counter_map");
        }

        public FlatMapper(String recordPropertyToAggregate) {
               this.recordPropertyToAggregate = recordPropertyToAggregate;
        }

        @Override
        public void flatMap(GenericRecord record, Collector<Tuple2<String, Integer>> out)
                throws Exception {
					     this.counter.inc();
               out.collect(Tuple2.of(String.valueOf(
											 (record.get(recordPropertyToAggregate).toString()).hashCode() % 1000), 1));
        }

        @Override
        public void close() throws Exception {
               LOG.info("Number of records read: " + this.counter.getCount() + ";");
        }
    }
}
