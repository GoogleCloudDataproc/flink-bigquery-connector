/*
 * Copyright 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.flink.bigquery;

import com.google.cloud.bigquery.connector.common.BigQueryClientFactory;
import com.google.cloud.flink.bigquery.common.WriterCommitMessageContext;
import com.google.cloud.flink.bigquery.util.FlinkBigQueryConfig;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.flink.api.common.accumulators.ListAccumulator;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DataStreamSinkProvider;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.abilities.SupportsOverwrite;
import org.apache.flink.table.connector.sink.abilities.SupportsPartitioning;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.utils.TableConnectorUtils;
import org.apache.flink.types.RowKind;

public class BigQueryDynamicTableSink
    implements DynamicTableSink, SupportsOverwrite, SupportsPartitioning {
  private List<String> fieldNames;
  private List<DataType> fieldTypes;
  private FlinkBigQueryConfig bqconfig;
  private BigQueryClientFactory bigQueryWriteClientFactory;
  private boolean overwrite = false;
  private int configuredParallelism;
  private LinkedHashMap<String, String> staticPartitions = new LinkedHashMap<>();
  private List<String> partitionKeys;
  private ListAccumulator<WriterCommitMessageContext> accumulator;

  public BigQueryDynamicTableSink(
      List<String> fieldNames,
      List<DataType> fieldTypes,
      FlinkBigQueryConfig bqconfig,
      BigQueryClientFactory bigQueryWriteClientFactory,
      List<String> partitionKeys,
      ListAccumulator<WriterCommitMessageContext> accumulator) {
    this.fieldNames = fieldNames;
    this.fieldTypes = fieldTypes;
    this.bqconfig = bqconfig;
    this.bigQueryWriteClientFactory = bigQueryWriteClientFactory;
    this.partitionKeys = partitionKeys;
    this.accumulator = accumulator;
  }

  @Override
  public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
    return ChangelogMode.newBuilder()
        .addContainedKind(RowKind.INSERT)
        .addContainedKind(RowKind.DELETE)
        .addContainedKind(RowKind.UPDATE_AFTER)
        .build();
  }

  @Override
  public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
    return (DataStreamSinkProvider) dataStream -> consume(dataStream, context);
  }

  private DataStreamSink<?> consume(DataStream<RowData> dataStream, Context sinkContext) {
    final int inputParallelism = dataStream.getParallelism();
    final int parallelism = Optional.ofNullable(configuredParallelism).orElse(inputParallelism);

    if (overwrite) {
      throw new IllegalStateException("Streaming mode not support overwrite.");
    }

    return createStreamingSink(dataStream, sinkContext, parallelism);
  }

  @SuppressWarnings("deprecation")
  private DataStreamSink<?> createStreamingSink(
      DataStream<RowData> dataStream, Context sinkContext, final int parallelism) {
    BigqueryOutputFormat outputFormat =
        new BigqueryOutputFormat(
            fieldNames, fieldTypes, bqconfig, bigQueryWriteClientFactory, accumulator);
    return dataStream
        .writeUsingOutputFormat(outputFormat)
        .setParallelism(dataStream.getParallelism())
        .name(
            TableConnectorUtils.generateRuntimeName(
                BigQueryDynamicTableSink.class, fieldNames.toArray(new String[0])));
  }

  @Override
  public DynamicTableSink copy() {
    BigQueryDynamicTableSink sink =
        new BigQueryDynamicTableSink(
            fieldNames,
            fieldTypes,
            bqconfig,
            bigQueryWriteClientFactory,
            partitionKeys,
            accumulator);
    sink.overwrite = overwrite;
    sink.staticPartitions = staticPartitions;
    return sink;
  }

  @Override
  public String asSummaryString() {
    return "BigQuery Sink";
  }

  @Override
  public void applyOverwrite(boolean overwrite) {
    this.overwrite = overwrite;
  }

  @Override
  public void applyStaticPartition(Map<String, String> partition) {
    this.staticPartitions = toPartialLinkedPartSpec(partition);
  }

  private LinkedHashMap<String, String> toPartialLinkedPartSpec(Map<String, String> part) {
    LinkedHashMap<String, String> partSpec = new LinkedHashMap<>();
    for (String partitionKey : partitionKeys) {
      if (part.containsKey(partitionKey)) {
        partSpec.put(partitionKey, part.get(partitionKey));
      }
    }
    return partSpec;
  }
}
