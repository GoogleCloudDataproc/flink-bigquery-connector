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
import com.google.cloud.bigquery.storage.v1.BatchCommitWriteStreamsRequest;
import com.google.cloud.bigquery.storage.v1.BatchCommitWriteStreamsResponse;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.StorageError;
import com.google.cloud.flink.bigquery.common.BigQueryDirectWriterCommitMessageContext;
import com.google.cloud.flink.bigquery.common.WriterCommitMessageContext;
import com.google.cloud.flink.bigquery.exception.FlinkBigQueryException;
import com.google.cloud.flink.bigquery.util.FlinkBigQueryConfig;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import net.sf.jsqlparser.JSQLParserException;
import org.apache.flink.api.common.accumulators.ListAccumulator;
import org.apache.flink.api.common.io.FinalizeOnMaster;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters.RowConverter;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Runtime execution that writes data to big query using write streams */
public class BigqueryOutputFormat extends RichOutputFormat<RowData>
    implements FinalizeOnMaster, Serializable {
  private static final long serialVersionUID = 1L;
  final Logger logger = LoggerFactory.getLogger(BigqueryOutputFormat.class);
  private BigQueryClientFactory bigQueryWriteClientFactory;
  private FlinkBigQueryConfig bqConfig;
  private BigQueryDirectDataWriterContext writeContext;
  private List<String> fieldNames;
  private List<DataType> fieldDataTypes;
  private static ListAccumulator<WriterCommitMessageContext> accumulator;

  public BigqueryOutputFormat(
      List<String> fieldNames,
      List<DataType> fieldDataTypes,
      FlinkBigQueryConfig bqconfig,
      BigQueryClientFactory bigQueryWriteClientFactory,
      ListAccumulator<WriterCommitMessageContext> accumulator) {
    this.fieldNames = fieldNames;
    this.fieldDataTypes = fieldDataTypes;
    this.bqConfig = bqconfig;
    this.bigQueryWriteClientFactory = bigQueryWriteClientFactory;
    BigqueryOutputFormat.accumulator = accumulator;
  }

  @Override
  public void finalizeGlobal(int parallelism) throws IOException {
    ArrayList<WriterCommitMessageContext> writerCommitMessageContexts = accumulator.getLocalValue();
    if (parallelism == writerCommitMessageContexts.size()) {
      List<String> writeStreamNames = new ArrayList<String>();
      List<String> tablePaths = new ArrayList<String>();
      List<Boolean> commitNotAllowedList = new ArrayList<Boolean>();
      writerCommitMessageContexts.stream()
          .forEach(
              writerCommitMessageContext -> {
                BigQueryDirectWriterCommitMessageContext bqWriterCommitMessageContext =
                    (BigQueryDirectWriterCommitMessageContext) writerCommitMessageContext;
                if (bqWriterCommitMessageContext.getStatus()) {
                  commitNotAllowedList.add(false);
                }
                tablePaths.add(bqWriterCommitMessageContext.getTablePath());
                writeStreamNames.add(bqWriterCommitMessageContext.getWriteStreamName());
              });
      if (commitNotAllowedList.isEmpty()) {
        try (BigQueryWriteClient bigQueryWriteClient =
            bigQueryWriteClientFactory.getBigQueryWriteClient()) {
          BatchCommitWriteStreamsRequest commitRequest =
              BatchCommitWriteStreamsRequest.newBuilder()
                  .addAllWriteStreams(writeStreamNames)
                  .setParent(tablePaths.get(0))
                  .build();
          BatchCommitWriteStreamsResponse commitResponse =
              bigQueryWriteClient.batchCommitWriteStreams(commitRequest);

          if (commitResponse.hasCommitTime() == false) {
            for (StorageError err : commitResponse.getStreamErrorsList()) {
              logger.error(err.getErrorMessage());
            }
            throw new RuntimeException("Error committing the streams");
          }

          logger.debug("Write-streams {} committed successfully.", writeStreamNames);
          logger.info("Committed Write-streams " + writeStreamNames.toString() + " successfully.");
        }
      }
    }
  }

  @Override
  public void configure(Configuration parameters) {}

  @Override
  public void open(int taskNumber, int numTasks) throws IOException {
    try {
      this.writeContext =
          new BigQueryDirectDataWriterContext(
              taskNumber, fieldNames, fieldDataTypes, bqConfig, bigQueryWriteClientFactory);
    } catch (JSQLParserException e) {
      logger.error("Error while parsing write stream");
      throw new FlinkBigQueryException("Error while parsing write stream ", e);
    }
  }

  @Override
  public void writeRecord(RowData record) throws IOException {
    try {
      RowConverter rowRowConverter = new RowConverter(fieldDataTypes.toArray(new DataType[0]));
      Row row = rowRowConverter.toExternal((RowData) record);
      this.writeContext.write(row);
    } catch (IOException e) {
      logger.error("Error while writing data using sink function");
      throw new FlinkBigQueryException("Error while writing data ", e);
    }
  }

  @Override
  public void close() throws IOException {
    WriterCommitMessageContext writerCommitMessageContext = null;
    try {
      writerCommitMessageContext = this.writeContext.commit();
    } catch (IOException e) {
      writerCommitMessageContext = new BigQueryDirectWriterCommitMessageContext("", "", 0, 0, true);
      logger.error("Error while Finalising the Stream");
    } finally {
      accumulator.add(writerCommitMessageContext);
    }
  }
}
