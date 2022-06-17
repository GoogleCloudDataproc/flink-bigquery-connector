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
import java.io.IOException;
import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BigQuerySinkFunction implements SinkFunction<Row>, RichFunction {
  private static final long serialVersionUID = 1L;
  final Logger logger = LoggerFactory.getLogger(BigQuerySinkFunction.class);
  private BigQueryClientFactory bigQueryWriteClientFactory;
  private FlinkBigQueryConfig bqconfig;
  private BigQueryDirectDataWriterContext writeContext;
  private String[] fieldNames;
  private DataType[] fieldDataTypes;

  public BigQuerySinkFunction(
      String[] fieldNames,
      DataType[] fieldDataTypes,
      FlinkBigQueryConfig bqconfig,
      BigQueryClientFactory bigQueryWriteClientFactory) {
    this.fieldNames = fieldNames;
    this.fieldDataTypes = fieldDataTypes;
    this.bqconfig = bqconfig;
    this.bigQueryWriteClientFactory = bigQueryWriteClientFactory;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    this.writeContext =
        new BigQueryDirectDataWriterContext(
            fieldNames, fieldDataTypes, bqconfig, bigQueryWriteClientFactory);
  }

  @Override
  public void invoke(Row value, SinkFunction.Context context) {

    try {
      this.writeContext.write(value);
    } catch (IOException e) {
      logger.error("Error while wrting data using sink function");
      throw new FlinkBigQueryException("Error while wrting data using sink function", e);
    }
  }

  @Override
  public void close() throws Exception {
    this.writeContext.commit();
    this.writeContext.commitFinalizedStream();
    this.writeContext.abort();
  }

  @Override
  public RuntimeContext getRuntimeContext() {
    return null;
  }

  @Override
  public IterationRuntimeContext getIterationRuntimeContext() {
    return null;
  }

  @Override
  public void setRuntimeContext(RuntimeContext t) {}
}
