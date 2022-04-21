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

import java.io.IOException;
import net.sf.jsqlparser.JSQLParserException;
import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BigQuerySinkFunction implements SinkFunction<Row>, RichFunction {
  private static final long serialVersionUID = 1L;
  final Logger logger = LoggerFactory.getLogger(BigQuerySinkFunction.class);
  private static BigQueryDirectDataWriterContext writeContext;

  public BigQuerySinkFunction(
      org.apache.flink.table.api.Table sourceResultTable,
      String projectId,
      String dataset,
      String table)
      throws JSQLParserException {
    BigQueryDirectDataWriterContext writeContextObj =
        new BigQueryDirectDataWriterContext(sourceResultTable, projectId, dataset, table);
    writeContext = writeContextObj;
  }

  public BigQuerySinkFunction(Table sourceResultTable, String bigqueryWriteTable) {
    String[] tableProperties = bigqueryWriteTable.split("\\.");
    String projectId = tableProperties[0];
    String dataset = tableProperties[1];
    String table = tableProperties[2];
    try {
      new BigQuerySinkFunction(sourceResultTable, projectId, dataset, table);
    } catch (JSQLParserException e) {
      logger.error("Error while creating sink function");
    }
  }

  @Override
  public void invoke(Row value, SinkFunction.Context context) {
    try {
      this.writeContext.write(value);
    } catch (IOException e) {
      logger.error("Error while wrting data using sink function");
    }
  }

  @Override
  public void open(Configuration parameters) throws Exception {}

  @Override
  public void close() throws Exception {
    this.writeContext.finalizeStream();
    this.writeContext.commit();
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
