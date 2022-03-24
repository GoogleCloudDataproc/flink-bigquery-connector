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

import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.RuntimeContextInitializationContextAdapters;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class BigQuerySourceFunction extends RichSourceFunction<RowData>
    implements ResultTypeQueryable<RowData> {

  private static final long serialVersionUID = 1;
  private static final Logger log = LoggerFactory.getLogger(BigQuerySourceFunction.class);

  private final DeserializationSchema<RowData> deserializer;

  public BigQuerySourceFunction(DeserializationSchema<RowData> deserializer) {
    this.deserializer = deserializer;
  }

  @Override
  public TypeInformation<RowData> getProducedType() {
    return deserializer.getProducedType();
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    deserializer.open(
        RuntimeContextInitializationContextAdapters.deserializationAdapter(getRuntimeContext()));
  }

  @Override
  public void run(SourceContext<RowData> ctx) throws Exception {

    BigQueryReadClient client = BigQueryReadClient.create();
    ReadSession readSession = BigQueryDynamicTableFactory.readSession;
    String streamName = readSession.getStreams(0).getName();
    ReadRows readRows = new ReadRows();
    readRows.createReadRowRequest(deserializer, streamName, client, ctx);
  }

  @Override
  public void cancel() {}
}
