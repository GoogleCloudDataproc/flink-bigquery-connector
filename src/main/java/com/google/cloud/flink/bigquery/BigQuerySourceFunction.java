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
import com.google.cloud.bigquery.connector.common.ReadRowsHelper;
import com.google.cloud.bigquery.connector.common.ReadRowsHelper.Options;
import com.google.cloud.bigquery.storage.v1.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import org.apache.flink.api.common.functions.util.ListCollector;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.RuntimeContextInitializationContextAdapters;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class BigQuerySourceFunction extends RichSourceFunction<RowData>
    implements ResultTypeQueryable<RowData> {

  private static final long serialVersionUID = 1;
  private static final Logger log = LoggerFactory.getLogger(BigQuerySourceFunction.class);

  private final DeserializationSchema<RowData> deserializer;
  private List<String> readSessionStreamList;
  private BigQueryClientFactory bigQueryReadClientFactory;

  public BigQuerySourceFunction(
      DeserializationSchema<RowData> deserializer,
      List<String> readSessionStreamList,
      BigQueryClientFactory bigQueryReadClientFactory) {
    this.deserializer = deserializer;
    this.readSessionStreamList = readSessionStreamList;
    this.bigQueryReadClientFactory = bigQueryReadClientFactory;
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

    List<RowData> outputCollector = new ArrayList<>();
    ListCollector<RowData> listCollector = new ListCollector<>(outputCollector);
    Options options =
        new ReadRowsHelper.Options(
            /* maxRetries= */ 5,
            Optional.of("endpoint"),
            /* backgroundParsingThreads= */ 5,
            /* prebufferResponses= */ 1);

    for (String streamName : readSessionStreamList) {

      ReadRowsRequest.Builder readRowsRequest =
          ReadRowsRequest.newBuilder().setReadStream(streamName);

      ReadRowsHelper readRowsHelper =
          new ReadRowsHelper(bigQueryReadClientFactory, readRowsRequest, options);
      Iterator<ReadRowsResponse> readRows = readRowsHelper.readRows();

      while (readRows.hasNext()) {

        ReadRowsResponse response = readRows.next();

        Preconditions.checkState(response.hasArrowRecordBatch());
        try {
          deserializer.deserialize(
              response.getArrowRecordBatch().getSerializedRecordBatch().toByteArray(),
              (Collector<RowData>) listCollector);
        } catch (IOException ex) {
          log.error("Error while deserialization");
          throw new FlinkBigQueryException("Error while deserialization:", ex);
        }
      }
      readRowsHelper.close();
    }

    for (int i = 0; i < outputCollector.size(); i++) {
      ctx.collect((RowData) outputCollector.get(i));
    }
  }

  @Override
  public void cancel() {}
}
