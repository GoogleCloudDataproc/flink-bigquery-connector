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

import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import com.google.cloud.bigquery.storage.v1.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.functions.util.ListCollector;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReadRows {

  private static final Logger log = LoggerFactory.getLogger(ReadRows.class);

  public void createReadRowRequest(
      DeserializationSchema<RowData> deserializer,
      String streamName,
      BigQueryReadClient client,
      SourceContext<RowData> ctx) {
    ReadRowsRequest readRowsRequest =
        ReadRowsRequest.newBuilder().setReadStream(streamName).build();
    ServerStream<ReadRowsResponse> stream = client.readRowsCallable().call(readRowsRequest);
    List<RowData> outputCollector = new ArrayList<>();
    ListCollector<RowData> listCollector = new ListCollector<>(outputCollector);
    for (ReadRowsResponse response : stream) {
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

    for (int i = 0; i < outputCollector.size(); i++) {
      ctx.collect((RowData) outputCollector.get(i));
    }
  }
}
