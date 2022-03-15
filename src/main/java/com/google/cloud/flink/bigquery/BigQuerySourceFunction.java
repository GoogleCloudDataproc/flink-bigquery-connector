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

<<<<<<< HEAD
import com.google.cloud.bigquery.connector.common.BigQueryClientFactory;
import com.google.cloud.bigquery.connector.common.ReadRowsHelper;
import com.google.cloud.bigquery.connector.common.ReadRowsHelper.Options;
import com.google.cloud.bigquery.storage.v1.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class BigQuerySourceFunction extends RichParallelSourceFunction<RowData>
    implements ResultTypeQueryable<RowData> {

  Boolean running = true;
  private static final long serialVersionUID = 1;
  private static final Logger log = LoggerFactory.getLogger(BigQuerySourceFunction.class);
  private int numOfStreams;
  private int numOfExecutors;
  private int executorIndex;
  private DeserializationSchema<RowData> deserializer;
  private LinkedList<String> readSessionStreamList = new LinkedList<>();
  private List<String> streamNames = new ArrayList<String>();
  private BigQueryClientFactory bigQueryReadClientFactory;

  public BigQuerySourceFunction(
      DeserializationSchema<RowData> deserializer,
      LinkedList<String> readSessionStreams,
      BigQueryClientFactory bigQueryReadClientFactory) {
    this.deserializer = deserializer;
    this.readSessionStreamList = readSessionStreams;
    this.bigQueryReadClientFactory = bigQueryReadClientFactory;
  }

  @Override
  public TypeInformation<RowData> getProducedType() {
    return deserializer.getProducedType();
  }

  @Override
  public void open(Configuration parameters) throws Exception {

    this.executorIndex = getRuntimeContext().getIndexOfThisSubtask();
    this.numOfExecutors = getRuntimeContext().getNumberOfParallelSubtasks();
    this.numOfStreams = readSessionStreamList.size();
    for (int i = executorIndex; i < numOfStreams; i += numOfExecutors) {
      if (running) {
        this.streamNames.add(readSessionStreamList.get(i));
      }
    }
  }

  @Override
  public void run(final SourceContext<RowData> ctx) throws Exception {
    Collector<RowData> collector = new SourceContextCollector<RowData>(ctx);
    Options options =
        new ReadRowsHelper.Options(
            /* maxRetries= */ 5,
            Optional.of("endpoint"),
            /* backgroundParsingThreads= */ 5,
            /* prebufferResponses= */ 1);

    for (String streamName : streamNames) {
      ReadRowsRequest.Builder readRowsRequest =
          ReadRowsRequest.newBuilder().setReadStream(streamName);
      try (ReadRowsHelper readRowsHelper =
          new ReadRowsHelper(bigQueryReadClientFactory, readRowsRequest, options)) {
        Iterator<ReadRowsResponse> readRows = readRowsHelper.readRows();
        while (readRows.hasNext()) {
          ReadRowsResponse response = readRows.next();
          try {
            if (response.hasArrowRecordBatch()) {
              Preconditions.checkState(response.hasArrowRecordBatch());
              deserializer.deserialize(
                  response.getArrowRecordBatch().getSerializedRecordBatch().toByteArray(),
                  collector);
            } else if (response.hasAvroRows()) {
              Preconditions.checkState(response.hasAvroRows());
              deserializer.deserialize(
                  response.getAvroRows().getSerializedBinaryRows().toByteArray(), collector);
              break;
            }
          } catch (IOException ex) {
            log.error("Error while deserialization", ex);
            throw new FlinkBigQueryException("Error while deserialization:", ex);
          }
        }
      }
    }
  }

  @Override
  public void cancel() {
    running = false;
  }

  static class SourceContextCollector<T> implements Collector<T> {

    private SourceContext<T> context;

    public SourceContextCollector(SourceContext<T> context) {
      this.context = context;
    }

    @Override
    public void collect(T t) {
      context.collect(t);
    }

    @Override
    public void close() {
      // no op as we don't want to close ctx
    }
  };
=======
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.RuntimeContextInitializationContextAdapters;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.bigquery.storage.v1.ReadSession;

public final class BigQuerySourceFunction extends RichSourceFunction<RowData> implements ResultTypeQueryable<RowData> {

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
		deserializer.open(RuntimeContextInitializationContextAdapters.deserializationAdapter(getRuntimeContext()));
	}

	@Override
	public void run(SourceContext<RowData> ctx) throws Exception {
		
		log.info("Get the data in Arrow format");
		ReadSession readSession = BigQueryDynamicTableFactory.readSession;
		log.info("Read Session object in Arrow format:"+readSession);		
	}

	@Override
	public void cancel() {
	}
>>>>>>> 63846ce (Flink big query - Arrow format support)
}
