package com.flink.connector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.functions.util.ListCollector;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import com.google.cloud.bigquery.storage.v1.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;

public class ReadRows {
	
	public void createReadRowRequest(DeserializationSchema<RowData> deserializer, String streamName, BigQueryReadClient client, SourceContext<RowData> ctx) {
		ReadRowsRequest readRowsRequest = ReadRowsRequest.newBuilder().setReadStream(streamName).build();
		ServerStream<ReadRowsResponse> stream = client.readRowsCallable().call(readRowsRequest);
		List<RowData> outputCollector = new ArrayList<>();
		ListCollector<RowData> listCollector = new ListCollector<>(outputCollector);
		for (ReadRowsResponse response : stream) {
			Preconditions.checkState(response.hasArrowRecordBatch());
			long numOfRows = response.getRowCount();
			try {
				deserializer.deserialize(
						response.getArrowRecordBatch()
						.getSerializedRecordBatch()
						.toByteArray(), (Collector<RowData>) listCollector);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			for (int i=0;i<numOfRows;i++) {
				ctx.collect((RowData) outputCollector.get(i));
			}
		}
	}
	

}
