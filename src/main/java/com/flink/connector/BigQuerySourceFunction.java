package com.flink.connector;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.RuntimeContextInitializationContextAdapters;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.data.RowData;

import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;


public final class BigQuerySourceFunction extends RichSourceFunction<RowData> implements ResultTypeQueryable<RowData> {
	private static final long serialVersionUID = 1;

	private final String project_id;
	private final String dataset;
	private final String table;
	private final int snapshotMillis;
	private final DeserializationSchema<RowData> deserializer;
	public BigQuerySourceFunction(String project_id, String dataset, String table, int snapshotMillis,
			DeserializationSchema<RowData> deserializer) {
		this.project_id = project_id;
		this.dataset = dataset;
		this.table = table;
		this.snapshotMillis = snapshotMillis;
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
		BiqQueryReadSession readSession = new BiqQueryReadSession();
		ReadRows readRows = new ReadRows();
		try (BigQueryReadClient client = BigQueryReadClient.create()) {
			String streamName = readSession.createReadSession(client, project_id,dataset,table,snapshotMillis);
			readRows.createReadRowRequest(deserializer,streamName, client, ctx);
		}
	}

	@Override
	public void cancel() {
	}
}