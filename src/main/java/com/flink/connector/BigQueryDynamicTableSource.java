package com.flink.connector;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;


public final class BigQueryDynamicTableSource implements ScanTableSource {
	private static final long serialVersionUID = 1;
	private final String project_id;
	private final String dataset;
	private final String table;
	private final int snapshotMillis;
	private final DecodingFormat<DeserializationSchema<RowData>> decodingFormat;
	private final DataType producedDataType;
	public BigQueryDynamicTableSource(String project_id,String dataset,String table,int snapshotMillis, DecodingFormat<DeserializationSchema<RowData>> decodingFormat,
			DataType producedDataType) {
		this.project_id = project_id;
		this.dataset = dataset;
		this.table = table;
		this.snapshotMillis = snapshotMillis;
		this.decodingFormat = decodingFormat;
		this.producedDataType = producedDataType;
	}

	@Override
	public ChangelogMode getChangelogMode() {
		// in our example the format decides about the changelog mode
		// but it could also be the source itself
		return decodingFormat.getChangelogMode();
	}

	@Override
	public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {

		// create runtime classes that are shipped to the cluster

		final DeserializationSchema<RowData> deserializer = decodingFormat.createRuntimeDecoder(runtimeProviderContext,
				producedDataType);

		final SourceFunction<RowData> sourceFunction = new BigQuerySourceFunction(project_id,dataset,table,snapshotMillis, deserializer);

		return SourceFunctionProvider.of(sourceFunction, false);
	}

	@Override
	public DynamicTableSource copy() {
		return new BigQueryDynamicTableSource(project_id,dataset,table,snapshotMillis, decodingFormat, producedDataType);
	}

	@Override
	public String asSummaryString() {
		return "BigQuery Table Source";
	}
}