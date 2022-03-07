package com.flink.connector;

import java.util.HashSet;
import java.util.Set;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;


public final class BigQueryDynamicTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {
	private static final long serialVersionUID = 1;
	// define all options statically
	public static final ConfigOption<Integer> SNAPSHOTMILLIS =
            ConfigOptions.key("snapshotMillis").intType().defaultValue(null);
    public static final ConfigOption<String> PROJECT_ID =
            ConfigOptions.key("project_id").stringType().noDefaultValue();
    public static final ConfigOption<String> DATASET =
            ConfigOptions.key("dataset").stringType().noDefaultValue();
    public static final ConfigOption<String> TABLE =
            ConfigOptions.key("table").stringType().noDefaultValue();

    @Override
	public String factoryIdentifier() {
		return "bigquery"; // used for matching to `connector = '...'`
	}

	@Override
	public Set<ConfigOption<?>> requiredOptions() {
		final Set<ConfigOption<?>> options = new HashSet<>();
		options.add(FactoryUtil.FORMAT); // use pre-defined option for format
		options.add(PROJECT_ID);
		options.add(DATASET);
		options.add(TABLE);
		return options;
	}

	@Override
	public Set<ConfigOption<?>> optionalOptions() {
		final Set<ConfigOption<?>> options = new HashSet<>();
		options.add(SNAPSHOTMILLIS);
		return options;
	}

	@Override
	public DynamicTableSource createDynamicTableSource(Context context) {
		// either implement your custom validation logic here ...
		// or use the provided helper utility
		final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
		// discover a suitable decoding format
		final DecodingFormat<DeserializationSchema<RowData>> decodingFormat = helper
				.discoverDecodingFormat(ArrowFormatFactory.class, FactoryUtil.FORMAT);

		// validate all options
		helper.validate();

		// get the validated options
		final ReadableConfig options = helper.getOptions();
		final int snapshotMillis = options.get(SNAPSHOTMILLIS);
		final String project_id = options.get(PROJECT_ID);
		final String dataset = options.get(DATASET);
		final String table = options.get(TABLE);
		// derive the produced data type (excluding computed columns) from the catalog
		// table
		final DataType producedDataType = context.getCatalogTable().getResolvedSchema().toPhysicalRowDataType();

		// create and return dynamic table source
		return new BigQueryDynamicTableSource(project_id,dataset, table,snapshotMillis, decodingFormat, producedDataType);
	}

	@Override
	public DynamicTableSink createDynamicTableSink(Context context) {
		// TODO Auto-generated method stub
		return null;
	}
}