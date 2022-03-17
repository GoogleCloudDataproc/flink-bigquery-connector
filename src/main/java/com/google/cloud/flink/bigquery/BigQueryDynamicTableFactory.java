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

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.cloud.flink.bigquery.model.Configuration;

public final class BigQueryDynamicTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {

	private static final Logger log = LoggerFactory.getLogger(BigQueryDynamicTableFactory.class);
	Configuration config = new Configuration();

	public static final ConfigOption<String> CONFIGOPTIONS = ConfigOptions.key("configOptions").stringType()
			.noDefaultValue();
	public static ReadSession readSession;

	@Override
	public String factoryIdentifier() {
		return "bigquery";
	}

	@Override
	public Set<ConfigOption<?>> requiredOptions() {
		final Set<ConfigOption<?>> options = new HashSet<>();
		options.add(FactoryUtil.FORMAT);
		options.add(CONFIGOPTIONS);
		return options;
	}

	@Override
	public Set<ConfigOption<?>> optionalOptions() {
		final Set<ConfigOption<?>> options = new HashSet<>();
		return options;
	}

	@Override
	public DynamicTableSource createDynamicTableSource(Context context) {

		final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

		final DecodingFormat<DeserializationSchema<RowData>> decodingFormat = helper
				.discoverDecodingFormat(ArrowFormatFactory.class, FactoryUtil.FORMAT);

		helper.validate();
		Map<String, String> configOption = new HashMap<>();
		final ReadableConfig options = helper.getOptions();
		final String configStr = options.get(CONFIGOPTIONS);
		for (String pair : Arrays.asList(configStr.split("#"))) {
			String[] entry = pair.split("::");
			configOption.put(entry[0].trim(),entry.length == 2 ? entry[1].trim():"");
		}

		final String table = configOption.get("table");
		final String dataset = configOption.get("dataset");
		final String projectId = configOption.get("projectId");

		log.info("Config Options -> " + configOption);
		DataType producedDataType = null;
		try {
//			List<DataType> datatypeList = context.getCatalogTable().getResolvedSchema().getColumnDataTypes();
//			List<String> columnNameList = context.getCatalogTable().getResolvedSchema().getColumnNames();
//			if (datatypeList.indexOf(DataTypes.INT())>-1 ) {
//				datatypeList.set(datatypeList.indexOf(DataTypes.INT()),DataTypes.BIGINT());
//			}
//			
//			context.getCatalogTable().getResolvedSchema();
			producedDataType = context.getCatalogTable().getResolvedSchema().toPhysicalRowDataType();
			readSession = BigQueryReadSession.getReadsession(projectId, table, dataset, configOption);
		} catch (IOException ex) {
			log.error("Error while reading big query session", ex);
			throw new FlinkBigQueryException("Error while reading big query session", ex);

		}
		return new BigQueryDynamicTableSource(decodingFormat, producedDataType);
	}

	@Override
	public DynamicTableSink createDynamicTableSink(Context context) {
		return null;
	}
}