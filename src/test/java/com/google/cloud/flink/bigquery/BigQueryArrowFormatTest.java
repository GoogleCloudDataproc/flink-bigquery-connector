/*
 * Copyright 2020 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.flink.bigquery;

import static com.google.common.truth.Truth.assertThat;

import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.DynamicTableFactory.Context;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;
import org.junit.Test;

public class BigQueryArrowFormatTest {

  BigQueryArrowFormat bigQueryArrowFormat =
      new BigQueryArrowFormat(Arrays.asList("word", "wordcount_output"));
  MockScanContext mockScanContext = new MockScanContext();
  static StreamTableEnvironment flinkTableEnv;
  static String bigqueryReadTable;
  static String srcQueryString;
  static String flinkSrcTable;
  static ObjectIdentifier tableIdentifier;
  static TableSchema tableSchema;
  public static final int DEFAULT_PARALLELISM = 10;
  public static final String FLINK_VERSION = "1.11.0";
  ImmutableMap<String, String> defaultOptions = ImmutableMap.of("table", "dataset.table");

  public BigQueryArrowFormatTest() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    flinkTableEnv = StreamTableEnvironment.create(env);

    bigqueryReadTable = "q-gcp-6750-pso-gs-flink-22-01.wordcount_dataset.wordcount_output";
    flinkSrcTable = "table1";
    srcQueryString = "CREATE TABLE " + flinkSrcTable + " (word STRING , word_count BIGINT)";

    tableIdentifier = ObjectIdentifier.of("bigquerycatalog", "default", flinkSrcTable);
    tableSchema =
        TableSchema.builder()
            .field("word", DataTypes.STRING())
            .field("word_count", DataTypes.BIGINT())
            .build();
  }

  @Test
  public void getChangelogModeTest() {
    ChangelogMode result = bigQueryArrowFormat.getChangelogMode();
    assertThat(result).isNotNull();
    assertThat(result).isInstanceOf(ChangelogMode.class);
    assertThat(result.getContainedKinds().toString()).isEqualTo("[INSERT]");
    assertThat(result.contains(RowKind.INSERT)).isTrue();
  }

  @Test
  public void createRuntimeDecoderTest() {
    Context context = createContextObject();
    final DataType producedDataType =
        (context).getCatalogTable().getSchema().toPhysicalRowDataType();
    DeserializationSchema<RowData> result =
        bigQueryArrowFormat.createRuntimeDecoder(mockScanContext, producedDataType);
    assertThat(result).isNotNull();
    assertThat(result.getProducedType()).isNull();
  }

  public MockDynamicTableContext createContextObject() {

    ObjectIdentifier tableIdentifier = ObjectIdentifier.of("csvcatalog", "default", "csvtable");
    List<String> partitionColumnList = new ArrayList<String>();
    DescriptorProperties tableSchemaProps = new DescriptorProperties(true);

    TableSchema tableSchema =
        tableSchemaProps
            .getOptionalTableSchema("Schema")
            .orElseGet(
                () ->
                    tableSchemaProps
                        .getOptionalTableSchema("generic.table.schema")
                        .orElseGet(() -> TableSchema.builder().build()));
    Map<String, String> configOptions = new HashMap<>();
    configOptions.put("table", bigqueryReadTable);
    configOptions.put(FactoryUtil.FORMAT.key(), "arrow");
    configOptions.put(FactoryUtil.CONNECTOR.key(), "bigquery");
    configOptions.put("selectedFields", "word,word_count");

    Configuration options = new Configuration();

    ConfigOption<String> table = ConfigOptions.key("table").stringType().noDefaultValue();
    ConfigOption<String> query = ConfigOptions.key("query").stringType().noDefaultValue();
    ConfigOption<String> filter = ConfigOptions.key("filter").stringType().defaultValue("");
    ConfigOption<String> format = ConfigOptions.key("format").stringType().defaultValue("");
    ConfigOption<String> selected_fields =
        ConfigOptions.key("selectedFields").stringType().noDefaultValue();

    options.set(table, bigqueryReadTable);
    options.set(format, "arrow");
    options.set(query, "select word,word_count from table");
    options.set(filter, "word_count>100");
    options.set(selected_fields, "word,word_count");

    CatalogTable catalogTable =
        (CatalogTable)
            new CatalogTableImpl(
                tableSchema, partitionColumnList, configOptions, "sample table creation");

    CatalogTableImpl resolvedCatalogTable =
        new CatalogTableImpl(catalogTable.getSchema(), configOptions, "comments for table");

    ClassLoader classloader = Thread.currentThread().getContextClassLoader();
    // Create the context object
    MockDynamicTableContext contextObj =
        new MockDynamicTableContext(
            tableIdentifier, resolvedCatalogTable, configOptions, options, classloader, false);
    return contextObj;
  }
}
