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
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Schema.Builder;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableFactory.Context;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;
import org.junit.Before;
import org.junit.Test;

public class BigQueryArrowFormatTest {

  static BigQueryArrowFormat bigQueryArrowFormat;
  MockScanContext mockScanContext = new MockScanContext();
  static StreamTableEnvironment flinkTableEnv;
  static String bigqueryReadTable;
  static String srcQueryString;
  static String flinkSrcTable;
  static ObjectIdentifier tableIdentifier;
  static TableSchema tableSchema;
  public static final int DEFAULT_PARALLELISM = 10;
  public static final String FLINK_VERSION = "1.11.0";
  static ImmutableMap<String, String> defaultOptions;

  @Before
  public void setUp() {

    bigQueryArrowFormat =
        new BigQueryArrowFormat(Arrays.asList("col1", "col2"), Arrays.asList("col1", "col2"));
    defaultOptions = ImmutableMap.of("table", "dataset.table");

    bigqueryReadTable = "project.datatset.table";
    flinkSrcTable = "testTable";
    srcQueryString = "CREATE TABLE " + flinkSrcTable + " (col1 STRING , col2 BIGINT)";

    tableIdentifier = ObjectIdentifier.of("bigquerycatalog", "default", flinkSrcTable);
    tableSchema =
        TableSchema.builder()
            .field("col1", DataTypes.STRING())
            .field("col2", DataTypes.BIGINT())
            .build();
  }

  @Test
  public void getChangelogModeTest() {
    ChangelogMode result = bigQueryArrowFormat.getChangelogMode();
    assertThat(result).isNotNull();
    assertThat(result.contains(RowKind.INSERT)).isTrue();
  }

  @Test
  public void createRuntimeDecoderTest() {
    Context context = createContextObject();
    final DataType producedDataType =
        (context).getCatalogTable().getSchema().toPhysicalRowDataType();

    List<String> fieldNames = Arrays.asList("id", "location");
    DataType intDT = DataTypes.BIGINT();
    DataType chatDT = DataTypes.VARCHAR(10);
    List<DataType> fieldDataTypes = Arrays.asList(intDT, chatDT);
    ResolvedSchema physical = ResolvedSchema.physical(fieldNames, fieldDataTypes);

    DeserializationSchema<RowData> result =
        bigQueryArrowFormat.createRuntimeDecoder(mockScanContext, physical.toPhysicalRowDataType());
    assertThat(result).isNotNull();
    assertThat(result.getProducedType()).isNull();
  }

  public MockDynamicTableContext createContextObject() {

    ObjectIdentifier tableIdentifier = ObjectIdentifier.of("csvcatalog", "default", "csvtable");
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
    ConfigOption<String> selectedFields =
        ConfigOptions.key("selectedFields").stringType().noDefaultValue();

    options.set(table, bigqueryReadTable);
    options.set(format, "arrow");
    options.set(query, "select word,word_count from table");
    options.set(filter, "word_count>100");
    options.set(selectedFields, "word,word_count");

    ResolvedCatalogTable resolvedCatalogTable = getResolvedCatalogTable();
    ClassLoader classloader = Thread.currentThread().getContextClassLoader();

    MockDynamicTableContext contextObj =
        new MockDynamicTableContext(
            tableIdentifier, resolvedCatalogTable, configOptions, options, classloader, false);
    return contextObj;
  }

  private ResolvedCatalogTable getResolvedCatalogTable() {

    List<String> fieldNames = Arrays.asList("id", "location");
    DataType intDT = DataTypes.BIGINT();
    DataType chatDT = DataTypes.CHAR(10);
    List<DataType> fieldDataTypes = Arrays.asList(intDT, chatDT);

    Builder schemaBuilder = Schema.newBuilder();
    Schema tableSchema = schemaBuilder.fromFields(fieldNames, fieldDataTypes).build();

    Map<String, String> configOptions = new HashMap<>();
    String bigqueryReadTable = "project.dataset.table";
    configOptions.put("table", bigqueryReadTable);
    configOptions.put(FactoryUtil.FORMAT.key(), "arrow");
    configOptions.put(FactoryUtil.CONNECTOR.key(), "bigquery");
    configOptions.put("selectedFields", "word,word_count");
    configOptions.put("filter", "word_count>100");
    configOptions.put("arrowFields", "f1,f2");
    CatalogTable catalogTable =
        CatalogTable.of(
            tableSchema, "sample table creation", new ArrayList<String>(), configOptions);
    ResolvedSchema physical = ResolvedSchema.physical(fieldNames, fieldDataTypes);
    return new ResolvedCatalogTable(catalogTable, physical);
  }
}
