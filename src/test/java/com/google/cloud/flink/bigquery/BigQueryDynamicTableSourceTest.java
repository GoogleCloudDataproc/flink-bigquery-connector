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
import static org.mockito.Mockito.mock;

import com.google.cloud.bigquery.connector.common.BigQueryClientFactory;
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
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.ScanTableSource.ScanContext;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

public class BigQueryDynamicTableSourceTest {

  static String bigqueryReadTable;
  static ObjectIdentifier tableIdentifier;
  static TableSchema tableSchema;
  static Configuration options = new Configuration();

  @BeforeClass
  public static void setup() {
    bigqueryReadTable = "bigquery-public-data.samples.shakespeare";
  }

  @Test
  public void getScanRuntimeProviderSuccessfulTest() {

    // Mock the big query client factory
    BigQueryClientFactory mockBigQueryClientFactory = mock(BigQueryClientFactory.class);

    ArrowFormatFactory arrowFormat = new ArrowFormatFactory();
    DataType producedDataType =
        createContextObject().getCatalogTable().getSchema().toPhysicalRowDataType();

    DecodingFormat<DeserializationSchema<RowData>> decodingFormat =
        arrowFormat.createDecodingFormat(createContextObject(), options);

    ArrayList<String> readStreamNames = new ArrayList<String>();
    CatalogTable catalogTableMock = Mockito.mock(CatalogTable.class);

    // Initialize the constructor
    BigQueryDynamicTableSource bigQueryDynamicTableSource =
        new BigQueryDynamicTableSource(
            decodingFormat,
            producedDataType,
            readStreamNames,
            mockBigQueryClientFactory,
            catalogTableMock);

    ScanContext mockScanContext = mock(ScanContext.class);
    bigQueryDynamicTableSource.getScanRuntimeProvider(mockScanContext);

    assertThat(bigQueryDynamicTableSource instanceof BigQueryDynamicTableSource);
    assertThat(bigQueryDynamicTableSource.getChangelogMode()).isNotNull();
    assertThat(bigQueryDynamicTableSource.getChangelogMode() instanceof ChangelogMode);
    assertThat(bigQueryDynamicTableSource.copy() instanceof BigQueryDynamicTableSource);
    assertThat(bigQueryDynamicTableSource.asSummaryString()).isEqualTo("BigQuery Table Source");
  }

  private MockDynamicTableContext createContextObject() {

    ObjectIdentifier tableIdentifier = ObjectIdentifier.of("csvcatalog", "default", "csvtable");

    Map<String, String> configOptions = new HashMap<>();
    configOptions.put("table", bigqueryReadTable);
    configOptions.put(FactoryUtil.FORMAT.key(), "arrow");
    configOptions.put(FactoryUtil.CONNECTOR.key(), "bigquery");
    configOptions.put("selectedFields", "word,word_count");

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

    List<String> fieldNames = Arrays.asList("word", "word_count");
    DataType varDT = DataTypes.VARCHAR(20);
    DataType chatDT = DataTypes.VARCHAR(10);
    List<DataType> fieldDataTypes = Arrays.asList(varDT, chatDT);

    Builder schemaBuilder = Schema.newBuilder();
    Schema tableSchema = schemaBuilder.fromFields(fieldNames, fieldDataTypes).build();

    Map<String, String> configOptions = new HashMap<>();
    String bigqueryReadTable = "project.dataset.table";
    configOptions.put("table", bigqueryReadTable);
    configOptions.put(FactoryUtil.FORMAT.key(), "arrow");
    configOptions.put(FactoryUtil.CONNECTOR.key(), "bigquery");
    configOptions.put("selectedFields", "word,word_count");
    configOptions.put("filter", "word_count>100");
    configOptions.put("arrowFields", "word,word_count");
    CatalogTable catalogTable =
        CatalogTable.of(
            tableSchema, "sample table creation", new ArrayList<String>(), configOptions);
    ResolvedSchema physical = ResolvedSchema.physical(fieldNames, fieldDataTypes);
    return new ResolvedCatalogTable(catalogTable, physical);
  }
}
