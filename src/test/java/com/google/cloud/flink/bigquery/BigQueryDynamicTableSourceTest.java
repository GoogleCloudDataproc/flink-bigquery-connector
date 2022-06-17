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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.ScanTableSource.ScanContext;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.junit.BeforeClass;
import org.junit.Test;

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

    // Initialize the constructor
    BigQueryDynamicTableSource bigQueryDynamicTableSource =
        new BigQueryDynamicTableSource(
            decodingFormat, producedDataType, readStreamNames, mockBigQueryClientFactory);

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
    return new MockDynamicTableContext(
        tableIdentifier, resolvedCatalogTable, configOptions, options, classloader, false);
  }
}
