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

import static com.google.common.truth.Truth.assertThat;

import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import net.sf.jsqlparser.JSQLParserException;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Schema.Builder;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.junit.Test;
import org.mockito.Mockito;

public class BigQueryDynamicSinkFactoryTest {

  BigQueryDynamicSinkFactory bigQueryDynamicSinkFactory = new BigQueryDynamicSinkFactory();
  static StreamTableEnvironment flinkTableEnv;
  static String bigqueryWriteTable;
  static String srcQueryString;
  static String flinkSinkTable;
  static ObjectIdentifier tableIdentifier;
  static TableSchema tableSchema;
  public static final int DEFAULT_PARALLELISM = 10;
  public static final String FLINK_VERSION = "1.13.1";
  ImmutableMap<String, String> defaultOptions = ImmutableMap.of("table", "dataset.table");

  public BigQueryDynamicSinkFactoryTest() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    flinkTableEnv = StreamTableEnvironment.create(env);

    bigqueryWriteTable = "project.dataset.table";
    flinkSinkTable = "table1";
    srcQueryString = "CREATE TABLE " + flinkSinkTable + " (word STRING , word_count BIGINT)";

    tableIdentifier = ObjectIdentifier.of("bigquerycatalog", "default", flinkSinkTable);
    tableSchema =
        TableSchema.builder()
            .field("word", DataTypes.STRING())
            .field("word_count", DataTypes.BIGINT())
            .build();
  }

  @Test
  public void factoryIdentifierTest() {
    String result = bigQueryDynamicSinkFactory.factoryIdentifier();
    assertThat(result).isNotNull();
    assertThat(result).contains("bigquery");
    assertThat(result).isInstanceOf(String.class);
  }

  @Test
  public void requiredOptionsTest() {
    Set<ConfigOption<?>> result = bigQueryDynamicSinkFactory.requiredOptions();
    assertThat(result).isNotNull();
    assertThat(result).isNotEmpty();
  }

  @Test
  public void optionalOptionsTest() {
    Set<ConfigOption<?>> result = bigQueryDynamicSinkFactory.optionalOptions();
    assertThat(result).isNotNull();
    assertThat(result).isNotEmpty();
  }

  @Test
  public void createDynamicTableSinkTest() throws JSQLParserException {
    BigQueryDynamicSinkFactory mockBigQueryDynamicSinkFactory =
        Mockito.spy(bigQueryDynamicSinkFactory);
    Mockito.doNothing().when(mockBigQueryDynamicSinkFactory).createBigQueryTable();
    MockDynamicSinkFactoryContext context = createContextObject();
    DynamicTableSink result = mockBigQueryDynamicSinkFactory.createDynamicTableSink(context);
    assertThat(mockBigQueryDynamicSinkFactory.factoryIdentifier()).isEqualTo("bigquery");
    assertThat(result).isNotNull();
    assertThat(result).isInstanceOf(BigQueryDynamicTableSink.class);
  }

  public MockDynamicSinkFactoryContext createContextObject() {

    ObjectIdentifier tableIdentifier = ObjectIdentifier.of("csvcatalog", "default", "csvtable");
    Map<String, String> configOptions = new HashMap<>();
    configOptions.put("table", bigqueryWriteTable);
    configOptions.put(FactoryUtil.CONNECTOR.key(), "bigquery");
    Configuration options = new Configuration();
    ConfigOption<String> table = ConfigOptions.key("table").stringType().noDefaultValue();
    options.set(table, bigqueryWriteTable);

    ResolvedCatalogTable resolvedCatalogTable = getResolvedCatalogTable();
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

    MockDynamicSinkFactoryContext contextObj =
        new MockDynamicSinkFactoryContext(
            tableIdentifier, resolvedCatalogTable, configOptions, options, classLoader, false);
    return contextObj;
  }

  private ResolvedCatalogTable getResolvedCatalogTable() {

    List<String> fieldNames = Arrays.asList("word", "word_count");
    DataType intDT = DataTypes.BIGINT();
    DataType charDT = DataTypes.VARCHAR(10);
    List<DataType> fieldDataTypes = Arrays.asList(charDT, intDT);

    Builder schemaBuilder = Schema.newBuilder();
    Schema tableSchema = schemaBuilder.fromFields(fieldNames, fieldDataTypes).build();

    Map<String, String> configOptions = new HashMap<>();
    String bigqueryWriteTable = "project.dataset.table";
    configOptions.put("table", bigqueryWriteTable);
    configOptions.put(FactoryUtil.CONNECTOR.key(), "bigquery");
    CatalogTable catalogTable =
        CatalogTable.of(
            tableSchema, "sample table creation", new ArrayList<String>(), configOptions);
    ResolvedSchema physical = ResolvedSchema.physical(fieldNames, fieldDataTypes);
    return new ResolvedCatalogTable(catalogTable, physical);
  }
}
