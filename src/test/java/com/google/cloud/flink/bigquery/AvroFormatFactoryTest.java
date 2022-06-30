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

import com.google.cloud.flink.bigquery.util.FlinkBigQueryConfig;
import com.google.common.collect.ImmutableMap;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
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
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableFactory.Context;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;
import org.junit.Test;

public class AvroFormatFactoryTest {

  AvroFormatFactory avroFormatFactory = new AvroFormatFactory();
  static StreamTableEnvironment flinkTableEnv;
  static String bigqueryReadTable;
  static String srcQueryString;
  static String flinkSrcTable;
  static ObjectIdentifier tableIdentifier;
  static TableSchema tableSchema;
  public static final int DEFAULT_PARALLELISM = 10;
  public static final String FLINK_VERSION = "1.13.1";
  ImmutableMap<String, String> defaultOptions = ImmutableMap.of("table", "dataset.table");

  public AvroFormatFactoryTest() {

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    flinkTableEnv = StreamTableEnvironment.create(env);

    bigqueryReadTable = "project.dataset.table";
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
  public void createDecodingFormatTest() throws IOException {
    Context context = createContextObject();
    ReadableConfig formatOptions = createFormatOptions();
    DecodingFormat<DeserializationSchema<RowData>> result =
        avroFormatFactory.createDecodingFormat(context, formatOptions);
    assertThat(result).isNotNull();
    assertThat(result).isInstanceOf(BigQueryAvroFormat.class);
    assertThat(result.getChangelogMode().getContainedKinds().toString()).isEqualTo("[INSERT]");
    assertThat(result.getChangelogMode().contains(RowKind.INSERT)).isTrue();
  }

  @Test
  public void factoryIdentifierTest() {
    String result = avroFormatFactory.factoryIdentifier();
    assertThat(result).isNotNull();
    assertThat(result).isEqualTo("avro");
  }

  @Test
  public void createEncodingFormatTest() throws IOException {
    Context context = createContextObject();
    ReadableConfig formatOptions = createFormatOptions();
    EncodingFormat<SerializationSchema<RowData>> result =
        avroFormatFactory.createEncodingFormat(context, formatOptions);
    assertThat(result).isNull();
  }

  @Test
  public void requiredOptionsTest() {
    Set<ConfigOption<?>> result = avroFormatFactory.requiredOptions();
    assertThat(result).isEmpty();
  }

  @Test
  public void optionalOptionsTest() {
    Set<ConfigOption<?>> result = avroFormatFactory.optionalOptions();
    assertThat(result).isEmpty();
  }

  private ReadableConfig createFormatOptions() throws IOException {

    org.apache.hadoop.conf.Configuration hadoopConfiguration =
        new org.apache.hadoop.conf.Configuration();
    org.apache.flink.configuration.Configuration options =
        new org.apache.flink.configuration.Configuration();
    ConfigOption<String> table = ConfigOptions.key("table").stringType().noDefaultValue();
    ConfigOption<String> selectedFields =
        ConfigOptions.key("selectedFields").stringType().noDefaultValue();
    ConfigOption<String> defaultParallelism =
        ConfigOptions.key("defaultParallelism").stringType().noDefaultValue();
    ConfigOption<String> connector = ConfigOptions.key("connector").stringType().noDefaultValue();
    ConfigOption<String> format = ConfigOptions.key("format").stringType().noDefaultValue();
    options.set(table, "bigquery-public-data.samples.shakespeare");
    options.set(selectedFields, "word,word_count");
    options.set(defaultParallelism, "5");
    options.set(connector, "bigquery");
    options.set(format, "avro");

    BigQueryDynamicTableFactory factory = new BigQueryDynamicTableFactory();
    new ObjectOutputStream(new ByteArrayOutputStream())
        .writeObject(
            FlinkBigQueryConfig.from(
                factory.requiredOptions(),
                factory.optionalOptions(),
                (ReadableConfig) options,
                defaultOptions,
                hadoopConfiguration,
                DEFAULT_PARALLELISM,
                new org.apache.flink.configuration.Configuration(),
                FLINK_VERSION,
                Optional.empty()));
    return options;
  }

  public MockDynamicTableContext createContextObject() {

    ObjectIdentifier tableIdentifier = ObjectIdentifier.of("csvcatalog", "default", "csvtable");
    Map<String, String> configOptions = new HashMap<>();
    configOptions.put("table", bigqueryReadTable);
    configOptions.put(FactoryUtil.FORMAT.key(), "avro");
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
    options.set(format, "avro");
    options.set(query, "select word,word_count from table");
    options.set(filter, "word_count>100");
    options.set(selectedFields, "word,word_count");

    ResolvedCatalogTable resolvedCatalogTable = getResolvedCatalogTable();
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

    MockDynamicTableContext mockDynamicTableContext =
        new MockDynamicTableContext(
            tableIdentifier, resolvedCatalogTable, configOptions, options, classLoader, false);
    return mockDynamicTableContext;
  }

  private ResolvedCatalogTable getResolvedCatalogTable() {

    List<String> fieldNames = Arrays.asList("id", "location");
    DataType intDT = DataTypes.BIGINT();
    DataType charDT = DataTypes.CHAR(10);
    List<DataType> fieldDataTypes = Arrays.asList(intDT, charDT);

    Builder schemaBuilder = Schema.newBuilder();
    Schema tableSchema = schemaBuilder.fromFields(fieldNames, fieldDataTypes).build();

    Map<String, String> configOptions = new HashMap<>();
    String bigqueryReadTable = "project.dataset.table";
    configOptions.put("table", bigqueryReadTable);
    configOptions.put(FactoryUtil.FORMAT.key(), "avro");
    configOptions.put(FactoryUtil.CONNECTOR.key(), "bigquery");
    configOptions.put("selectedFields", "word,word_count");
    configOptions.put("filter", "word_count>100");
    configOptions.put("avroFields", "f1,f2");
    CatalogTable catalogTable =
        CatalogTable.of(
            tableSchema, "sample table creation", new ArrayList<String>(), configOptions);
    ResolvedSchema physical = ResolvedSchema.physical(fieldNames, fieldDataTypes);
    return new ResolvedCatalogTable(catalogTable, physical);
  }
}
