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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.FactoryUtil;
import org.junit.Test;

public class BigQueryDynamicTableFactoryTest {
  @Test
  public void factoryIdentifierTestSuccess() {
    BigQueryDynamicTableFactory bigQueryDynamicTableFactory = new BigQueryDynamicTableFactory();
    String factoryIdentfier = bigQueryDynamicTableFactory.factoryIdentifier();
    String expectedFactoryIdentifier = "bigquery";
    assertThat(factoryIdentfier).isNotNull();
    assertThat(factoryIdentfier).isEqualTo(expectedFactoryIdentifier);
  }

  @Test
  public void requiredOptionsTestSuccess() {

    BigQueryDynamicTableFactory bigQueryDynamicTableFactory = new BigQueryDynamicTableFactory();
    List<String> expectedOptions =
        Arrays.asList(
            "materializationDataset", "materializationProject", "query", "selectedFields", "table");
    Set<ConfigOption<?>> configOptionSet = bigQueryDynamicTableFactory.requiredOptions();
    assertThat(configOptionSet).isNotNull();
    assertThat(configOptionSet.size()).isEqualTo(5);
    List<String> options = new ArrayList<String>();
    Iterator<ConfigOption<?>> configOptionIterator = configOptionSet.iterator();
    while (configOptionIterator.hasNext()) {
      ConfigOption configOption = configOptionIterator.next();
      options.add(configOption.key().toString());
    }
    Collections.sort(options);
    assertTrue(expectedOptions.equals(options));
  }

  @Test
  public void optionalOptionsTestSuccess() {
    List<String> expectedOptions =
        Arrays.asList(
            "arrowCompressionCodec",
            "bqBackgroundThreadsPerStream",
            "bqEncodedCreateReadSessionRequest",
            "credentials",
            "credentialsFile",
            "defaultParallelism",
            "filter",
            "format",
            "gcpAccessToken",
            "maxParallelism",
            "parallelism",
            "partitionExpirationMs",
            "partitionField",
            "partitionRequireFilter",
            "partitionType",
            "proxyPassword",
            "proxyUri",
            "proxyUsername");
    List<String> options = new ArrayList<String>();
    BigQueryDynamicTableFactory bigQueryDynamicTableFactory = new BigQueryDynamicTableFactory();
    Set<ConfigOption<?>> optionalOptions = bigQueryDynamicTableFactory.optionalOptions();
    assertThat(optionalOptions).isNotNull();
    assertThat(optionalOptions.size()).isEqualTo(18);
    optionalOptions.forEach(
        option -> {
          options.add(option.key().toString());
        });
    Collections.sort(options);
    assertTrue(expectedOptions.equals(options));
  }

  @Test
  public void createDynamicTableSourceTestSuccess() {
    BigQueryDynamicTableFactory bigQueryDynamicTableFactory = new BigQueryDynamicTableFactory();
    MockDynamicTableContext MockDynamicTableContext = createContextObject();
    FactoryUtil.TableFactoryHelper inputHelperObject =
        FactoryUtil.createTableFactoryHelper(bigQueryDynamicTableFactory, MockDynamicTableContext);
    ReadableConfig options = inputHelperObject.getOptions();
    try {
      inputHelperObject.validate();
    } catch (Exception ex) {
      String exceptionString = ensureExpectedException(ex.toString(), options);
      if (exceptionString != null) {
        throw new IllegalArgumentException(exceptionString);
      }
    }
    assertThat(options.get(ConfigOptions.key("connector").stringType().noDefaultValue()))
        .isEqualTo("bigquery");
    assertThat(options.get(ConfigOptions.key("table").stringType().noDefaultValue()))
        .isEqualTo("project.dataset.table");
    assertThat(options.get(ConfigOptions.key("filter").stringType().noDefaultValue()))
        .isEqualTo("word_count>100");
    assertThat(options.get(ConfigOptions.key("format").stringType().noDefaultValue()))
        .isEqualTo("arrow");
    assertThat(options.get(ConfigOptions.key("selectedFields").stringType().noDefaultValue()))
        .isEqualTo("word,word_count");
  }

  @Test
  public void createDynamicTableSourceTestFailure() {
    BigQueryDynamicTableFactory bigQueryDynamicTableFactory = new BigQueryDynamicTableFactory();
    MockDynamicTableContext MockDynamicTableContext = createIncorrectContextObject();
    FactoryUtil.TableFactoryHelper inputHelperObject =
        FactoryUtil.createTableFactoryHelper(bigQueryDynamicTableFactory, MockDynamicTableContext);
    assertThrows(
        "One or more required options are missing",
        ValidationException.class,
        () -> {
          inputHelperObject.validate();
        });
  }

  private MockDynamicTableContext createContextObject() {
    ObjectIdentifier tableIdentifier =
        ObjectIdentifier.of("default-catalog", "default-dataset", "flink-table");
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
    String bigqueryReadTable = "project.dataset.table";
    configOptions.put("table", bigqueryReadTable);
    configOptions.put(FactoryUtil.FORMAT.key(), "arrow");
    configOptions.put(FactoryUtil.CONNECTOR.key(), "bigquery");
    configOptions.put("selectedFields", "word,word_count");
    configOptions.put("filter", "word_count>100");
    Configuration options = new Configuration();
    CatalogTable catalogTable =
        (CatalogTable)
            new CatalogTableImpl(
                tableSchema, partitionColumnList, configOptions, "sample table creation");
    CatalogTableImpl resolvedCatalogTable =
        new CatalogTableImpl(catalogTable.getSchema(), configOptions, "comments for table");
    ClassLoader classloader = Thread.currentThread().getContextClassLoader();
    MockDynamicTableContext contextObj =
        new MockDynamicTableContext(
            tableIdentifier, resolvedCatalogTable, configOptions, options, classloader, false);
    return contextObj;
  }

  private MockDynamicTableContext createIncorrectContextObject() {
    ObjectIdentifier tableIdentifier =
        ObjectIdentifier.of("default-catalog", "default-dataset", "flink-table");
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
    String bigqueryReadTable = "project.dataset.table";
    configOptions.put("table", bigqueryReadTable);
    configOptions.put(FactoryUtil.FORMAT.key(), "arrow");
    configOptions.put(FactoryUtil.CONNECTOR.key(), "bigquery");
    configOptions.put("filter", "word_count>100");
    Configuration options = new Configuration();
    CatalogTable catalogTable =
        (CatalogTable)
            new CatalogTableImpl(
                tableSchema, partitionColumnList, configOptions, "sample table creation");
    CatalogTableImpl resolvedCatalogTable =
        new CatalogTableImpl(catalogTable.getSchema(), configOptions, "comments for table");
    ClassLoader classloader = Thread.currentThread().getContextClassLoader();
    MockDynamicTableContext contextObj =
        new MockDynamicTableContext(
            tableIdentifier, resolvedCatalogTable, configOptions, options, classloader, false);
    return contextObj;
  }

  private String ensureExpectedException(String exceptionString, ReadableConfig options) {
    String errorString = null;
    String stringToCheck = "Missing required options are:";
    String exceptionHead =
        exceptionString.substring(
            0, exceptionString.lastIndexOf(stringToCheck) + stringToCheck.length());
    ArrayList<String> missingArgs =
        new ArrayList<>(
            Arrays.asList(
                StringUtils.substringAfterLast(exceptionString, stringToCheck).trim().split("\n")));
    if (options.get(ConfigOptions.key("table").stringType().noDefaultValue()) != null) {
      missingArgs.remove("query");
      missingArgs.remove("materializationProject");
      missingArgs.remove("materializationDataset");
    } else if (options.get(ConfigOptions.key("query").stringType().noDefaultValue()) != null) {
      missingArgs.remove("table");
      missingArgs.remove("selectedFields");
    }
    if (!missingArgs.isEmpty()) {
      errorString = errorString + exceptionHead + "\n\n" + String.join("\n", missingArgs);
    }
    return errorString;
  }
}
