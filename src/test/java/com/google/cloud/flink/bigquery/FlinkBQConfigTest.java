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

import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.storage.v1.ArrowSerializationOptions.CompressionCodec;
import com.google.cloud.flink.bigquery.util.FlinkBigQueryConfig;
import com.google.common.collect.ImmutableMap;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import net.sf.jsqlparser.JSQLParserException;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

public class FlinkBQConfigTest {
  public static final int DEFAULT_PARALLELISM = 10;
  public static final String FLINK_VERSION = "1.13.1";
  ImmutableMap<String, String> defaultOptions = ImmutableMap.of("table", "dataset.table");

  @Test
  public void testSerializability() throws IOException {
    Configuration hadoopConfiguration = new Configuration();
    org.apache.flink.configuration.Configuration options =
        new org.apache.flink.configuration.Configuration();
    ConfigOption<String> table = ConfigOptions.key("table").stringType().noDefaultValue();
    ConfigOption<String> selectedFields =
        ConfigOptions.key("selectedFields").stringType().noDefaultValue();
    options.set(table, "bigquery-public-data.samples.shakespeare");
    options.set(selectedFields, "word,word_count");

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
  }

  @Test
  public void testDefaults() throws IOException, JSQLParserException {
    Configuration hadoopConfiguration = new Configuration();
    org.apache.flink.configuration.Configuration options =
        new org.apache.flink.configuration.Configuration();
    ConfigOption<String> table = ConfigOptions.key("table").stringType().noDefaultValue();
    ConfigOption<String> selectedFields =
        ConfigOptions.key("selectedFields").stringType().noDefaultValue();
    options.set(table, "bigquery-public-data.samples.shakespeare");
    options.set(selectedFields, "word,word_count");

    BigQueryDynamicTableFactory factory = new BigQueryDynamicTableFactory();
    FlinkBigQueryConfig config =
        FlinkBigQueryConfig.from(
            factory.requiredOptions(),
            factory.optionalOptions(),
            (ReadableConfig) options,
            defaultOptions,
            hadoopConfiguration,
            DEFAULT_PARALLELISM,
            new org.apache.flink.configuration.Configuration(),
            FLINK_VERSION,
            Optional.empty());

    assertThat(config.getTableId())
        .isEqualTo(TableId.of("bigquery-public-data", "samples", "shakespeare"));
    assertThat(config.getFilter()).isEqualTo(Optional.empty());
    assertThat(config.getSchema()).isEqualTo(Optional.empty());
    assertThat(config.getMaxParallelism()).isEqualTo(OptionalInt.empty());
    assertThat(config.getQuery()).isEqualTo(Optional.empty());
    assertThat(config.getPartitionField()).isEqualTo(Optional.empty());
    assertThat(config.getPartitionType()).isEqualTo(Optional.empty());
    assertThat(config.getPartitionExpirationMs()).isEqualTo(OptionalLong.empty());
    assertThat(config.getPartitionRequireFilter()).isEqualTo(Optional.empty());
    assertThat(config.getDefaultParallelism()).isEqualTo(10);
    assertThat(config.getCredentialsFile()).isEqualTo(Optional.empty());
    assertThat(config.getAccessToken()).isEqualTo(Optional.empty());
    assertThat(config.getCredentialsKey()).isEqualTo(Optional.empty());
    assertThat(config.getArrowCompressionCodec())
        .isEqualTo(CompressionCodec.COMPRESSION_UNSPECIFIED);
    assertThat(config.getClusteredFields()).isEqualTo(Optional.empty());
    assertThat(config.getMaterializationDataset()).isEqualTo(Optional.empty());
    assertThat(config.getMaterializationExpirationTimeInMinutes()).isEqualTo(1440);
    assertThat(config.getMaterializationProject()).isEqualTo(Optional.empty());
    assertThat(config.getReadDataFormat())
        .isEqualTo(com.google.cloud.bigquery.storage.v1.DataFormat.ARROW);
    assertThat(config.getSelectedFields()).isEqualTo("word,word_count");
    assertThat(config.getEndpoint()).isEqualTo(Optional.empty());
  }

  @Test
  public void testConfigFromOptions() throws JSQLParserException {
    Configuration hadoopConfiguration = new Configuration();
    org.apache.flink.configuration.Configuration options =
        new org.apache.flink.configuration.Configuration();
    ConfigOption<String> table = ConfigOptions.key("table").stringType().noDefaultValue();
    ConfigOption<String> query = ConfigOptions.key("query").stringType().noDefaultValue();
    ConfigOption<String> filter = ConfigOptions.key("filter").stringType().defaultValue("");
    ConfigOption<String> partitionField =
        ConfigOptions.key("partitionField").stringType().defaultValue("");
    ConfigOption<String> partitionType =
        ConfigOptions.key("partitionType").stringType().defaultValue("");
    ConfigOption<String> partitionExpirationMS =
        ConfigOptions.key("partitionExpirationMs").stringType().defaultValue("");
    ConfigOption<String> partitionRequireFilter =
        ConfigOptions.key("partitionRequireFilter").stringType().defaultValue("");
    ConfigOption<String> flink_version =
        ConfigOptions.key("flinkVersion").stringType().defaultValue("1.11");
    ConfigOption<Integer> maxParallelism =
        ConfigOptions.key("maxParallelism").intType().defaultValue(1);
    ConfigOption<String> selectedFields =
        ConfigOptions.key("selectedFields").stringType().noDefaultValue();
    ConfigOption<String> materializationProject =
        ConfigOptions.key("materializationProject").stringType().noDefaultValue();
    ConfigOption<String> materializationDataset =
        ConfigOptions.key("materializationDataset").stringType().noDefaultValue();
    options.set(table, "bigquery-public-data.samples.shakespeare");
    options.set(query, "select word,word_count from table");
    options.set(filter, "word_count>100");
    options.set(partitionField, "a");
    options.set(partitionType, "DAY");
    options.set(partitionExpirationMS, "999");
    options.set(partitionRequireFilter, "true");
    options.set(flink_version, "1.13.1");
    options.set(maxParallelism, 99);
    options.set(selectedFields, "word,word_count");
    options.set(materializationProject, "vmp");
    options.set(materializationDataset, "vmd");

    BigQueryDynamicTableFactory factory = new BigQueryDynamicTableFactory();
    FlinkBigQueryConfig config =
        FlinkBigQueryConfig.from(
            factory.requiredOptions(),
            factory.optionalOptions(),
            (ReadableConfig) options,
            defaultOptions,
            hadoopConfiguration,
            DEFAULT_PARALLELISM,
            new org.apache.flink.configuration.Configuration(),
            FLINK_VERSION,
            Optional.empty());

    assertThat(config.getTableId())
        .isEqualTo(TableId.of("bigquery-public-data", "samples", "shakespeare"));
    assertThat(config.getFilter()).isEqualTo(Optional.ofNullable("word_count>100"));
    assertThat(config.getMaxParallelism()).isEqualTo(OptionalInt.of(99));
    assertThat(config.getQuery()).isEqualTo(Optional.empty());
    assertThat(config.getPartitionField()).isEqualTo(Optional.ofNullable("a"));
    assertThat(config.getPartitionExpirationMs()).isEqualTo(OptionalLong.of(999));
    assertThat(config.getPartitionRequireFilter()).isEqualTo(Optional.of(true));
    assertThat(config.getDefaultParallelism()).isEqualTo(10);
    assertThat(config.getMaterializationDataset()).isEqualTo(Optional.ofNullable("vmd"));
    assertThat(config.getMaterializationExpirationTimeInMinutes()).isEqualTo(1440);
    assertThat(config.getMaterializationProject()).isEqualTo(Optional.ofNullable("vmp"));
    assertThat(config.getSelectedFields()).isEqualTo("word,word_count");
  }

  @Test
  public void testInvalidCompressionCodec() {
    Configuration hadoopConfiguration = new Configuration();
    org.apache.flink.configuration.Configuration options =
        new org.apache.flink.configuration.Configuration();
    ConfigOption<String> table = ConfigOptions.key("table").stringType().noDefaultValue();
    ConfigOption<String> selectedFields =
        ConfigOptions.key("selectedFields").stringType().noDefaultValue();
    ConfigOption<String> arrowCompressionCodec =
        ConfigOptions.key("arrowCompressionCodec").stringType().noDefaultValue();
    options.set(table, "bigquery-public-data.samples.shakespeare");
    options.set(selectedFields, "word,word_count");
    options.set(arrowCompressionCodec, "randomCompression");

    BigQueryDynamicTableFactory factory = new BigQueryDynamicTableFactory();
    IllegalArgumentException exception =
        Assert.assertThrows(
            IllegalArgumentException.class,
            () ->
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
    assertThat(
        exception
            .toString()
            .contains(
                "Compression codec 'RANDOMCOMPRESSION' for Arrow is not supported."
                    + " Supported formats are "
                    + Arrays.toString(CompressionCodec.values())));
  }
}
