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

import com.google.auth.Credentials;
import com.google.cloud.bigquery.connector.common.BigQueryClientFactory;
import com.google.cloud.bigquery.connector.common.BigQueryCredentialsSupplier;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.cloud.bigquery.storage.v1.ReadStream;
import com.google.cloud.flink.bigquery.common.UserAgentHeaderProvider;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class BigQueryDynamicTableFactory
    implements DynamicTableSourceFactory, DynamicTableSinkFactory {

  private static final Logger log = LoggerFactory.getLogger(BigQueryDynamicTableFactory.class);

  private static Map<String, String> configOption = new HashMap<>();
  List<String> readSessionStreamList = new ArrayList<String>();

  public static final ConfigOption<String> TABLE =
      ConfigOptions.key("table").stringType().noDefaultValue();
  public static final ConfigOption<String> QUERY =
      ConfigOptions.key("query").stringType().noDefaultValue();
  public static final ConfigOption<String> FILTER =
      ConfigOptions.key("filter").stringType().defaultValue("");
  public static final ConfigOption<String> PARTITIONFIELD =
      ConfigOptions.key("partitionField").stringType().defaultValue("");
  public static final ConfigOption<String> FLINKVERSION =
      ConfigOptions.key("flinkVersion").stringType().defaultValue("1.11");
  public static final ConfigOption<Integer> PARALLELISM =
      ConfigOptions.key("parallelism").intType().defaultValue(1);
  public static final ConfigOption<String> SELECTEDFIELDS =
      ConfigOptions.key("selectedFields").stringType().noDefaultValue();
  public static final ConfigOption<Integer> DEFAULTPARALLELISM =
      ConfigOptions.key("defaultParallelism").intType().defaultValue(1);
  public static final ConfigOption<String> CREDENTIALKEYFILE =
      ConfigOptions.key("credentialsFile").stringType().noDefaultValue();
  public static final ConfigOption<String> ACCESSTOKEN =
      ConfigOptions.key("gcpAccessToken").stringType().defaultValue("");
  public static final ConfigOption<String> CREDENTIALSKEY =
      ConfigOptions.key("credentials").stringType().defaultValue("");
  public static final ConfigOption<String> PROXYURI =
      ConfigOptions.key("proxyUri").stringType().defaultValue("");
  public static final ConfigOption<String> PROXYUSERNAME =
      ConfigOptions.key("proxyUsername").stringType().defaultValue("");
  public static final ConfigOption<String> PROXYPASSWORD =
      ConfigOptions.key("proxyPassword").stringType().defaultValue("");
  public static final ConfigOption<String> BQENCODEDCREATERREADSESSIONREQUEST =
      ConfigOptions.key("bQEncodedCreaterReadSessionRequest").stringType().noDefaultValue();
  public static final ConfigOption<String> BQBACKGROUNDTHREADSPERSTREAM =
      ConfigOptions.key("bQBackgroundThreadsPerStream").stringType().noDefaultValue();
  

  @Override
  public String factoryIdentifier() {
    return "bigquery";
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    final Set<ConfigOption<?>> options = new HashSet<>();
    options.add(FactoryUtil.FORMAT);
    options.add(TABLE);
    options.add(SELECTEDFIELDS);
    return options;
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    final Set<ConfigOption<?>> options = new HashSet<>();
    options.add(FILTER);
    options.add(CREDENTIALKEYFILE);
    options.add(FLINKVERSION);
    options.add(PARTITIONFIELD);
    options.add(DEFAULTPARALLELISM);
    options.add(BQENCODEDCREATERREADSESSIONREQUEST);
    options.add(BQBACKGROUNDTHREADSPERSTREAM);
    return options;
  }

  @Override
  public DynamicTableSource createDynamicTableSource(Context context) {

    final FactoryUtil.TableFactoryHelper helper =
        FactoryUtil.createTableFactoryHelper(this, context);

    final DecodingFormat<DeserializationSchema<RowData>> decodingFormat =
        helper.discoverDecodingFormat(ArrowFormatFactory.class, FactoryUtil.FORMAT);

    helper.validate();
    final ReadableConfig options = helper.getOptions();

    requiredOptions().stream()
        .forEach(
            ele ->
                configOption.put(
                    ele.key(),
                    options.get(ConfigOptions.key(ele.key()).stringType().noDefaultValue())));
    optionalOptions().stream()
        .filter(ele -> Objects.nonNull(ele))
        .forEach(
            ele ->
                configOption.put(
                    ele.key(),
                    options.get(ConfigOptions.key(ele.key()).stringType().noDefaultValue())));
    this.configOption.values().removeIf(Objects::isNull);
    final DataType producedDataType =
        context.getCatalogTable().getResolvedSchema().toPhysicalRowDataType();

    BigQueryClientFactory bigQueryReadClientFactory = null;
    try {

      // Create client factory
      String table = options.get(TABLE);

      ImmutableMap<String, String> defaultOptions =
          ImmutableMap.of(
              "table",
              table.split("\\.")[0] + "." + table.split("\\.")[1] + "." + table.split("\\.")[2]);

      int defaultParallelism =
          configOption.get("defaultParallelism") != null
              ? Integer.parseInt(configOption.get("defaultParallelism"))
              : 1;

      FlinkBigQueryConfig bqconfig =
          FlinkBigQueryConfig.from(
              configOption,
              defaultOptions, // ImmutableMap.of(),
              new org.apache.hadoop.conf.Configuration(),
              defaultParallelism,
              new org.apache.flink.configuration.Configuration(),
              configOption.get("FLINK_VERSION"),
              Optional.empty());

      Credentials credentials = bqconfig.createCredentials();
      BigQueryCredentialsSupplier bigQueryCredentialsSupplier =
          new BigQueryCredentialsSupplier(
              bqconfig.getAccessToken(),
              bqconfig.getCredentialsKey(),
              bqconfig.getCredentialsFile(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty());

      final UserAgentHeaderProvider userAgentHeaderProvider =
          new UserAgentHeaderProvider("test-agent");
      bigQueryReadClientFactory =
          new BigQueryClientFactory(bigQueryCredentialsSupplier, userAgentHeaderProvider, bqconfig);

      // Create read session
      ReadSession readSession =
          BigQueryReadSession.getReadsession(
              credentials, bqconfig, table, bigQueryReadClientFactory, configOption);
      List<ReadStream> readsessionList = readSession.getStreamsList();

      // Iterate over read stream
      for (ReadStream stream : readsessionList) {
        this.readSessionStreamList.add(stream.getName());
      }
    } catch (IOException ex) {
      log.error("Error while reading big query session", ex);
    }

    return new BigQueryDynamicTableSource(
        decodingFormat, producedDataType, readSessionStreamList, bigQueryReadClientFactory);
  }

  @Override
  public DynamicTableSink createDynamicTableSink(Context context) {
    return null;
  }

  public static Map<String, String> getConfigOption() {
    return configOption;
  }
}
