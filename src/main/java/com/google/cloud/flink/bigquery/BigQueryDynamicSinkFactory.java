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

import static com.google.cloud.flink.bigquery.util.ProtobufUtils.getFields;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.bigquery.connector.common.BigQueryClientFactory;
import com.google.cloud.bigquery.connector.common.BigQueryCredentialsSupplier;
import com.google.cloud.flink.bigquery.common.FlinkBigQueryConnectorUserAgentProvider;
import com.google.cloud.flink.bigquery.common.UserAgentHeaderProvider;
import com.google.cloud.flink.bigquery.common.WriterCommitMessageContext;
import com.google.cloud.flink.bigquery.exception.FlinkBigQueryException;
import com.google.cloud.flink.bigquery.util.FlinkBigQueryConfig;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import net.sf.jsqlparser.JSQLParserException;
import org.apache.flink.api.common.accumulators.ListAccumulator;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class BigQueryDynamicSinkFactory implements DynamicTableSinkFactory {

  private static final Logger log = LoggerFactory.getLogger(BigQueryDynamicSinkFactory.class);
  public static final ConfigOption<String> PARENT_PROJECT =
      ConfigOptions.key("parentProject").stringType().noDefaultValue();
  public static final ConfigOption<String> TABLE =
      ConfigOptions.key("table").stringType().noDefaultValue();
  public static final ConfigOption<Integer> PARALLELISM =
      ConfigOptions.key("parallelism").intType().defaultValue(1);
  public static final ConfigOption<Integer> MAX_PARALLELISM =
      ConfigOptions.key("maxParallelism").intType().defaultValue(1);
  public static final ConfigOption<String> CREDENTIAL_KEY_FILE =
      ConfigOptions.key("credentialsFile").stringType().noDefaultValue();
  public static final ConfigOption<String> ACCESS_TOKEN =
      ConfigOptions.key("gcpAccessToken").stringType().defaultValue("");
  public static final ConfigOption<String> CREDENTIALS_KEY =
      ConfigOptions.key("credentials").stringType().defaultValue("");
  public static final ConfigOption<String> PROXY_URI =
      ConfigOptions.key("proxyUri").stringType().defaultValue("");
  public static final ConfigOption<String> PROXY_USERNAME =
      ConfigOptions.key("proxyUsername").stringType().defaultValue("");
  public static final ConfigOption<String> PROXY_PASSWORD =
      ConfigOptions.key("proxyPassword").stringType().defaultValue("");
  public static final ConfigOption<String> PARTITION_FIELD =
      ConfigOptions.key("partitionField").stringType().defaultValue("");
  public static final ConfigOption<String> PARTITION_TYPE =
      ConfigOptions.key("partitionType").stringType().defaultValue("");
  public static final ConfigOption<String> PARTITION_EXPIRATION_MS =
      ConfigOptions.key("partitionExpirationMs").stringType().defaultValue("");
  public static final ConfigOption<String> PARTITION_REQUIRE_FILTER =
      ConfigOptions.key("partitionRequireFilter").stringType().defaultValue("");
  private FlinkBigQueryConfig bqconfig;
  private BigQueryClientFactory bigQueryWriteClientFactory;
  private BigQueryClient bigQueryClient;
  private List<DataType> fieldDataTypes;
  private List<String> fieldNames;
  private static ListAccumulator<WriterCommitMessageContext> accumulator =
      new ListAccumulator<WriterCommitMessageContext>();

  @Override
  public String factoryIdentifier() {
    return "bigquery";
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    final Set<ConfigOption<?>> options = new HashSet<>();
    options.add(TABLE);
    return options;
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    final Set<ConfigOption<?>> options = new HashSet<>();
    options.add(FactoryUtil.FORMAT);
    options.add(PARENT_PROJECT);
    options.add(CREDENTIAL_KEY_FILE);
    options.add(ACCESS_TOKEN);
    options.add(CREDENTIALS_KEY);
    options.add(PROXY_URI);
    options.add(PROXY_USERNAME);
    options.add(PROXY_PASSWORD);
    options.add(PARALLELISM);
    options.add(MAX_PARALLELISM);
    options.add(PARTITION_FIELD);
    options.add(PARTITION_TYPE);
    options.add(PARTITION_EXPIRATION_MS);
    options.add(PARTITION_REQUIRE_FILTER);
    return options;
  }

  @Override
  public DynamicTableSink createDynamicTableSink(Context context) {
    List<String> partitionKeys = context.getCatalogTable().getPartitionKeys();
    final FactoryUtil.TableFactoryHelper helper =
        FactoryUtil.createTableFactoryHelper(this, context);
    helper.validate();
    final ReadableConfig tableOptions = helper.getOptions();
    fieldDataTypes = context.getCatalogTable().getResolvedSchema().getColumnDataTypes();
    fieldNames = context.getCatalogTable().getResolvedSchema().getColumnNames();

    createBigQueryConfig(tableOptions);
    try {
      createBigQueryTable();
    } catch (JSQLParserException e) {
      log.error("Error while Creating big query table");
      throw new FlinkBigQueryException("Error while Creating big query table:", e);
    }
    return new BigQueryDynamicTableSink(
        fieldNames,
        fieldDataTypes,
        bqconfig,
        bigQueryWriteClientFactory,
        partitionKeys,
        accumulator);
  }

  private void createBigQueryConfig(ReadableConfig tableOptions) {
    ImmutableMap<String, String> defaultOptions = ImmutableMap.of("bqWrite", "bqWrite");
    bqconfig =
        FlinkBigQueryConfig.from(
            requiredOptions(),
            optionalOptions(),
            tableOptions,
            defaultOptions,
            new org.apache.hadoop.conf.Configuration(),
            1,
            new org.apache.flink.configuration.Configuration(),
            EnvironmentInformation.getVersion(),
            Optional.empty());
    BigQueryCredentialsSupplier bigQueryCredentialsSupplier =
        new BigQueryCredentialsSupplier(
            bqconfig.getAccessToken(),
            bqconfig.getCredentialsKey(),
            bqconfig.getCredentialsFile(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty());

    FlinkBigQueryConnectorUserAgentProvider agentProvider =
        new FlinkBigQueryConnectorUserAgentProvider(EnvironmentInformation.getVersion());
    UserAgentHeaderProvider userAgentHeaderProvider =
        new UserAgentHeaderProvider(agentProvider.getUserAgent());
    this.bigQueryWriteClientFactory =
        new BigQueryClientFactory(bigQueryCredentialsSupplier, userAgentHeaderProvider, bqconfig);
  }

  void createBigQueryTable() throws JSQLParserException {
    TableId tableId = bqconfig.getTableId();
    if (bigQueryClient == null) {
      final BigQuery bigquery =
          BigQueryOptions.newBuilder()
              .setCredentials(bqconfig.createCredentials())
              .build()
              .getService();
      Cache<String, TableInfo> destinationTableCache =
          CacheBuilder.newBuilder()
              .expireAfterWrite(bqconfig.getCacheExpirationTimeInMinutes(), TimeUnit.MINUTES)
              .maximumSize(1000)
              .build();
      bigQueryClient =
          new BigQueryClient(
              bigquery,
              Optional.of(bqconfig.getTableId().getProject()),
              Optional.of(bqconfig.getTableId().getDataset()),
              destinationTableCache,
              ImmutableMap.of());
    }
    boolean destTableExists = bigQueryClient.tableExists(tableId);
    if (!destTableExists) {
      List<DataType> columnDataTypeList = fieldDataTypes;
      List<String> columnNameList = fieldNames;
      List<Field> listOfFields = new ArrayList<Field>();
      for (int i = 0; i < columnNameList.size(); i++) {
        RowField rowField =
            new RowField(
                columnNameList.get(i).toString(), columnDataTypeList.get(i).getLogicalType());
        listOfFields.addAll(getFields(rowField, null));
      }
      FieldList fieldlist = FieldList.of(listOfFields);
      Schema schema = Schema.of(fieldlist);
      bigQueryClient.createTable(tableId, schema);
    }
  }
}
