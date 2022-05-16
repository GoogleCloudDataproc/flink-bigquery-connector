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

import static com.google.cloud.flink.bigquery.ProtobufUtils.getListOfSubFields;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Field.Mode;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.bigquery.connector.common.BigQueryClientFactory;
import com.google.cloud.bigquery.connector.common.BigQueryCredentialsSupplier;
import com.google.cloud.flink.bigquery.common.FlinkBigQueryConnectorUserAgentProvider;
import com.google.cloud.flink.bigquery.common.UserAgentHeaderProvider;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nullable;
import net.sf.jsqlparser.JSQLParserException;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.OverwritableTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.table.utils.TableConnectorUtils;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BigQueryTableSink implements AppendStreamTableSink<Row>, OverwritableTableSink {

  private static final Logger log = LoggerFactory.getLogger(BigQueryTableSink.class);
  public static final ConfigOption<String> TABLE =
      ConfigOptions.key("table").stringType().noDefaultValue();
  public static final ConfigOption<String> PARTITION_FIELD =
      ConfigOptions.key("partitionField").stringType().defaultValue("");
  public static final ConfigOption<String> PARTITION_TYPE =
      ConfigOptions.key("partitionType").stringType().defaultValue("");
  public static final ConfigOption<String> PARTITION_EXPIRATION_MS =
      ConfigOptions.key("partitionExpirationMs").stringType().defaultValue("");
  public static final ConfigOption<String> PARTITION_REQUIRE_FILTER =
      ConfigOptions.key("partitionRequireFilter").stringType().defaultValue("");
  public static final ConfigOption<String> FLINK_VERSION =
      ConfigOptions.key("flinkVersion").stringType().defaultValue("1.11");
  public static final ConfigOption<String> ACCESS_TOKEN =
      ConfigOptions.key("gcpAccessToken").stringType().defaultValue("");
  public static final ConfigOption<String> CREDENTIAL_KEY_FILE =
      ConfigOptions.key("credentialsFile").stringType().noDefaultValue();
  public static final ConfigOption<String> CREDENTIALS_KEY =
      ConfigOptions.key("credentials").stringType().defaultValue("");
  public static final ConfigOption<String> PROXY_URI =
      ConfigOptions.key("proxyUri").stringType().defaultValue("");
  public static final ConfigOption<String> PROXY_USERNAME =
      ConfigOptions.key("proxyUsername").stringType().defaultValue("");
  public static final ConfigOption<String> PROXY_PASSWORD =
      ConfigOptions.key("proxyPassword").stringType().defaultValue("");
  public static final ConfigOption<Integer> DEFAULT_PARALLELISM =
      ConfigOptions.key("defaultParallelism").intType().defaultValue(1);
  private String table;
  private String[] fieldNames;
  private DataType[] fieldTypes;
  private DataStreamSink<Row> sink;
  public Configuration allOptions = new Configuration();
  private FlinkBigQueryConfig bqconfig;
  private BigQueryClientFactory bigQueryWriteClientFactory;
  private BigQueryClient bigQueryClient;

  public BigQueryTableSink(Table srcTable, String bigqueryWriteTable) {
    this.table = bigqueryWriteTable;
    this.fieldNames = srcTable.getSchema().getFieldNames();
    this.fieldTypes = srcTable.getSchema().getFieldDataTypes();
  }

  @Override
  public DataStreamSink<Row> consumeDataStream(DataStream<Row> dataStream) {
    createBigQueryConfig();
    try {
      createBigQueryTable();
    } catch (JSQLParserException e) {
      log.error("Error while creating big query table:", e);
      throw new FlinkBigQueryException("Error while creating big query table:", e);
    }
    BigQuerySinkFunction bigQuerySinkFunction =
        new BigQuerySinkFunction(fieldNames, fieldTypes, bqconfig, bigQueryWriteClientFactory);
    this.sink =
        dataStream
            .rebalance()
            .addSink(bigQuerySinkFunction)
            .setParallelism(dataStream.getParallelism())
            .name(TableConnectorUtils.generateRuntimeName(BigQueryTableSink.class, fieldNames));
    return sink;
  }

  private void createBigQueryConfig() {
    ImmutableMap<String, String> defaultOptions = ImmutableMap.of("bqWrite", "bqWrite");
    this.allOptions.set(TABLE, table);
    bqconfig =
        FlinkBigQueryConfig.from(
            requiredOptions(),
            optionalOptions(),
            allOptions,
            defaultOptions,
            new org.apache.hadoop.conf.Configuration(),
            allOptions.get(DEFAULT_PARALLELISM),
            new org.apache.flink.configuration.Configuration(),
            allOptions.get(FLINK_VERSION),
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
        new FlinkBigQueryConnectorUserAgentProvider(allOptions.get(FLINK_VERSION));
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
      bigQueryClient =
          new BigQueryClient(
              bigquery, Optional.of(tableId.getProject()), Optional.of(tableId.getDataset()));
    }
    boolean destTableExists = bigQueryClient.tableExists(tableId);
    if (!destTableExists) {
      List<DataType> columnDataTypeList = Arrays.asList(fieldTypes);
      List<String> columnNameList = Arrays.asList(fieldNames);
      ArrayList<RowField> listOfRowFields = new ArrayList<RowField>();
      for (int i = 0; i < columnNameList.size(); i++) {
        listOfRowFields.add(
            new RowField(
                columnNameList.get(i).toString(), columnDataTypeList.get(i).getLogicalType()));
      }
      RowType flinkSchema = new RowType(listOfRowFields);

      ArrayList<Field> listOfFields = new ArrayList<Field>();

      Iterator<RowField> rowFieldItrator = flinkSchema.getFields().iterator();
      while (rowFieldItrator.hasNext()) {
        RowField elem = rowFieldItrator.next();
        if ("ROW".equals(elem.getType().getTypeRoot().toString())) {
          listOfFields.add(
              Field.newBuilder(
                      elem.getName(),
                      StandardSQLTypeName.STRUCT,
                      FieldList.of(getListOfSubFields(elem.getType())))
                  .setMode(elem.getType().isNullable() ? Mode.NULLABLE : Mode.REQUIRED)
                  .build());
        } else if ("ARRAY".equals(elem.getType().getTypeRoot().toString())) {
          listOfFields.add(
              Field.newBuilder(
                      elem.getName(),
                      StandardSQLTypeHandler.handle(((ArrayType) elem.getType()).getElementType()))
                  .setMode(Mode.REPEATED)
                  .build());
        } else {
          listOfFields.add(
              Field.newBuilder(elem.getName(), StandardSQLTypeHandler.handle(elem.getType()))
                  .setMode(elem.getType().isNullable() ? Mode.NULLABLE : Mode.REQUIRED)
                  .build());
        }
      }
      FieldList fieldlist = FieldList.of(listOfFields);
      Schema schema = Schema.of(fieldlist);
      bigQueryClient.createTable(tableId, schema);
    }
  }

  @Override
  public TableSink<Row> configure(String[] fieldNames, TypeInformation[] fieldTypes) {
    return null;
  }

  @Override
  public DataType getConsumedDataType() {
    return getTableSchema().toRowDataType();
  }

  @Override
  public TableSchema getTableSchema() {
    return TableSchema.builder().fields(fieldNames, fieldTypes).build();
  }

  @Override
  public void setOverwrite(boolean overwrite) {}

  public void setFinkVersion(@Nullable String flinkVersion) {
    if (flinkVersion != null) this.allOptions.set(FLINK_VERSION, flinkVersion);
  }

  public void setGcpCredentialFile(@Nullable String credentialsFile) {
    if (credentialsFile != null) this.allOptions.set(CREDENTIAL_KEY_FILE, credentialsFile);
  }

  public void setGcpCredentialKey(@Nullable String credentials) {
    if (credentials != null) this.allOptions.set(CREDENTIALS_KEY, credentials);
  }

  public void setGcpAccessToken(@Nullable String gcpAccessToken) {
    if (gcpAccessToken != null) this.allOptions.set(ACCESS_TOKEN, gcpAccessToken);
  }

  public void setProxyDetails(
      String proxyUri, @Nullable String proxyUsername, @Nullable String proxyPassword) {
    if (proxyUri != null) this.allOptions.set(PROXY_URI, proxyUri);
    if (proxyUsername != null) this.allOptions.set(PROXY_USERNAME, proxyUsername);
    if (proxyPassword != null) this.allOptions.set(PROXY_PASSWORD, proxyPassword);
  }

  public void setDefaultParallelism(int defaultParallelism) {
    if (defaultParallelism != 0) this.allOptions.set(DEFAULT_PARALLELISM, defaultParallelism);
  }

  public void setPartitionDetails(
      String partitionField,
      @Nullable String partitionType,
      @Nullable String partitionExpirationMs,
      @Nullable String partitionRequireFilter) {
    this.allOptions.set(PARTITION_FIELD, partitionField);
    if (partitionType != null) this.allOptions.set(PARTITION_TYPE, partitionType);
    if (partitionExpirationMs != null)
      this.allOptions.set(PARTITION_EXPIRATION_MS, partitionExpirationMs);
    if (partitionRequireFilter != null)
      this.allOptions.set(PARTITION_REQUIRE_FILTER, partitionRequireFilter);
  }

  public Set<ConfigOption<?>> requiredOptions() {
    final Set<ConfigOption<?>> options = new HashSet<>();
    options.add(TABLE);
    return options;
  }

  public Set<ConfigOption<?>> optionalOptions() {
    final Set<ConfigOption<?>> options = new HashSet<>();
    options.add(CREDENTIAL_KEY_FILE);
    options.add(ACCESS_TOKEN);
    options.add(CREDENTIALS_KEY);
    options.add(FLINK_VERSION);
    options.add(PARTITION_FIELD);
    options.add(PARTITION_TYPE);
    options.add(PARTITION_EXPIRATION_MS);
    options.add(PARTITION_REQUIRE_FILTER);
    options.add(PROXY_URI);
    options.add(PROXY_USERNAME);
    options.add(PROXY_PASSWORD);
    return options;
  }
}
