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
import com.google.cloud.flink.bigquery.common.FlinkBigQueryConnectorUserAgentProvider;
import com.google.cloud.flink.bigquery.common.UserAgentHeaderProvider;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import net.sf.jsqlparser.JSQLParserException;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class BigQueryDynamicTableFactory implements DynamicTableSourceFactory {

  private static final Logger log = LoggerFactory.getLogger(BigQueryDynamicTableFactory.class);
  private FlinkBigQueryConfig bqconfig;
  private BigQueryClientFactory bigQueryReadClientFactory;
  public static final ConfigOption<String> TABLE =
      ConfigOptions.key("table").stringType().noDefaultValue();
  public static final ConfigOption<String> QUERY =
      ConfigOptions.key("query").stringType().noDefaultValue();
  public static final ConfigOption<String> FILTER =
      ConfigOptions.key("filter").stringType().defaultValue("");
  public static final ConfigOption<String> FLINK_VERSION =
      ConfigOptions.key("flinkVersion").stringType().defaultValue("1.11");
  public static final ConfigOption<Integer> PARALLELISM =
      ConfigOptions.key("parallelism").intType().defaultValue(1);
  public static final ConfigOption<Integer> MAX_PARALLELISM =
      ConfigOptions.key("maxParallelism").intType().defaultValue(1);
  public static final ConfigOption<String> SELECTED_FIELDS =
      ConfigOptions.key("selectedFields").stringType().noDefaultValue();
  public static final ConfigOption<Integer> DEFAULT_PARALLELISM =
      ConfigOptions.key("defaultParallelism").intType().defaultValue(1);
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
  public static final ConfigOption<String> BQ_ENCODED_CREATER_READSESSION_REQUEST =
      ConfigOptions.key("bqEncodedCreateReadSessionRequest").stringType().noDefaultValue();
  public static final ConfigOption<String> BQ_BACKGROUND_THREADS_PER_STREAM =
      ConfigOptions.key("bqBackgroundThreadsPerStream").stringType().noDefaultValue();
  public static final ConfigOption<String> MATERIALIZATION_PROJECT =
      ConfigOptions.key("materializationProject").stringType().noDefaultValue();
  public static final ConfigOption<String> MATERIALIZATION_DATASET =
      ConfigOptions.key("materializationDataset").stringType().noDefaultValue();
  public static final ConfigOption<String> ARROW_COMPRESSION_CODEC =
      ConfigOptions.key("arrowCompressionCodec").stringType().noDefaultValue();
  public static ConfigOption<String> READ_SESSION_ARROW_SCHEMA_FIELDS;

  @Override
  public String factoryIdentifier() {
    return "bigquery";
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    final Set<ConfigOption<?>> options = new HashSet<>();
    options.add(TABLE);
    options.add(SELECTED_FIELDS);
    options.add(QUERY);
    options.add(MATERIALIZATION_PROJECT);
    options.add(MATERIALIZATION_DATASET);
    return options;
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    final Set<ConfigOption<?>> options = new HashSet<>();
    options.add(FactoryUtil.FORMAT);
    options.add(CREDENTIAL_KEY_FILE);
    options.add(ACCESS_TOKEN);
    options.add(CREDENTIALS_KEY);
    options.add(FILTER);
    options.add(FLINK_VERSION);
    options.add(DEFAULT_PARALLELISM);
    options.add(PROXY_URI);
    options.add(PROXY_USERNAME);
    options.add(PROXY_PASSWORD);
    options.add(BQ_ENCODED_CREATER_READSESSION_REQUEST);
    options.add(BQ_BACKGROUND_THREADS_PER_STREAM);
    options.add(PARALLELISM);
    options.add(MAX_PARALLELISM);
    options.add(ARROW_COMPRESSION_CODEC);
    return options;
  }

  DecodingFormat<DeserializationSchema<RowData>> decodingFormat;
  private String arrowReadSessionSchema;

  @Override
  public DynamicTableSource createDynamicTableSource(Context context) {
    final FactoryUtil.TableFactoryHelper helper =
        FactoryUtil.createTableFactoryHelper(this, context);
    final ReadableConfig options = helper.getOptions();
    try {
      helper.validate();
    } catch (Exception ex) {
      String exceptionString = ensureExpectedException(ex.toString(), options);
      if (exceptionString != null) {
        throw new IllegalArgumentException(exceptionString);
      }
    }

    decodingFormat = helper.discoverDecodingFormat(ArrowFormatFactory.class, FactoryUtil.FORMAT);

    final DataType producedDataType = context.getCatalogTable().getSchema().toPhysicalRowDataType();
    return new BigQueryDynamicTableSource(
        decodingFormat, producedDataType, getReadStreamNames(options), bigQueryReadClientFactory);
  }

  private LinkedList<String> getReadStreamNames(ReadableConfig options) {
    bigQueryReadClientFactory = null;
    UserAgentHeaderProvider userAgentHeaderProvider;
    BigQueryCredentialsSupplier bigQueryCredentialsSupplier;
    LinkedList<String> readStreamNames = new LinkedList<String>();
    try {
      ImmutableMap<String, String> defaultOptions =
          ImmutableMap.of("flinkVersion", options.get(FLINK_VERSION));

      bqconfig =
          FlinkBigQueryConfig.from(
              requiredOptions(),
              optionalOptions(),
              options,
              defaultOptions,
              new org.apache.hadoop.conf.Configuration(),
              options.get(DEFAULT_PARALLELISM),
              new org.apache.flink.configuration.Configuration(),
              options.get(FLINK_VERSION),
              Optional.empty());

      Credentials credentials = bqconfig.createCredentials();
      bigQueryCredentialsSupplier =
          new BigQueryCredentialsSupplier(
              bqconfig.getAccessToken(),
              bqconfig.getCredentialsKey(),
              bqconfig.getCredentialsFile(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty());

      FlinkBigQueryConnectorUserAgentProvider agentProvider =
          new FlinkBigQueryConnectorUserAgentProvider(options.get(FLINK_VERSION));
      userAgentHeaderProvider = new UserAgentHeaderProvider(agentProvider.getUserAgent());
      bigQueryReadClientFactory =
          new BigQueryClientFactory(bigQueryCredentialsSupplier, userAgentHeaderProvider, bqconfig);

      // Create read session
      ReadSession readSession =
          BigQueryReadSession.getReadsession(credentials, bqconfig, bigQueryReadClientFactory);
      List<ReadStream> readsessionList = readSession.getStreamsList();
      Schema arrowReadSchema =
          MessageSerializer.deserializeSchema(
              new ReadChannel(
                  new ByteArrayReadableSeekableByteChannel(
                      readSession.getArrowSchema().getSerializedSchema().toByteArray())));
      this.arrowReadSessionSchema = arrowReadSchema.toJson();
      String fieldList = new String();
      for (int i = 0; i < arrowReadSchema.getFields().size(); i++) {
        fieldList = fieldList.concat(arrowReadSchema.getFields().get(i).getName() + ",");
      }
      bqconfig.setArrowSchemaFields(fieldList);
      ConfigOption<String> readSessionSchemaFields =
          ConfigOptions.key("readSessionSchemaFields").stringType().defaultValue(fieldList);
      optionalOptions().add(readSessionSchemaFields);
      for (ReadStream stream : readsessionList) {
        readStreamNames.add(stream.getName());
      }
    } catch (JSQLParserException | IOException ex) {
      log.error("Error while reading big query session", ex);
    }
    return readStreamNames;
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
    if (options.get(TABLE) != null) {
      missingArgs.remove("query");
      missingArgs.remove("materializationProject");
      missingArgs.remove("materializationDataset");
    } else if (options.get(QUERY) != null) {
      missingArgs.remove("table");
      missingArgs.remove("selectedFields");
    }
    if (!missingArgs.isEmpty()) {
      errorString = errorString + exceptionHead + "\n\n" + String.join("\n", missingArgs);
    }
    return errorString;
  }
}
