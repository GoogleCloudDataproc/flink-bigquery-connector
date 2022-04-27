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

import static com.google.cloud.bigquery.connector.common.BigQueryUtil.firstPresent;
import static com.google.cloud.bigquery.connector.common.BigQueryUtil.parseTableId;
import static java.lang.String.format;

import com.google.api.gax.retrying.RetrySettings;
import com.google.auth.Credentials;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TimePartitioning;
import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.bigquery.connector.common.BigQueryConfig;
import com.google.cloud.bigquery.connector.common.BigQueryCredentialsSupplier;
import com.google.cloud.bigquery.connector.common.BigQueryProxyConfig;
import com.google.cloud.bigquery.connector.common.ReadSessionCreatorConfig;
import com.google.cloud.bigquery.connector.common.ReadSessionCreatorConfigBuilder;
import com.google.cloud.bigquery.storage.v1.ArrowSerializationOptions.CompressionCodec;
import com.google.cloud.bigquery.storage.v1.DataFormat;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.Serializable;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.util.TablesNamesFinder;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.hadoop.conf.Configuration;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.bp.Duration;

public class FlinkBigQueryConfig implements BigQueryConfig, Serializable {

  private static final long serialVersionUID = 1L;
  final Logger logger = LoggerFactory.getLogger(FlinkBigQueryConfig.class);

  public enum WriteMethod {
    DIRECT,
    INDIRECT;

    public static WriteMethod from(@Nullable String writeMethod) {
      try {
        return WriteMethod.valueOf(writeMethod.toUpperCase(Locale.ENGLISH));
      } catch (RuntimeException e) {
        throw new IllegalArgumentException(
            "WriteMethod can be only " + Arrays.toString(WriteMethod.values()));
      }
    }
  }

  public static final String VIEWS_ENABLED_OPTION = "viewsEnabled";
  public static final String USE_AVRO_LOGICAL_TYPES_OPTION = "useAvroLogicalTypes";
  public static final String DATE_PARTITION_PARAM = "datePartition";
  public static final String VALIDATE_SPARK_AVRO_PARAM = "validateSparkAvroInternalParam";
  public static final String INTERMEDIATE_FORMAT_OPTION = "intermediateFormat";
  public static final int DEFAULT_MATERIALIZATION_EXPRIRATION_TIME_IN_MINUTES = 24 * 60;
  @VisibleForTesting static final DataFormat DEFAULT_READ_DATA_FORMAT = DataFormat.ARROW;

  @VisibleForTesting
  static final CompressionCodec DEFAULT_ARROW_COMPRESSION_CODEC =
      CompressionCodec.COMPRESSION_UNSPECIFIED;

  static final String GCS_CONFIG_CREDENTIALS_FILE_PROPERTY =
      "google.cloud.auth.service.account.json.keyfile";
  static final String GCS_CONFIG_PROJECT_ID_PROPERTY = "q-gcp-6750-pso-gs-flink-22-01";
  private static final String READ_DATA_FORMAT_OPTION = DataFormat.ARROW.toString();
  private static final ImmutableList<String> PERMITTED_READ_DATA_FORMATS =
      ImmutableList.of(DataFormat.ARROW.toString(), DataFormat.AVRO.toString());
  private static final Supplier<com.google.common.base.Optional<String>> DEFAULT_FALLBACK =
      () -> empty();
  private static final String CONF_PREFIX = "flink.datasource.bigquery.";
  private static final int DEFAULT_BIGQUERY_CLIENT_CONNECT_TIMEOUT = 60 * 1000;
  private static final int DEFAULT_BIGQUERY_CLIENT_READ_TIMEOUT = 60 * 1000;
  private static final Pattern LOWERCASE_QUERY_PATTERN = Pattern.compile("^(select|with)\\s+.*$");

  public static final int MIN_BUFFERED_RESPONSES_PER_STREAM = 1;
  public static final int MIN_STREAMS_PER_PARTITION = 1;
  private static final int DEFAULT_BIGQUERY_CLIENT_RETRIES = 10;
  private static final String ARROW_COMPRESSION_CODEC_OPTION = "arrowCompressionCodec";
  private static final WriteMethod DEFAULT_WRITE_METHOD = WriteMethod.INDIRECT;

  TableId tableId;
  String selectedFields;
  String flinkVersion;
  com.google.common.base.Optional<String> query = empty();
  String parentProjectId;
  boolean useParentProjectForMetadataOperations;
  com.google.common.base.Optional<String> credentialsKey;
  com.google.common.base.Optional<String> credentialsFile;
  com.google.common.base.Optional<String> accessToken;
  com.google.common.base.Optional<String> filter = empty();
  com.google.common.base.Optional<TableSchema> schema = empty();
  Integer maxParallelism = null;
  int defaultParallelism = 1;
  com.google.common.base.Optional<String> temporaryGcsBucket = empty();
  com.google.common.base.Optional<String> persistentGcsBucket = empty();
  com.google.common.base.Optional<String> persistentGcsPath = empty();

  DataFormat readDataFormat = DEFAULT_READ_DATA_FORMAT;
  boolean combinePushedDownFilters = true;
  boolean viewsEnabled = false;
  com.google.common.base.Optional<String> materializationProject = empty();
  com.google.common.base.Optional<String> materializationDataset = empty();
  com.google.common.base.Optional<String> partitionField = empty();
  Long partitionExpirationMs = null;
  com.google.common.base.Optional<Boolean> partitionRequireFilter = empty();
  com.google.common.base.Optional<TimePartitioning.Type> partitionType = empty();
  com.google.common.base.Optional<String[]> clusteredFields = empty();
  com.google.common.base.Optional<JobInfo.CreateDisposition> createDisposition = empty();
  boolean optimizedEmptyProjection = true;
  boolean useAvroLogicalTypes = false;
  ImmutableList<JobInfo.SchemaUpdateOption> loadSchemaUpdateOptions = ImmutableList.of();
  int materializationExpirationTimeInMinutes = DEFAULT_MATERIALIZATION_EXPRIRATION_TIME_IN_MINUTES;
  int maxReadRowsRetries = 3;
  boolean pushAllFilters = true;
  private com.google.common.base.Optional<String> encodedCreateReadSessionRequest = empty();
  private com.google.common.base.Optional<String> storageReadEndpoint = empty();
  private int numBackgroundThreadsPerStream = 0;
  private int numPrebufferReadRowsResponses = MIN_BUFFERED_RESPONSES_PER_STREAM;
  private int numStreamsPerPartition = MIN_STREAMS_PER_PARTITION;
  private FlinkBigQueryProxyAndHttpConfig flinkBigQueryProxyAndHttpConfig;
  private CompressionCodec arrowCompressionCodec = DEFAULT_ARROW_COMPRESSION_CODEC;
  private WriteMethod writeMethod = DEFAULT_WRITE_METHOD;
  // for V2 write with BigQuery Storage Write API
  RetrySettings bigqueryDataWriteHelperRetrySettings =
      RetrySettings.newBuilder().setMaxAttempts(5).build();
  private String fieldList;

  @VisibleForTesting
  public static FlinkBigQueryConfig from(
      Set<ConfigOption<?>> requiredOptions,
      Set<ConfigOption<?>> optionalOptions,
      ReadableConfig readableConfigOptions,
      ImmutableMap<String, String> originalGlobalOptions,
      Configuration hadoopConfiguration,
      int defaultParallelism,
      org.apache.flink.configuration.Configuration sqlConf,
      String flinkVersion,
      Optional<TableSchema> schema) {
    FlinkBigQueryConfig config = new FlinkBigQueryConfig();
    ImmutableMap<String, String> options =
        toConfigOptionsKeyMap(requiredOptions, optionalOptions, readableConfigOptions);
    ImmutableMap<String, String> globalOptions = normalizeConf(originalGlobalOptions);
    config.flinkBigQueryProxyAndHttpConfig =
        FlinkBigQueryProxyAndHttpConfig.from(options, globalOptions, hadoopConfiguration);

    // we need those parameters in case a read from query is issued
    config.viewsEnabled = getAnyBooleanOption(globalOptions, options, VIEWS_ENABLED_OPTION, false);
    config.flinkVersion = flinkVersion;
    config.materializationProject =
        getAnyOption(
            globalOptions,
            options,
            ImmutableList.of("materializationProject", "viewMaterializationProject"));
    config.materializationDataset =
        getAnyOption(
            globalOptions,
            options,
            ImmutableList.of("materializationDataset", "viewMaterializationDataset"));
    config.materializationExpirationTimeInMinutes =
        getAnyOption(globalOptions, options, "materializationExpirationTimeInMinutes")
            .transform(Integer::parseInt)
            .or(DEFAULT_MATERIALIZATION_EXPRIRATION_TIME_IN_MINUTES);
    if (config.materializationExpirationTimeInMinutes < 1) {
      throw new IllegalArgumentException(
          "materializationExpirationTimeInMinutes must have a positive value, the configured value"
              + " is "
              + config.materializationExpirationTimeInMinutes);
    }
    // get the table details
    Optional<String> tableParam =
        getOptionFromMultipleParams(options, ImmutableList.of("table", "path"), DEFAULT_FALLBACK)
            .toJavaUtil();
    Optional<String> datasetParam =
        getOption(options, "dataset").or(config.materializationDataset).toJavaUtil();
    Optional<String> projectParam =
        firstPresent(
            getOption(options, "project").toJavaUtil(),
            com.google.common.base.Optional.fromNullable(
                    hadoopConfiguration.get(GCS_CONFIG_PROJECT_ID_PROPERTY))
                .toJavaUtil());
    config.partitionType =
        getOption(options, "partitionType").transform(TimePartitioning.Type::valueOf);
    Optional<String> datePartitionParam = getOption(options, DATE_PARTITION_PARAM).toJavaUtil();
    datePartitionParam.ifPresent(
        date -> validateDateFormat(date, config.getPartitionTypeOrDefault(), DATE_PARTITION_PARAM));
    // checking for query
    if (tableParam.isPresent()) {
      String tableParamStr = tableParam.get().trim().replaceAll("\\s+", " ");
      if (isQuery(tableParamStr)) {
        // it is a query in practice
        config.query = com.google.common.base.Optional.of(tableParamStr);
        config.tableId = parseTableId("QUERY", datasetParam, projectParam, datePartitionParam);
      } else {
        config.selectedFields = options.get("selectedFields");
        config.tableId =
            parseTableId(tableParamStr, datasetParam, projectParam, datePartitionParam);
      }
    } else {
      // no table has been provided, it is either a query or an error
      config.query = getOption(options, "query").transform(String::trim);
      if (config.query.isPresent()) {
        config.tableId = parseTableId("QUERY", datasetParam, projectParam, datePartitionParam);
      } else {
        // No table nor query were set. We cannot go further.
        throw new IllegalArgumentException("No table has been specified");
      }
    }

    config.parentProjectId =
        getAnyOption(globalOptions, options, "parentProject").or(defaultBilledProject());
    config.useParentProjectForMetadataOperations =
        getAnyBooleanOption(globalOptions, options, "useParentProjectForMetadataOperations", false);
    config.credentialsKey = getAnyOption(globalOptions, options, "credentials");
    config.credentialsFile =
        fromJavaUtil(
            firstPresent(
                getAnyOption(globalOptions, options, "credentialsFile").toJavaUtil(),
                com.google.common.base.Optional.fromNullable(
                        hadoopConfiguration.get(GCS_CONFIG_CREDENTIALS_FILE_PROPERTY))
                    .toJavaUtil()));
    config.accessToken = getAnyOption(globalOptions, options, "gcpAccessToken");
    config.filter = getOption(options, "filter");
    config.schema = fromJavaUtil(schema);
    config.maxParallelism =
        getOptionFromMultipleParams(
                options, ImmutableList.of("maxParallelism", "parallelism"), DEFAULT_FALLBACK)
            .transform(Integer::valueOf)
            .orNull();
    config.defaultParallelism = defaultParallelism;
    config.temporaryGcsBucket = getAnyOption(globalOptions, options, "temporaryGcsBucket");
    config.persistentGcsBucket = getAnyOption(globalOptions, options, "persistentGcsBucket");
    config.persistentGcsPath = getOption(options, "persistentGcsPath");
    String readDataFormatParam =
        getAnyOption(globalOptions, options, "format")
            .transform(String::toUpperCase)
            .or(DEFAULT_READ_DATA_FORMAT.toString());
    if (!PERMITTED_READ_DATA_FORMATS.contains(readDataFormatParam)) {
      throw new IllegalArgumentException(
          format(
              "Data read format '%s' is not supported. Supported formats are '%s'",
              readDataFormatParam, String.join(",", PERMITTED_READ_DATA_FORMATS)));
    }
    config.useAvroLogicalTypes =
        getAnyBooleanOption(globalOptions, options, USE_AVRO_LOGICAL_TYPES_OPTION, false);
    config.readDataFormat = DataFormat.valueOf(readDataFormatParam);
    config.combinePushedDownFilters =
        getAnyBooleanOption(globalOptions, options, "combinePushedDownFilters", true);

    config.partitionField = getOption(options, "partitionField");
    config.partitionExpirationMs =
        getOption(options, "partitionExpirationMs").transform(Long::valueOf).orNull();
    config.partitionRequireFilter =
        getOption(options, "partitionRequireFilter").transform(Boolean::valueOf);
    config.clusteredFields = getOption(options, "clusteredFields").transform(s -> s.split(","));

    config.createDisposition =
        getOption(options, "createDisposition")
            .transform(String::toUpperCase)
            .transform(JobInfo.CreateDisposition::valueOf);

    config.optimizedEmptyProjection =
        getAnyBooleanOption(globalOptions, options, "optimizedEmptyProjection", true);

    boolean allowFieldAddition =
        getAnyBooleanOption(globalOptions, options, "allowFieldAddition", false);
    boolean allowFieldRelaxation =
        getAnyBooleanOption(globalOptions, options, "allowFieldRelaxation", false);
    ImmutableList.Builder<JobInfo.SchemaUpdateOption> loadSchemaUpdateOptions =
        ImmutableList.builder();
    if (allowFieldAddition) {
      loadSchemaUpdateOptions.add(JobInfo.SchemaUpdateOption.ALLOW_FIELD_ADDITION);
    }
    if (allowFieldRelaxation) {
      loadSchemaUpdateOptions.add(JobInfo.SchemaUpdateOption.ALLOW_FIELD_RELAXATION);
    }
    config.loadSchemaUpdateOptions = loadSchemaUpdateOptions.build();
    config.storageReadEndpoint = getAnyOption(globalOptions, options, "bqStorageReadEndpoint");
    config.encodedCreateReadSessionRequest =
        getAnyOption(globalOptions, options, "bqEncodedCreateReadSessionRequest");
    config.numBackgroundThreadsPerStream =
        getAnyOption(globalOptions, options, "bqBackgroundThreadsPerStream")
            .transform(Integer::parseInt)
            .or(0);
    config.pushAllFilters = getAnyBooleanOption(globalOptions, options, "pushAllFilters", true);
    config.numPrebufferReadRowsResponses =
        getAnyOption(globalOptions, options, "bqPrebufferResponsesPerStream")
            .transform(Integer::parseInt)
            .or(MIN_BUFFERED_RESPONSES_PER_STREAM);
    config.numStreamsPerPartition =
        getAnyOption(globalOptions, options, "bqNumStreamsPerPartition")
            .transform(Integer::parseInt)
            .or(MIN_STREAMS_PER_PARTITION);

    String arrowCompressionCodecParam =
        getAnyOption(globalOptions, options, ARROW_COMPRESSION_CODEC_OPTION)
            .transform(String::toUpperCase)
            .or(DEFAULT_ARROW_COMPRESSION_CODEC.toString());

    config.writeMethod =
        getAnyOption(globalOptions, options, "writeMethod")
            .transform(WriteMethod::from)
            .or(DEFAULT_WRITE_METHOD);

    try {
      config.arrowCompressionCodec = CompressionCodec.valueOf(arrowCompressionCodecParam);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          format(
              "Compression codec '%s' for Arrow is not supported. Supported formats are %s",
              arrowCompressionCodecParam, Arrays.toString(CompressionCodec.values())));
    }

    return config;
  }

  private static ImmutableMap<String, String> toConfigOptionsKeyMap(
      Set<ConfigOption<?>> requiredOptions,
      Set<ConfigOption<?>> optionalOptions,
      ReadableConfig readableConfig) {

    Map<String, String> result = new HashMap<>();

    requiredOptions.stream()
        .forEach(
            ele ->
                result.put(
                    ele.key(),
                    readableConfig.get(
                        ConfigOptions.key(ele.key()).stringType().noDefaultValue())));
    optionalOptions.stream()
        .filter(ele -> Objects.nonNull(ele))
        .forEach(
            ele ->
                result.put(
                    ele.key(),
                    readableConfig.get(
                        ConfigOptions.key(ele.key()).stringType().noDefaultValue())));
    result.values().removeIf(Objects::isNull);
    return ImmutableMap.copyOf(result);
  }

  @VisibleForTesting
  static boolean isQuery(String tableParamStr) {
    String potentialQuery = tableParamStr.toLowerCase().replace('\n', ' ');
    return LOWERCASE_QUERY_PATTERN.matcher(potentialQuery).matches();
  }

  private static void validateDateFormat(
      String date, TimePartitioning.Type partitionType, String optionName) {
    try {
      Map<TimePartitioning.Type, DateTimeFormatter> formatterMap =
          ImmutableMap.<TimePartitioning.Type, DateTimeFormatter>of(
              TimePartitioning.Type.HOUR, DateTimeFormatter.ofPattern("yyyyMMddHH"), //
              TimePartitioning.Type.DAY, DateTimeFormatter.BASIC_ISO_DATE, //
              TimePartitioning.Type.MONTH, DateTimeFormatter.ofPattern("yyyyMM"), //
              TimePartitioning.Type.YEAR, DateTimeFormatter.ofPattern("yyyy"));
      DateTimeFormatter dateTimeFormatter = formatterMap.get(partitionType);
      dateTimeFormatter.parse(date);
    } catch (DateTimeParseException e) {
      throw new IllegalArgumentException(
          String.format("Invalid argument for option %s, format is YYYYMMDD", optionName));
    }
  }

  private static com.google.common.base.Supplier<String> defaultBilledProject() {
    return () -> BigQueryOptions.getDefaultInstance().getProjectId();
  }

  private static String getRequiredOption(Map<String, String> options, String name) {
    return getOption(options, name, DEFAULT_FALLBACK)
        .toJavaUtil()
        .orElseThrow(() -> new IllegalArgumentException(format("Option %s required.", name)));
  }

  private static String getRequiredOption(
      Map<String, String> options, String name, com.google.common.base.Supplier<String> fallback) {
    return getOption(options, name, DEFAULT_FALLBACK).or(fallback);
  }

  private static com.google.common.base.Optional<String> getOption(
      Map<String, String> options, String name) {
    return getOption(options, name, DEFAULT_FALLBACK);
  }

  private static com.google.common.base.Optional<String> getOption(
      Map<String, String> options,
      String name,
      Supplier<com.google.common.base.Optional<String>> fallback) {
    return fromJavaUtil(
        firstPresent(Optional.ofNullable(options.get(name)), fallback.get().toJavaUtil()));
  }

  private static com.google.common.base.Optional<String> getOptionFromMultipleParams(
      Map<String, String> options,
      Collection<String> names,
      Supplier<com.google.common.base.Optional<String>> fallback) {
    return names.stream()
        .map(name -> getOption(options, name))
        .filter(com.google.common.base.Optional::isPresent)
        .findFirst()
        .orElseGet(fallback);
  }

  private static com.google.common.base.Optional<String> getAnyOption(
      ImmutableMap<String, String> globalOptions, Map<String, String> options, String name) {
    return com.google.common.base.Optional.fromNullable(options.get(name))
        .or(com.google.common.base.Optional.fromNullable(globalOptions.get(name)));
  }

  // gives the option to support old configurations as fallback
  // Used to provide backward compatibility
  private static com.google.common.base.Optional<String> getAnyOption(
      ImmutableMap<String, String> globalOptions,
      Map<String, String> options,
      Collection<String> names) {
    return names.stream()
        .map(name -> getAnyOption(globalOptions, options, name))
        .filter(optional -> optional.isPresent())
        .findFirst()
        .orElse(empty());
  }

  private static boolean getAnyBooleanOption(
      ImmutableMap<String, String> globalOptions,
      Map<String, String> options,
      String name,
      boolean defaultValue) {
    return getAnyOption(globalOptions, options, name).transform(Boolean::valueOf).or(defaultValue);
  }

  public static ImmutableMap<String, String> normalizeConf(Map<String, String> conf) {
    Map<String, String> normalizeConf =
        conf.entrySet().stream()
            .filter(e -> e.getKey().startsWith(CONF_PREFIX))
            .collect(
                Collectors.toMap(
                    e -> e.getKey().substring(CONF_PREFIX.length()), e -> e.getValue()));
    Map<String, String> result = new HashMap<>(conf);
    result.putAll(normalizeConf);
    return ImmutableMap.copyOf(result);
  }

  private static com.google.common.base.Optional empty() {
    return com.google.common.base.Optional.absent();
  }

  private static com.google.common.base.Optional fromJavaUtil(Optional o) {
    return com.google.common.base.Optional.fromJavaUtil(o);
  }

  public Credentials createCredentials() {

    return new BigQueryCredentialsSupplier(
            accessToken.toJavaUtil(),
            credentialsKey.toJavaUtil(),
            credentialsFile.toJavaUtil(),
            flinkBigQueryProxyAndHttpConfig.getProxyUri(),
            flinkBigQueryProxyAndHttpConfig.getProxyUsername(),
            flinkBigQueryProxyAndHttpConfig.getProxyPassword())
        .getCredentials();
  }

  public TableId getTableId() {
    return tableId;
  }

  public void setTableId(TableId tableId) {
    this.tableId = tableId;
  }

  public String getFlinkVersion() {
    return this.flinkVersion;
  }

  public String getSelectedFields() {
    String selectedFieldString = null;
    try {
      selectedFieldString =
          selectedFields != null ? selectedFields : new ParseSqlString(query).getSelectedFields();
    } catch (JSQLParserException e) {
      logger.error("Error while parsing sql query", e);
    }
    return selectedFieldString;
  }

  /** Returns the table id, without the added partition id if it exists. */
  public TableId getTableIdWithoutThePartition() {
    String tableAndPartition = tableId.getTable();
    if (!tableAndPartition.contains("$")) {
      // there is no partition id
      return tableId;
    }
    String table = tableAndPartition.substring(0, tableAndPartition.indexOf('$'));
    return tableId.getProject() != null
        ? TableId.of(tableId.getProject(), tableId.getDataset(), table)
        : TableId.of(tableId.getDataset(), table);
  }

  public Optional<String> getQuery() {
    return query.toJavaUtil();
  }

  @Override
  public String getParentProjectId() {
    return parentProjectId;
  }

  @Override
  public boolean useParentProjectForMetadataOperations() {
    return useParentProjectForMetadataOperations;
  }

  @Override
  public Optional<String> getCredentialsKey() {
    return credentialsKey.toJavaUtil();
  }

  @Override
  public Optional<String> getCredentialsFile() {
    return credentialsFile.toJavaUtil();
  }

  @Override
  public Optional<String> getAccessToken() {
    return accessToken.toJavaUtil();
  }

  public Optional<String> getFilter() {
    return filter.toJavaUtil();
  }

  public Optional<TableSchema> getSchema() {
    return schema.toJavaUtil();
  }

  public void setArrowSchemaFields(String fieldList) {
    this.fieldList = fieldList;
  }

  public String getArrowSchemaFields() {
    return this.fieldList;
  }

  public OptionalInt getMaxParallelism() {
    return maxParallelism == null ? OptionalInt.empty() : OptionalInt.of(maxParallelism);
  }

  public int getDefaultParallelism() {
    return defaultParallelism;
  }

  public Optional<String> getTemporaryGcsBucket() {
    return temporaryGcsBucket.toJavaUtil();
  }

  public Optional<String> getPersistentGcsBucket() {
    return persistentGcsBucket.toJavaUtil();
  }

  public Optional<String> getPersistentGcsPath() {
    return persistentGcsPath.toJavaUtil();
  }

  // public IntermediateFormat getIntermediateFormat() {
  // return intermediateFormat;
  // }

  public DataFormat getReadDataFormat() {
    return readDataFormat;
  }

  public CompressionCodec getArrowCompressionCodec() {
    return arrowCompressionCodec;
  }

  public boolean isCombinePushedDownFilters() {
    return combinePushedDownFilters;
  }

  public boolean isUseAvroLogicalTypes() {
    return useAvroLogicalTypes;
  }

  public boolean isViewsEnabled() {
    return viewsEnabled;
  }

  @Override
  public Optional<String> getMaterializationProject() {
    return materializationProject.toJavaUtil();
  }

  @Override
  public Optional<String> getMaterializationDataset() {
    return materializationDataset.toJavaUtil();
  }

  public Optional<String> getPartitionField() {
    return partitionField.toJavaUtil();
  }

  public OptionalLong getPartitionExpirationMs() {
    return partitionExpirationMs == null
        ? OptionalLong.empty()
        : OptionalLong.of(partitionExpirationMs);
  }

  public Optional<Boolean> getPartitionRequireFilter() {
    return partitionRequireFilter.toJavaUtil();
  }

  public Optional<TimePartitioning.Type> getPartitionType() {
    return partitionType.toJavaUtil();
  }

  public TimePartitioning.Type getPartitionTypeOrDefault() {
    return partitionType.or(TimePartitioning.Type.DAY);
  }

  public Optional<ImmutableList<String>> getClusteredFields() {
    return clusteredFields.transform(fields -> ImmutableList.copyOf(fields)).toJavaUtil();
  }

  public Optional<JobInfo.CreateDisposition> getCreateDisposition() {
    return createDisposition.toJavaUtil();
  }

  public boolean isOptimizedEmptyProjection() {
    return optimizedEmptyProjection;
  }

  public ImmutableList<JobInfo.SchemaUpdateOption> getLoadSchemaUpdateOptions() {
    return loadSchemaUpdateOptions;
  }

  public int getMaterializationExpirationTimeInMinutes() {
    return materializationExpirationTimeInMinutes;
  }

  public int getMaxReadRowsRetries() {
    return maxReadRowsRetries;
  }

  public boolean getPushAllFilters() {
    return pushAllFilters;
  }

  // in order to simplify the configuration, the BigQuery client settings are
  // fixed. If needed
  // we will add configuration properties for them.

  @Override
  public int getBigQueryClientConnectTimeout() {
    return flinkBigQueryProxyAndHttpConfig
        .getHttpConnectTimeout()
        .orElse(DEFAULT_BIGQUERY_CLIENT_CONNECT_TIMEOUT);
  }

  @Override
  public int getBigQueryClientReadTimeout() {
    return flinkBigQueryProxyAndHttpConfig
        .getHttpReadTimeout()
        .orElse(DEFAULT_BIGQUERY_CLIENT_READ_TIMEOUT);
  }

  @Override
  public BigQueryProxyConfig getBigQueryProxyConfig() {
    return flinkBigQueryProxyAndHttpConfig;
  }

  @Override
  public Optional<String> getEndpoint() {
    return storageReadEndpoint.toJavaUtil();
  }

  @Override
  public RetrySettings getBigQueryClientRetrySettings() {
    return RetrySettings.newBuilder()
        .setMaxAttempts(
            flinkBigQueryProxyAndHttpConfig
                .getHttpMaxRetry()
                .orElse(DEFAULT_BIGQUERY_CLIENT_RETRIES))
        .setTotalTimeout(Duration.ofMinutes(10))
        .setInitialRpcTimeout(Duration.ofSeconds(60))
        .setMaxRpcTimeout(Duration.ofMinutes(5))
        .setRpcTimeoutMultiplier(1.6)
        .setRetryDelayMultiplier(1.6)
        .setInitialRetryDelay(Duration.ofMillis(1250))
        .setMaxRetryDelay(Duration.ofSeconds(5))
        .build();
  }

  public RetrySettings getBigqueryDataWriteHelperRetrySettings() {
    return bigqueryDataWriteHelperRetrySettings;
  }

  public WriteMethod getWriteMethod() {
    return writeMethod;
  }

  public ReadSessionCreatorConfig toReadSessionCreatorConfig() {
    return new ReadSessionCreatorConfigBuilder()
        .setViewsEnabled(viewsEnabled)
        .setMaterializationProject(materializationProject.toJavaUtil())
        .setMaterializationDataset(materializationDataset.toJavaUtil())
        .setMaterializationExpirationTimeInMinutes(materializationExpirationTimeInMinutes)
        .setReadDataFormat(readDataFormat)
        .setMaxReadRowsRetries(maxReadRowsRetries)
        .setViewEnabledParamName(VIEWS_ENABLED_OPTION)
        .setDefaultParallelism(defaultParallelism)
        .setMaxParallelism(getMaxParallelism())
        .setRequestEncodedBase(encodedCreateReadSessionRequest.toJavaUtil())
        .setEndpoint(storageReadEndpoint.toJavaUtil())
        .setBackgroundParsingThreads(numBackgroundThreadsPerStream)
        .setPushAllFilters(pushAllFilters)
        .setPrebufferReadRowsResponses(numPrebufferReadRowsResponses)
        .setStreamsPerPartition(numStreamsPerPartition)
        .setArrowCompressionCodec(arrowCompressionCodec)
        .build();
  }

  public BigQueryClient.ReadTableOptions toReadTableOptions() {
    return new BigQueryClient.ReadTableOptions() {
      @Override
      public TableId tableId() {
        return FlinkBigQueryConfig.this.getTableId();
      }

      @Override
      public Optional<String> query() {
        return FlinkBigQueryConfig.this.getQuery();
      }

      @Override
      public boolean viewsEnabled() {
        return FlinkBigQueryConfig.this.isViewsEnabled();
      }

      @Override
      public String viewEnabledParamName() {
        return FlinkBigQueryConfig.VIEWS_ENABLED_OPTION;
      }

      @Override
      public int expirationTimeInMinutes() {
        return FlinkBigQueryConfig.this.getMaterializationExpirationTimeInMinutes();
      }
    };
  }

  class ParseSqlString {
    List<String> tableList = new ArrayList<String>();
    List<SelectItem> selectCols = new ArrayList<SelectItem>();

    ParseSqlString(com.google.common.base.Optional<String> query) throws JSQLParserException {
      String sql = query.get();
      Statement select = (Statement) CCJSqlParserUtil.parse(sql);
      TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
      this.tableList = tablesNamesFinder.getTableList(select);
      this.selectCols = ((PlainSelect) ((Select) select).getSelectBody()).getSelectItems();
    }

    private String getTable() {
      String tableName = tableList.get(0);
      tableName = tableName.replace("`", "");
      return tableName;
    }

    String getSelectedFields() {
      String selectedFields = "";
      for (SelectItem field : selectCols) {
        selectedFields =
            selectedFields
                + field.toString().substring(field.toString().lastIndexOf(" ") + 1)
                + ",";
      }
      if (selectedFields.endsWith(",")) {
        selectedFields = selectedFields.substring(0, selectedFields.length() - 1);
      }
      return selectedFields;
    }
  }
}
