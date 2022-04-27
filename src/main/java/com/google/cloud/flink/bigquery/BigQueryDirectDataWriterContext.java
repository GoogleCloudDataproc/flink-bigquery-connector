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

import static com.google.cloud.flink.bigquery.ProtobufUtils.buildSingleRowMessage;
import static com.google.cloud.flink.bigquery.ProtobufUtils.getListOfSubFields;
import static com.google.cloud.flink.bigquery.ProtobufUtils.toDescriptor;

import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Field.Mode;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TimePartitioning;
import com.google.cloud.bigquery.connector.common.BigQueryClientFactory;
import com.google.cloud.bigquery.connector.common.BigQueryConfig;
import com.google.cloud.bigquery.connector.common.BigQueryConnectorException;
import com.google.cloud.bigquery.connector.common.BigQueryCredentialsSupplier;
import com.google.cloud.bigquery.storage.v1beta2.ProtoSchema;
import com.google.cloud.flink.bigquery.common.BigQueryDirectDataWriteHelper;
import com.google.cloud.flink.bigquery.common.BigQueryDirectWriterCommitMessageContext;
import com.google.cloud.flink.bigquery.common.DataWriterContext;
import com.google.cloud.flink.bigquery.common.FlinkBigQueryConnectorUserAgentProvider;
import com.google.cloud.flink.bigquery.common.UserAgentHeaderProvider;
import com.google.cloud.flink.bigquery.common.WriterCommitMessageContext;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import net.sf.jsqlparser.JSQLParserException;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BigQueryDirectDataWriterContext implements DataWriterContext<Row> {
  final Logger logger = LoggerFactory.getLogger(BigQueryDirectDataWriterContext.class);

  final String tablePath;
  final RowType flinkSchema;
  final Descriptors.Descriptor schemaDescriptor;
  static BigQuery bigquery;
  private BigQueryDirectDataWriteHelper writerHelper;
  Table srcTable;
  String projectId;
  String dataset;
  String table;
  StandardSQLTypeName type;
  FlinkBigQueryConfig bqconfig;

  public BigQueryDirectDataWriterContext(
      Table src_table, String projectId, String dataset, String table) throws JSQLParserException {
    List<DataType> columnDataTypeList = Arrays.asList(src_table.getSchema().getFieldDataTypes());
    List<String> columnNameList = Arrays.asList(src_table.getSchema().getFieldNames());
    ArrayList<RowField> listOfRowFields = new ArrayList<RowField>();
    for (int i = 0; i < columnNameList.size(); i++) {
      listOfRowFields.add(
          new RowField(
              columnNameList.get(i).toString(), columnDataTypeList.get(i).getLogicalType()));
    }
    this.projectId = projectId;
    this.dataset = dataset;
    this.table = table;
    flinkSchema = new RowType(listOfRowFields);
    this.tablePath = String.format("projects/%s/datasets/%s/tables/%s", projectId, dataset, table);
    this.srcTable = srcTable;
    try {
      this.schemaDescriptor = toDescriptor(flinkSchema);
    } catch (Descriptors.DescriptorValidationException e) {
      throw new BigQueryConnectorException.InvalidSchemaException(
          "Could not convert flink-schema to descriptor object", e);
    }
  }

  public void setBigQueryConfig(FlinkBigQueryConfig config) {
    this.bqconfig = config;
  }

  void checkBigQueryInitialized() throws JSQLParserException {
    if (bigquery == null) {
      bigquery =
          BigQueryOptions.newBuilder()
              .setCredentials(bqconfig.createCredentials())
              .build()
              .getService();
    }
    com.google.cloud.bigquery.Table dest_table =
        bigquery.getTable(TableId.of(projectId, dataset, table));
    if (dest_table == null || !dest_table.exists()) {
      ArrayList<Field> listOfFileds = new ArrayList<Field>();
      Iterator<RowField> rowFieldItrator = flinkSchema.getFields().iterator();
      while (rowFieldItrator.hasNext()) {
        RowField elem = rowFieldItrator.next();
        if ("ROW".equals(elem.getType().getTypeRoot().toString())) {
          listOfFileds.add(
              Field.newBuilder(
                      elem.getName(),
                      StandardSQLTypeName.STRUCT,
                      FieldList.of(getListOfSubFields(elem.getType())))
                  .setMode(elem.getType().isNullable() ? Mode.NULLABLE : Mode.REQUIRED)
                  .build());
        } else if ("ARRAY".equals(elem.getType().getTypeRoot().toString())) {
          listOfFileds.add(
              Field.newBuilder(
                      elem.getName(),
                      StandardSQLTypeHandler.handle(((ArrayType) elem.getType()).getElementType()))
                  .setMode(Mode.REPEATED)
                  .build());
        } else {
          listOfFileds.add(
              Field.newBuilder(elem.getName(), StandardSQLTypeHandler.handle(elem.getType()))
                  .setMode(elem.getType().isNullable() ? Mode.NULLABLE : Mode.REQUIRED)
                  .build());
        }
      }
      FieldList fieldlist = FieldList.of(listOfFileds);
      Schema schema = Schema.of(fieldlist);
      createTable(projectId, dataset, table, schema);
    }
  }

  private void createTable(String project_id, String datasetName, String tableName, Schema schema) {
    try {
      TableId tableId = TableId.of(project_id, datasetName, tableName);
      StandardTableDefinition tableDefinition;
      tableDefinition = StandardTableDefinition.of(schema);
      if (bqconfig.getPartitionField().isPresent() || bqconfig.getPartitionType().isPresent()) {
        TimePartitioning.Builder timePartitionBuilder =
            TimePartitioning.newBuilder(bqconfig.getPartitionTypeOrDefault());
        bqconfig.getPartitionExpirationMs().ifPresent(timePartitionBuilder::setExpirationMs);
        bqconfig
            .getPartitionRequireFilter()
            .ifPresent(timePartitionBuilder::setRequirePartitionFilter);
        bqconfig.getPartitionField().ifPresent(timePartitionBuilder::setField);
        TimePartitioning partitioning = timePartitionBuilder.build();
        tableDefinition =
            StandardTableDefinition.newBuilder()
                .setSchema(schema)
                .setTimePartitioning(partitioning)
                .build();
      }
      TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();
      bigquery.create(tableInfo);
      logger.info("Table " + tableId.getTable() + " created successfully");
    } catch (BigQueryException e) {
      throw new FlinkBigQueryException("Table was not created. \n" + e.toString());
    }
  }

  @Override
  public void write(Row record) throws IOException {
    ByteString message =
        buildSingleRowMessage(flinkSchema, schemaDescriptor, record).toByteString();
    if (writerHelper == null) initializeWriterHelper();
    writerHelper.addRow(message);
  }

  private void initializeWriterHelper() {

    if (this.bqconfig == null) setBigQueryConfig(BigQueryDynamicTableFactory.getBqConfig());

    BigQueryCredentialsSupplier bigQueryCredentialsSupplier =
        new BigQueryCredentialsSupplier(
            bqconfig.getAccessToken(),
            bqconfig.getCredentialsKey(),
            bqconfig.getCredentialsFile(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty());

    FlinkBigQueryConnectorUserAgentProvider agentProvider =
        new FlinkBigQueryConnectorUserAgentProvider(bqconfig.getFlinkVersion());
    final UserAgentHeaderProvider userAgentHeaderProvider =
        new UserAgentHeaderProvider(agentProvider.getUserAgent());

    BigQueryClientFactory writeClientFactory =
        new BigQueryClientFactory(
            bigQueryCredentialsSupplier, userAgentHeaderProvider, (BigQueryConfig) bqconfig);
    ProtoSchema protoSchema = ProtobufUtils.toProtoSchema(flinkSchema);
    RetrySettings.Builder retrySettingsBuilder = RetrySettings.newBuilder();
    retrySettingsBuilder.setMaxAttempts(3);
    RetrySettings bigqueryDataWriterHelperRetrySettings = retrySettingsBuilder.build();
    try {
      checkBigQueryInitialized();
    } catch (JSQLParserException e) {
      throw new FlinkBigQueryException("Error while initializing big query", e);
    }

    this.writerHelper =
        new BigQueryDirectDataWriteHelper(
            writeClientFactory, tablePath, protoSchema, bigqueryDataWriterHelperRetrySettings);
  }

  public String getWriterStream() {

    return this.writerHelper.getWriteStreamName();
  }

  @Override
  public WriterCommitMessageContext finalizeStream() throws IOException {

    logger.debug("Data Writer commit()");

    long rowCount = writerHelper.finalizeStream();
    String writeStreamName = writerHelper.getWriteStreamName();

    logger.debug("Data Writer write-stream has finalized with row count: {}", rowCount);

    return new BigQueryDirectWriterCommitMessageContext(writeStreamName, tablePath, rowCount);
  }

  @Override
  public void commit() throws IOException {
    writerHelper.commitStream();
  }

  @Override
  public void abort() throws IOException {
    logger.debug("Data Writer {} abort()");
    writerHelper.abort();
  }
}
