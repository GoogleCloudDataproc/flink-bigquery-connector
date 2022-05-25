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
import static com.google.cloud.flink.bigquery.ProtobufUtils.toDescriptor;

import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.connector.common.BigQueryClientFactory;
import com.google.cloud.bigquery.connector.common.BigQueryConnectorException;
import com.google.cloud.bigquery.storage.v1beta2.ProtoSchema;
import com.google.cloud.flink.bigquery.common.BigQueryDirectDataWriteHelper;
import com.google.cloud.flink.bigquery.common.BigQueryDirectWriterCommitMessageContext;
import com.google.cloud.flink.bigquery.common.DataWriterContext;
import com.google.cloud.flink.bigquery.common.WriterCommitMessageContext;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import net.sf.jsqlparser.JSQLParserException;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BigQueryDirectDataWriterContext implements DataWriterContext<Row>, Serializable {

  private static final long serialVersionUID = 1L;

  final Logger logger = LoggerFactory.getLogger(BigQueryDirectDataWriterContext.class);

  private String tablePath;
  private RowType flinkSchema;
  private Descriptors.Descriptor schemaDescriptor;
  private BigQueryDirectDataWriteHelper writerHelper;
  private TableId tableId;
  private BigQueryClientFactory bigQueryWriteClientFactory;
  private String projectId;
  private String dataset;
  private String table;

  public BigQueryDirectDataWriterContext(
      String[] fieldNames,
      DataType[] fieldDataTypes,
      FlinkBigQueryConfig bqConfig,
      BigQueryClientFactory bigQueryWriteClientFactory)
      throws JSQLParserException {
    this.tableId = bqConfig.getTableId();
    this.bigQueryWriteClientFactory = bigQueryWriteClientFactory;
    List<DataType> columnDataTypeList = Arrays.asList(fieldDataTypes);
    List<String> columnNameList = Arrays.asList(fieldNames);
    ArrayList<RowField> listOfRowFields = new ArrayList<RowField>();
    for (int i = 0; i < columnNameList.size(); i++) {
      listOfRowFields.add(
          new RowField(
              columnNameList.get(i).toString(), columnDataTypeList.get(i).getLogicalType()));
    }
    this.projectId = tableId.getProject();
    this.dataset = tableId.getDataset();
    this.table = tableId.getTable();
    this.flinkSchema = new RowType(listOfRowFields);
    this.tablePath = String.format("projects/%s/datasets/%s/tables/%s", projectId, dataset, table);

    try {
      this.schemaDescriptor = toDescriptor(flinkSchema);
    } catch (Descriptors.DescriptorValidationException e) {
      throw new BigQueryConnectorException.InvalidSchemaException(
          "Could not convert flink-schema to descriptor object", e);
    }
    initializeWriterHelper();
  }

  private void initializeWriterHelper() {
    ProtoSchema protoSchema = ProtobufUtils.toProtoSchema(flinkSchema);
    RetrySettings.Builder retrySettingsBuilder = RetrySettings.newBuilder();
    retrySettingsBuilder.setMaxAttempts(3);
    RetrySettings bigqueryDataWriterHelperRetrySettings = retrySettingsBuilder.build();
    this.writerHelper =
        new BigQueryDirectDataWriteHelper(
            bigQueryWriteClientFactory,
            tablePath,
            protoSchema,
            bigqueryDataWriterHelperRetrySettings);
  }

  @Override
  public void write(Row record) throws IOException {
    ByteString message =
        buildSingleRowMessage(flinkSchema, schemaDescriptor, record).toByteString();
    this.writerHelper.addRow(message);
  }

  public String getWriterStream() {

    return writerHelper.getWriteStreamName();
  }

  @Override
  public WriterCommitMessageContext commit() throws IOException {

    logger.debug("Data Writer commit()");

    long rowCount = writerHelper.commit();
    String writeStreamName = writerHelper.getWriteStreamName();
    logger.debug("Data Writer write-stream has finalized with row count: {}", rowCount);

    return new BigQueryDirectWriterCommitMessageContext(writeStreamName, tablePath, rowCount);
  }

  @Override
  public void commitFinalizedStream() throws IOException {
    this.writerHelper.commitStream();
  }

  @Override
  public void abort() throws IOException {
    logger.debug("Data Writer {} abort()");
    this.writerHelper.abort();
  }
}
