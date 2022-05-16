/*
 * Copyright 2018 Google Inc. All Rights Reserved.
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
package com.google.cloud.flink.bigquery.integration;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Field.Mode;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.ViewDefinition;
import com.google.cloud.bigquery.connector.common.BigQueryClient;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IntegrationTestUtils {

  static Logger logger = LoggerFactory.getLogger(IntegrationTestUtils.class);

  public static BigQuery getBigquery() {
    return BigQueryOptions.getDefaultInstance().getService();
  }

  public static void createDataset(String dataset) {
    BigQuery bq = getBigquery();
    DatasetId datasetId = DatasetId.of(dataset);
    logger.warn("Creating test dataset: {}", datasetId);
    bq.create(DatasetInfo.of(datasetId));
  }

  public static void runQuery(String query) {
    BigQueryClient bigQueryClient =
        new BigQueryClient(getBigquery(), Optional.empty(), Optional.empty());
    bigQueryClient.query(query);
  }

  public static void deleteDatasetAndTables(String dataset) {
    BigQuery bq = getBigquery();
    logger.warn("Deleting test dataset '{}' and its contents", dataset);
    bq.delete(DatasetId.of(dataset), BigQuery.DatasetDeleteOption.deleteContents());
  }

  static void createView(String dataset, String table, String view) {
    BigQuery bq = getBigquery();
    String query = String.format("SELECT * FROM %s.%s", dataset, table);
    TableId tableId = TableId.of(dataset, view);
    ViewDefinition viewDefinition = ViewDefinition.newBuilder(query).setUseLegacySql(false).build();
    bq.create(TableInfo.of(tableId, viewDefinition));
  }

  public static void createTable(String dataset, String table, String function)
      throws UnsupportedEncodingException, InterruptedException {
    BigQuery bq = getBigquery();
    ArrayList<Field> listOfFileds = new ArrayList<Field>();
    ArrayList<Field> listOfSubFileds = new ArrayList<Field>();
    listOfSubFileds.add(
        Field.newBuilder("record1", StandardSQLTypeName.STRING).setMode(Mode.NULLABLE).build());
    listOfSubFileds.add(
        Field.newBuilder("record2", StandardSQLTypeName.NUMERIC).setMode(Mode.NULLABLE).build());
    listOfSubFileds.add(
        Field.newBuilder("record3", StandardSQLTypeName.BOOL).setMode(Mode.NULLABLE).build());

    listOfFileds.add(
        Field.newBuilder("numeric_datatype", StandardSQLTypeName.NUMERIC)
            .setMode(Mode.NULLABLE)
            .build());
    listOfFileds.add(
        Field.newBuilder("string_datatype", StandardSQLTypeName.STRING)
            .setMode(Mode.NULLABLE)
            .build());
    listOfFileds.add(
        Field.newBuilder("bytes_datatype", StandardSQLTypeName.BYTES)
            .setMode(Mode.NULLABLE)
            .build());
    listOfFileds.add(
        Field.newBuilder("integer_datatype", StandardSQLTypeName.INT64)
            .setMode(Mode.NULLABLE)
            .build());
    listOfFileds.add(
        Field.newBuilder("float_datatype", StandardSQLTypeName.FLOAT64)
            .setMode(Mode.NULLABLE)
            .build());
    listOfFileds.add(
        Field.newBuilder("boolean_datatype", StandardSQLTypeName.BOOL)
            .setMode(Mode.NULLABLE)
            .build());
    listOfFileds.add(
        Field.newBuilder("timestamp_datatype", StandardSQLTypeName.TIMESTAMP)
            .setMode(Mode.NULLABLE)
            .build());
    listOfFileds.add(
        Field.newBuilder("date_datatype", StandardSQLTypeName.DATE).setMode(Mode.NULLABLE).build());
    listOfFileds.add(
        Field.newBuilder("datetime_datatype", StandardSQLTypeName.DATETIME)
            .setMode(Mode.NULLABLE)
            .build());
    listOfFileds.add(
        Field.newBuilder("time_datatype", StandardSQLTypeName.TIME).setMode(Mode.NULLABLE).build());
    listOfFileds.add(
        Field.newBuilder("geography_datatype", StandardSQLTypeName.STRING)
            .setMode(Mode.NULLABLE)
            .build());
    listOfFileds.add(
        Field.newBuilder(
                "record_datatype", StandardSQLTypeName.STRUCT, FieldList.of(listOfSubFileds))
            .setMode(Mode.NULLABLE)
            .build());
    listOfFileds.add(
        Field.newBuilder("array_datatype", StandardSQLTypeName.STRING)
            .setMode(Mode.REPEATED)
            .build());
    FieldList fieldlist = FieldList.of(listOfFileds);
    Schema schema = Schema.of(fieldlist);

    TableId tableId = TableId.of(dataset, table);
    StandardTableDefinition tableDefinition;
    tableDefinition = StandardTableDefinition.of(schema);
    TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();
    bq.create(tableInfo);

    String base64encodedString = Base64.getEncoder().encodeToString("byte-test".getBytes("utf-8"));

    if (function.equals("read")) {
      Map<String, Object> subRowContent = new HashMap<>();
      subRowContent.put("record1", "stringTest1");
      subRowContent.put("record2", 1);
      subRowContent.put("record3", true);

      Map<String, Object> rowContent = new HashMap<>();
      rowContent.put("numeric_datatype", 123.345);
      rowContent.put("string_datatype", "flink");
      rowContent.put("bytes_datatype", base64encodedString);
      rowContent.put("integer_datatype", 12345);
      rowContent.put("float_datatype", 50.05f);
      rowContent.put("boolean_datatype", true);
      rowContent.put("timestamp_datatype", "2022-03-17 17:11:53 UTC");
      rowContent.put("date_datatype", "2022-01-01");
      rowContent.put("datetime_datatype", "2022-03-17T13:20:23.439071");
      rowContent.put("time_datatype", "13:20:23.439071");
      rowContent.put("geography_datatype", "POINT(51.500989020415 -0.124710813123368)");
      rowContent.put("record_datatype", subRowContent);
      rowContent.put("array_datatype", new String[] {"string1", "string2", "string3"});
      InsertAllResponse response =
          bq.insertAll(InsertAllRequest.newBuilder(tableId).addRow(rowContent).build());
      Thread.sleep(20000);
      if (response.hasErrors()) {
        logger.error(response.getInsertErrors().toString());
      }
    }
  }
}
