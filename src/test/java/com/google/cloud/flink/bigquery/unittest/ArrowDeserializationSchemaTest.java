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
package com.google.cloud.flink.bigquery.unittest;

import static org.junit.Assert.assertEquals;

import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigquery.storage.v1.ArrowSchema;
import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import com.google.cloud.bigquery.storage.v1.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1.DataFormat;
import com.google.cloud.bigquery.storage.v1.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.cloud.bigquery.storage.v1.ReadSession.TableReadOptions;
import com.google.cloud.flink.bigquery.ArrowDeserializationSchema;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.*;
import org.apache.flink.table.catalog.*;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.junit.Test;

public class ArrowDeserializationSchemaTest {
  @SuppressWarnings("unchecked")
  @Test
  public void forGeneric() throws IOException {
    ReadSession session = getReadSession();
    Schema schema = getSchema(session);
    TypeInformation<RowData> rowDataTypeInfo = null;
    byte[] message = null;
    ArrowDeserializationSchema<VectorSchemaRoot> temp =
        ArrowDeserializationSchema.forGeneric(schema.toJson(), rowDataTypeInfo);

    String streamName = session.getStreams(0).getName();
    BigQueryReadClient client = BigQueryReadClient.create();
    ReadRowsRequest readRowsRequest =
        ReadRowsRequest.newBuilder().setReadStream(streamName).build();
    ServerStream<ReadRowsResponse> stream = client.readRowsCallable().call(readRowsRequest);
    for (ReadRowsResponse response : stream) {
      message = response.getArrowRecordBatch().getSerializedRecordBatch().toByteArray();
    }
    assertEquals(temp.deserialize(message).getRowCount(), 672);
  }

  public ReadSession getReadSession() throws IOException {
    String projectId = "q-gcp-6750-pso-gs-flink-22-01";
    String table = "wordcount_dataset";
    String dataset = "wordcount_output";

    try (BigQueryReadClient client = BigQueryReadClient.create()) {
      String parent = String.format("projects/%s", projectId);
      String srcTable =
          String.format("projects/%s/datasets/%s/tables/%s", projectId, table, dataset);
      TableReadOptions options =
          TableReadOptions.newBuilder()
              .addSelectedFields("word")
              .addSelectedFields("word_count")
              .clearArrowSerializationOptions()
              .build();

      ReadSession.Builder sessionBuilder =
          ReadSession.newBuilder()
              .setTable(srcTable)
              .setDataFormat(DataFormat.ARROW)
              .setReadOptions(options);
      CreateReadSessionRequest.Builder builder =
          CreateReadSessionRequest.newBuilder()
              .setParent(parent)
              .setReadSession(sessionBuilder)
              .setMaxStreamCount(1);
      ReadSession session = client.createReadSession(builder.build());
      return session;
    }
  }

  public Schema getSchema(ReadSession session) throws IOException {
    ArrowSchema arrowSchema = session.getArrowSchema();
    Schema schema =
        MessageSerializer.deserializeSchema(
            new ReadChannel(
                new ByteArrayReadableSeekableByteChannel(
                    arrowSchema.getSerializedSchema().toByteArray())));
    return schema;
  }

  @SuppressWarnings("unused")
  private DefaultDynamicTableContext createContextObject() {
    ObjectIdentifier tableIdentifier = ObjectIdentifier.of("csvcatalog", "default", "csvtable");
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
    CatalogTable catalogTable =
        (CatalogTable)
            new CatalogTableImpl(
                tableSchema, partitionColumnList, Collections.emptyMap(), "sample table creation");

    Map<String, String> resolvedSchema = new HashMap();
    resolvedSchema.put("word", DataTypes.STRING().toString());
    resolvedSchema.put("word_count", DataTypes.BIGINT().toString());

    CatalogTableImpl resolvedCatalogTable =
        new CatalogTableImpl(catalogTable.getSchema(), resolvedSchema, "comments for table");

    Configuration configuration = new Configuration();

    ClassLoader classloader = Thread.currentThread().getContextClassLoader();
    DefaultDynamicTableContext contextObj =
        new DefaultDynamicTableContext(
            tableIdentifier,
            resolvedCatalogTable,
            Collections.emptyMap(),
            configuration,
            classloader,
            false);
    return contextObj;
  }
}
