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

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.gax.rpc.UnaryCallable;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.bigquery.connector.common.BigQueryClientFactory;
import com.google.cloud.bigquery.connector.common.ReadSessionCreator;
import com.google.cloud.bigquery.connector.common.ReadSessionCreatorConfig;
import com.google.cloud.bigquery.connector.common.ReadSessionCreatorConfigBuilder;
import com.google.cloud.bigquery.storage.v1.ArrowSchema;
import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import com.google.cloud.bigquery.storage.v1.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.cloud.bigquery.storage.v1.ReadSession.TableReadOptions;
import com.google.cloud.bigquery.storage.v1.stub.EnhancedBigQueryReadStub;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class ArrowDeserializationSchemaTest {

  @Test
  public void deserializeTest() throws IOException {
    String jsonStringArrowSchema = "Schema<word: Utf8, word_count: Int(64, true)>";
    ArrowDeserializationSchema<VectorSchemaRoot> arrowDeserializationSchema =
        new ArrowDeserializationSchema<VectorSchemaRoot>(
            VectorSchemaRoot.class, jsonStringArrowSchema, null);
    assertThat(arrowDeserializationSchema).isNotNull();
  }

  public ReadSession getReadSession() {
    EnhancedBigQueryReadStub stub = mock(EnhancedBigQueryReadStub.class);
    BigQueryClient bigQueryClient = mock(BigQueryClient.class);
    UnaryCallable<CreateReadSessionRequest, ReadSession> createReadSessionCall =
        mock(UnaryCallable.class);
    BigQueryReadClient readClient = BigQueryReadClient.create(stub);
    BigQueryClientFactory bigQueryReadClientFactory = mock(BigQueryClientFactory.class);
    TableInfo table =
        TableInfo.newBuilder(
                TableId.of("a", "b"),
                StandardTableDefinition.newBuilder()
                    .setSchema(
                        com.google.cloud.bigquery.Schema.of(
                            Field.of("name", StandardSQLTypeName.BOOL)))
                    .setNumBytes(1L)
                    .build())
            .build();
    TableReadOptions tableReadOptions = TableReadOptions.newBuilder().build();
    ReadSession readSession =
        ReadSession.newBuilder().setName("abc").setReadOptions(tableReadOptions).build();
    CreateReadSessionRequest request =
        CreateReadSessionRequest.newBuilder().setReadSession(readSession).build();
    Optional<String> encodedBase =
        Optional.of(java.util.Base64.getEncoder().encodeToString(request.toByteArray()));
    ReadSessionCreatorConfig config =
        new ReadSessionCreatorConfigBuilder().setRequestEncodedBase(encodedBase).build();
    ReadSessionCreator creator =
        new ReadSessionCreator(config, bigQueryClient, bigQueryReadClientFactory);
    when(bigQueryReadClientFactory.getBigQueryReadClient()).thenReturn(readClient);
    when(bigQueryClient.getTable(any())).thenReturn(table);
    when(stub.createReadSessionCallable()).thenReturn(createReadSessionCall);
    creator
        .create(TableId.of("dataset", "table"), ImmutableList.of("col1", "col2"), Optional.empty())
        .getReadSession();
    ArgumentCaptor<CreateReadSessionRequest> requestCaptor =
        ArgumentCaptor.forClass(CreateReadSessionRequest.class);
    verify(createReadSessionCall, times(1)).call(requestCaptor.capture());
    return requestCaptor.getValue().getReadSession();
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
  private MockDynamicTableContext createContextObject() {
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

    Map<String, String> resolvedSchema = new HashMap<String, String>();
    resolvedSchema.put("word", DataTypes.STRING().toString());
    resolvedSchema.put("word_count", DataTypes.BIGINT().toString());

    CatalogTableImpl resolvedCatalogTable =
        new CatalogTableImpl(catalogTable.getSchema(), resolvedSchema, "comments for table");

    Configuration configuration = new Configuration();

    ClassLoader classloader = Thread.currentThread().getContextClassLoader();
    MockDynamicTableContext contextObj =
        new MockDynamicTableContext(
            tableIdentifier,
            resolvedCatalogTable,
            Collections.emptyMap(),
            configuration,
            classloader,
            false);
    return contextObj;
  }
}
