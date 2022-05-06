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

import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ArrowDeserializationSchema<T> implements DeserializationSchema<T>, Serializable {

  private static final long serialVersionUID = 1L;
  final Logger logger = LoggerFactory.getLogger(ArrowDeserializationSchema.class);

  private BufferAllocator allocator;
  private final TypeInformation<RowData> typeInfo;
  ArrowRecordBatch deserializedBatch;
  private VectorSchemaRoot root;
  private VectorLoader loader;
  List<FieldVector> vectors = new ArrayList<>();
  private Schema schema;
  private final Class<T> recordClazz;
  private Schema readSessionSchema;

  ArrowDeserializationSchema(
      Class<T> recordClazz, String schemaJsonString, TypeInformation<RowData> typeInfo) {
    Preconditions.checkNotNull(recordClazz, "Arrow record class must not be null.");
    this.typeInfo = typeInfo;
    this.recordClazz = recordClazz;
  }

  public static ArrowDeserializationSchema<VectorSchemaRoot> forGeneric(
      String schemaJsonString, TypeInformation<RowData> typeInfo) {
    return new ArrowDeserializationSchema<>(VectorSchemaRoot.class, schemaJsonString, typeInfo);
  }

  @Override
  public T deserialize(byte[] responseByteMessage) throws IOException {
    ReadRowsResponse response = ReadRowsResponse.parseFrom(responseByteMessage);
    byte[] arrowRecordBatchMessage =
        response.getArrowRecordBatch().getSerializedRecordBatch().toByteArray();

    if (arrowRecordBatchMessage == null) {
      throw new FlinkBigQueryException("Deserializing message is empty");
    }
    if (this.schema == null) {
      try {
        this.readSessionSchema =
            MessageSerializer.deserializeSchema(
                new ReadChannel(
                    new ByteArrayReadableSeekableByteChannel(
                        response.getArrowSchema().getSerializedSchema().toByteArray())));
      } catch (Exception e) {
        logger.error("Error while deserializing schema:", e);
        throw new FlinkBigQueryException("Deserialization mush have schema:", e);
      }
      String jsonArrowSchemafromReadSession = this.readSessionSchema.toJson();
      this.schema = Schema.fromJSON(jsonArrowSchemafromReadSession);
    }
    initializeArrow();
    deserializedBatch =
        MessageSerializer.deserializeRecordBatch(
            new ReadChannel(new ByteArrayReadableSeekableByteChannel(arrowRecordBatchMessage)),
            allocator);
    loader.load(deserializedBatch);
    deserializedBatch.close();
    return (T) root;
  }

  private void initializeArrow() {
    Preconditions.checkNotNull(schema);
    if (root != null) {
      return;
    }
    if (allocator == null) {
      this.allocator = new RootAllocator(Long.MAX_VALUE);
    }
    for (Field field : schema.getFields()) {
      vectors.add(field.createVector(allocator));
    }
    root = new VectorSchemaRoot(vectors);
    this.loader = new VectorLoader(root);
  }

  @Override
  public boolean isEndOfStream(T nextElement) {
    return nextElement == null;
  }

  @Override
  public TypeInformation<T> getProducedType() {
    return (TypeInformation<T>) typeInfo;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ArrowDeserializationSchema<?> that = (ArrowDeserializationSchema<?>) o;
    return recordClazz.equals(that.recordClazz) && Objects.equals(schema, that.schema);
  }

  @Override
  public int hashCode() {
    return Objects.hash(recordClazz, schema);
  }
}
