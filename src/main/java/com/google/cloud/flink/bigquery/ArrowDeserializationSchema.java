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

public class ArrowDeserializationSchema<T> implements DeserializationSchema<T>, Serializable {

  private static final long serialVersionUID = 1L;

  private BufferAllocator allocator;
  private final TypeInformation<RowData> typeInfo;
  ArrowRecordBatch deserializedBatch;

  public static ArrowDeserializationSchema<VectorSchemaRoot> forGeneric(
      String schemaJsonString, TypeInformation<RowData> typeInfo) {
    return new ArrowDeserializationSchema<>(VectorSchemaRoot.class, schemaJsonString, typeInfo);
  }

  private VectorSchemaRoot root;
  private VectorLoader loader;
  List<FieldVector> vectors = new ArrayList<>();
  private Schema schema;
  private final Class<T> recordClazz;

  private String schemaJsonString;

  ArrowDeserializationSchema(
      Class<T> recordClazz, String schemaJsonString, TypeInformation<RowData> typeInfo) {
    Preconditions.checkNotNull(recordClazz, "Arrow record class must not be null.");
    this.typeInfo = typeInfo;
    this.recordClazz = recordClazz;
    this.schemaJsonString = schemaJsonString;
  }

  @SuppressWarnings("unchecked")
  @Override
  public T deserialize(byte[] message) throws IOException {
    this.schema = Schema.fromJSON(schemaJsonString);
    checkArrowInitialized();
    deserializedBatch =
        MessageSerializer.deserializeRecordBatch(
            new ReadChannel(new ByteArrayReadableSeekableByteChannel(message)), allocator);
    loader.load(deserializedBatch);
    deserializedBatch.close();
    return (T) root;
  }

  void checkArrowInitialized() {
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
    return nextElement == null ? Boolean.TRUE : Boolean.FALSE;
  }

  @Override
  @SuppressWarnings({"unchecked"})
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
