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

import com.google.cloud.flink.bigquery.exception.FlinkBigQueryException;
import com.google.cloud.flink.bigquery.util.avro.AvroToRowDataConverters;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;

/** Reading the deserialized avro data and converting into flink RowData */
public class AvroRowDataDeserializationSchema
    implements DeserializationSchema<RowData>, Serializable {

  public static final long serialVersionUID = 1L;
  private final TypeInformation<RowData> typeInfo;
  private final DeserializationSchema<GenericRecord> nestedSchema;
  private final AvroToRowDataConverters.AvroToRowDataConverter runtimeConverter;
  final List<String> readSessionFieldNames = new ArrayList<String>();

  public AvroRowDataDeserializationSchema(
      RowType rowType,
      TypeInformation<RowData> typeInfo,
      List<String> selectedFieldList,
      List<String> avroFieldList,
      String readAvroSchema) {

    this.typeInfo = typeInfo;
    Parser avroSchemaParser = new Schema.Parser();
    Schema avroSchema = avroSchemaParser.parse(readAvroSchema);
    avroSchema.getFields().stream()
        .forEach(
            field -> {
              this.readSessionFieldNames.add(field.name());
            });
    this.nestedSchema = AvroDeserializationSchema.forGeneric(avroSchema);
    this.runtimeConverter =
        AvroToRowDataConverters.createRowConverter(
            rowType, readSessionFieldNames, selectedFieldList);
  }

  @Override
  public TypeInformation<RowData> getProducedType() {
    return typeInfo;
  }

  @Override
  public void open(InitializationContext context) throws Exception {
    this.nestedSchema.open(context);
  }

  @Override
  public void deserialize(@Nullable byte[] message, Collector<RowData> out) throws IOException {
    if (message == null) {
      throw new FlinkBigQueryException("Deserializing message is empty");
    }
    GenericRecord root = null;
    try {
      root = nestedSchema.deserialize(message);
      List<GenericRowData> rowdatalist = (List<GenericRowData>) runtimeConverter.convert(root);
      for (int i = 0; i < rowdatalist.size(); i++) {
        out.collect(rowdatalist.get(i));
      }

    } catch (Exception ex) {
      throw new FlinkBigQueryException("Error while deserializing avro type", ex);
    }
  }

  @Override
  public RowData deserialize(@Nullable byte[] message) throws IOException {
    if (message == null) {
      return null;
    }
    RowData rowData;
    try {
      GenericRecord root = nestedSchema.deserialize(message);
      rowData = (RowData) runtimeConverter.convert(root);
    } catch (Exception ex) {
      throw new FlinkBigQueryException("Error while deserializing avro type", ex);
    }
    return rowData;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AvroRowDataDeserializationSchema that = (AvroRowDataDeserializationSchema) o;
    return nestedSchema.equals(that.nestedSchema) && typeInfo.equals(that.typeInfo);
  }

  @Override
  public int hashCode() {
    return Objects.hash(nestedSchema, typeInfo);
  }

  @Override
  public boolean isEndOfStream(RowData nextElement) {
    return nextElement == null;
  }
}
