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

import static com.google.cloud.flink.bigquery.util.ProtobufUtils.toDescriptor;
import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.bigquery.storage.v1.ProtoSchema;
import com.google.cloud.flink.bigquery.util.ProtobufUtils;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.flink.fnexecution.v1.FlinkFnApi.Schema.FieldType;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.Test;

public class ProtobufUtilsTest {

  @Test
  public void toDescriptorRowTypeTest() throws IOException, DescriptorValidationException {
    List<RowField> fields = new ArrayList<RowField>();
    fields.add(new RowField("id", DataTypes.BIGINT().getLogicalType()));
    fields.add(new RowField("name", DataTypes.STRING().getLogicalType()));
    RowType rowType = new RowType(fields);
    Descriptor descriptor = ProtobufUtils.toDescriptor(rowType);
    assertThat(descriptor).isNotNull();
    assertThat(descriptor.getFields().get(0).getName()).isEqualTo("id");
    assertThat(descriptor.getFields().get(1).getName()).isEqualTo("name");
  }

  @Test
  public void toProtoSchemaRowTypeTest() throws IOException, DescriptorValidationException {
    List<RowField> fields = new ArrayList<RowField>();
    fields.add(new RowField("id", DataTypes.BIGINT().getLogicalType()));
    fields.add(new RowField("name", DataTypes.STRING().getLogicalType()));
    RowType rowType = new RowType(fields);
    ProtoSchema protoSchema = ProtobufUtils.toProtoSchema(rowType);
    Descriptor descriptor = ProtobufUtils.toDescriptor(rowType);
    assertThat(protoSchema).isNotNull();
    assertThat(descriptor).isNotNull();
    assertThat(descriptor.getFields().get(0).getName()).isEqualTo("id");
    assertThat(descriptor.getFields().get(1).getName()).isEqualTo("name");
  }

  @Test
  public void buildSingleRowMessageRowTypeTest() throws IOException, DescriptorValidationException {
    List<RowField> fields = new ArrayList<RowField>();
    fields.add(new RowField("id", DataTypes.BIGINT().getLogicalType()));
    fields.add(new RowField("name", DataTypes.STRING().getLogicalType()));
    RowType rowType = new RowType(fields);
    Row row = new Row(3);
    row.setKind(RowKind.INSERT);
    row.setField(0, 100);
    row.setField(1, "John");
    Descriptor descriptor = toDescriptor(rowType);
    DynamicMessage dynamicMessage = ProtobufUtils.buildSingleRowMessage(rowType, descriptor, row);
    Set<FieldDescriptor> keySet = dynamicMessage.getAllFields().keySet();
    Object[] keySetArray = keySet.toArray();
    assertThat(rowType).isNotNull();
    assertThat(descriptor).isNotNull();
    assertThat(dynamicMessage).isNotNull();
    assertThat(keySet).isNotNull();
    assertThat(dynamicMessage.getAllFields().size()).isEqualTo(2);
    assertThat(((Long) dynamicMessage.getAllFields().get(keySetArray[0])).intValue())
        .isEqualTo(100);
    assertThat(dynamicMessage.getAllFields().get(keySetArray[1])).isEqualTo("John");
  }

  @Test
  public void logicalTypeToProtoTypeConverterTest() {
    FieldType fieldType;
    fieldType =
        DataTypes.BIGINT()
            .getLogicalType()
            .accept(new ProtobufUtils.LogicalTypeToProtoTypeConverter());
    assertThat(fieldType).isNotNull();
    assertThat(fieldType.getTypeName().toString()).isEqualTo("BIGINT");

    fieldType =
        DataTypes.STRING()
            .getLogicalType()
            .accept(new ProtobufUtils.LogicalTypeToProtoTypeConverter());
    assertThat(fieldType).isNotNull();
    assertThat(fieldType.getTypeName().toString()).isEqualTo("VARCHAR");

    fieldType =
        DataTypes.BINARY(10)
            .getLogicalType()
            .accept(new ProtobufUtils.LogicalTypeToProtoTypeConverter());
    assertThat(fieldType).isNotNull();
    assertThat(fieldType.getTypeName().toString()).isEqualTo("BINARY");

    fieldType =
        DataTypes.ARRAY(DataTypes.STRING())
            .getLogicalType()
            .accept(new ProtobufUtils.LogicalTypeToProtoTypeConverter());
    assertThat(fieldType).isNotNull();
    assertThat(fieldType.getTypeName().toString()).isEqualTo("BASIC_ARRAY");

    fieldType =
        DataTypes.BOOLEAN()
            .getLogicalType()
            .accept(new ProtobufUtils.LogicalTypeToProtoTypeConverter());
    assertThat(fieldType).isNotNull();
    assertThat(fieldType.getTypeName().toString()).isEqualTo("BOOLEAN");

    fieldType =
        DataTypes.BYTES()
            .getLogicalType()
            .accept(new ProtobufUtils.LogicalTypeToProtoTypeConverter());
    assertThat(fieldType).isNotNull();
    assertThat(fieldType.getTypeName().toString()).isEqualTo("VARBINARY");

    fieldType =
        DataTypes.DATE()
            .getLogicalType()
            .accept(new ProtobufUtils.LogicalTypeToProtoTypeConverter());
    assertThat(fieldType).isNotNull();
    assertThat(fieldType.getTypeName().toString()).isEqualTo("DATE");

    fieldType =
        DataTypes.DECIMAL(10, 0)
            .getLogicalType()
            .accept(new ProtobufUtils.LogicalTypeToProtoTypeConverter());
    assertThat(fieldType).isNotNull();
    assertThat(fieldType.getTypeName().toString()).isEqualTo("DECIMAL");

    fieldType =
        DataTypes.DOUBLE()
            .getLogicalType()
            .accept(new ProtobufUtils.LogicalTypeToProtoTypeConverter());
    assertThat(fieldType).isNotNull();
    assertThat(fieldType.getTypeName().toString()).isEqualTo("DOUBLE");

    fieldType =
        DataTypes.ROW()
            .getLogicalType()
            .accept(new ProtobufUtils.LogicalTypeToProtoTypeConverter());
    assertThat(fieldType).isNotNull();
    assertThat(fieldType.getTypeName().toString()).isEqualTo("ROW");

    fieldType =
        DataTypes.TIMESTAMP()
            .getLogicalType()
            .accept(new ProtobufUtils.LogicalTypeToProtoTypeConverter());
    assertThat(fieldType).isNotNull();
    assertThat(fieldType.getTypeName().toString()).isEqualTo("TIMESTAMP");

    fieldType =
        DataTypes.TIME()
            .getLogicalType()
            .accept(new ProtobufUtils.LogicalTypeToProtoTypeConverter());
    assertThat(fieldType).isNotNull();
    assertThat(fieldType.getTypeName().toString()).isEqualTo("TIME");
  }
}
