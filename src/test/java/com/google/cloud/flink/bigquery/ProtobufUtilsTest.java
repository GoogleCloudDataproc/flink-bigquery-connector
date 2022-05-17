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

import static com.google.cloud.flink.bigquery.ProtobufUtils.toDescriptor;
import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Field.Mode;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.storage.v1beta2.ProtoSchema;
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
import org.apache.flink.table.types.logical.StructuredType;
import org.apache.flink.table.types.logical.StructuredType.StructuredAttribute;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.Test;

public class ProtobufUtilsTest {

  @Test
  public void toProtoTypeTest() {
    FieldType fieldType;
    fieldType = ProtobufUtils.toProtoType(DataTypes.BIGINT().getLogicalType());
    assertThat(fieldType).isNotNull();
    assertThat(fieldType.getTypeName().toString()).isEqualTo("BIGINT");

    fieldType = ProtobufUtils.toProtoType(DataTypes.STRING().getLogicalType());
    assertThat(fieldType).isNotNull();
    assertThat(fieldType.getTypeName().toString()).isEqualTo("VARCHAR");

    fieldType = ProtobufUtils.toProtoType(DataTypes.BINARY(10).getLogicalType());
    assertThat(fieldType).isNotNull();
    assertThat(fieldType.getTypeName().toString()).isEqualTo("BINARY");

    fieldType = ProtobufUtils.toProtoType(DataTypes.ARRAY(DataTypes.STRING()).getLogicalType());
    assertThat(fieldType).isNotNull();
    assertThat(fieldType.getTypeName().toString()).isEqualTo("ARRAY");

    fieldType = ProtobufUtils.toProtoType(DataTypes.BOOLEAN().getLogicalType());
    assertThat(fieldType).isNotNull();
    assertThat(fieldType.getTypeName().toString()).isEqualTo("BOOLEAN");

    fieldType = ProtobufUtils.toProtoType(DataTypes.BYTES().getLogicalType());
    assertThat(fieldType).isNotNull();
    assertThat(fieldType.getTypeName().toString()).isEqualTo("VARBINARY");

    fieldType = ProtobufUtils.toProtoType(DataTypes.DATE().getLogicalType());
    assertThat(fieldType).isNotNull();
    assertThat(fieldType.getTypeName().toString()).isEqualTo("DATE");

    fieldType = ProtobufUtils.toProtoType(DataTypes.DECIMAL(10, 0).getLogicalType());
    assertThat(fieldType).isNotNull();
    assertThat(fieldType.getTypeName().toString()).isEqualTo("DECIMAL");

    fieldType = ProtobufUtils.toProtoType(DataTypes.DOUBLE().getLogicalType());
    assertThat(fieldType).isNotNull();
    assertThat(fieldType.getTypeName().toString()).isEqualTo("DOUBLE");

    fieldType = ProtobufUtils.toProtoType(DataTypes.ROW().getLogicalType());
    assertThat(fieldType).isNotNull();
    assertThat(fieldType.getTypeName().toString()).isEqualTo("ROW");

    fieldType = ProtobufUtils.toProtoType(DataTypes.TIMESTAMP().getLogicalType());
    assertThat(fieldType).isNotNull();
    assertThat(fieldType.getTypeName().toString()).isEqualTo("TIMESTAMP");

    fieldType = ProtobufUtils.toProtoType(DataTypes.TIME().getLogicalType());
    assertThat(fieldType).isNotNull();
    assertThat(fieldType.getTypeName().toString()).isEqualTo("TIME");
  }

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
  public void toDescriptorStructTypeTest() throws IOException, DescriptorValidationException {
    StructuredType.Builder builder = StructuredType.newBuilder(Row.class);
    ArrayList<StructuredAttribute> listOfAttribute = new ArrayList<StructuredAttribute>();
    listOfAttribute.add(new StructuredAttribute("id", DataTypes.BIGINT().getLogicalType()));
    listOfAttribute.add(new StructuredAttribute("name", DataTypes.STRING().getLogicalType()));
    builder.attributes(listOfAttribute);
    builder.setFinal(true);
    builder.setInstantiable(true);
    StructuredType structType = builder.build();
    Descriptor descriptor = ProtobufUtils.toDescriptor(structType);
    assertThat(descriptor).isNotNull();
    assertThat(descriptor.getFields().get(0).getName()).isEqualTo("id");
    assertThat(descriptor.getFields().get(1).getName()).isEqualTo("name");
  }

  @Test
  public void toDescriptorSchemaTest() throws IOException, DescriptorValidationException {
    ArrayList<Field> listOfFileds = new ArrayList<Field>();
    listOfFileds.add(
        Field.newBuilder("id", StandardSQLTypeName.INT64).setMode(Mode.NULLABLE).build());
    listOfFileds.add(
        Field.newBuilder("name", StandardSQLTypeName.STRING).setMode(Mode.NULLABLE).build());
    FieldList fieldlist = FieldList.of(listOfFileds);
    Schema schema = Schema.of(fieldlist);
    Descriptor descriptor = ProtobufUtils.toDescriptor(schema);
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
  public void toProtoSchemaStructTypeTest() throws IOException, DescriptorValidationException {
    StructuredType.Builder builder = StructuredType.newBuilder(Row.class);
    ArrayList<StructuredAttribute> listOfAttribute = new ArrayList<StructuredAttribute>();
    listOfAttribute.add(new StructuredAttribute("id", DataTypes.BIGINT().getLogicalType()));
    listOfAttribute.add(new StructuredAttribute("name", DataTypes.STRING().getLogicalType()));
    builder.attributes(listOfAttribute);
    builder.setFinal(true);
    builder.setInstantiable(true);
    StructuredType structType = builder.build();
    ProtoSchema protoSchema = ProtobufUtils.toProtoSchema(structType);
    Descriptor descriptor = ProtobufUtils.toDescriptor(structType);
    assertThat(protoSchema).isNotNull();
    assertThat(descriptor).isNotNull();
    assertThat(descriptor.getFields().get(0).getName()).isEqualTo("id");
    assertThat(descriptor.getFields().get(1).getName()).isEqualTo("name");
  }

  @Test
  public void toProtoSchemaSchemaTest() throws IOException, DescriptorValidationException {
    ArrayList<Field> listOfFileds = new ArrayList<Field>();
    listOfFileds.add(
        Field.newBuilder("id", StandardSQLTypeName.INT64).setMode(Mode.NULLABLE).build());
    listOfFileds.add(
        Field.newBuilder("name", StandardSQLTypeName.STRING).setMode(Mode.NULLABLE).build());
    FieldList fieldlist = FieldList.of(listOfFileds);
    Schema schema = Schema.of(fieldlist);
    ProtoSchema protoSchema = ProtobufUtils.toProtoSchema(schema);
    Descriptor descriptor = ProtobufUtils.toDescriptor(schema);
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
  public void buildSingleRowMessageStructTypeTest()
      throws IOException, DescriptorValidationException {
    StructuredType.Builder builder = StructuredType.newBuilder(Row.class);
    ArrayList<StructuredAttribute> listOfAttribute = new ArrayList<StructuredAttribute>();
    listOfAttribute.add(new StructuredAttribute("id", DataTypes.BIGINT().getLogicalType()));
    listOfAttribute.add(new StructuredAttribute("name", DataTypes.STRING().getLogicalType()));
    builder.attributes(listOfAttribute);
    builder.setFinal(true);
    builder.setInstantiable(true);
    StructuredType structType = builder.build();
    Row row = new Row(3);
    row.setKind(RowKind.INSERT);
    row.setField(0, 100);
    row.setField(1, "John");
    Descriptor descriptor = toDescriptor(structType);
    DynamicMessage dynamicMessage =
        ProtobufUtils.buildSingleRowMessage(structType, descriptor, row);
    Set<FieldDescriptor> keySet = dynamicMessage.getAllFields().keySet();
    Object[] keySetArray = keySet.toArray();

    assertThat(structType).isNotNull();
    assertThat(descriptor).isNotNull();
    assertThat(dynamicMessage).isNotNull();
    assertThat(keySet).isNotNull();

    assertThat(dynamicMessage.getAllFields().size()).isEqualTo(2);
    assertThat(((Long) dynamicMessage.getAllFields().get(keySetArray[0])).intValue())
        .isEqualTo(100);
    assertThat(dynamicMessage.getAllFields().get(keySetArray[1])).isEqualTo("John");
  }

  @Test
  public void LogicalTypeToProtoTypeConverterTest() {
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
    assertThat(fieldType.getTypeName().toString()).isEqualTo("ARRAY");

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
