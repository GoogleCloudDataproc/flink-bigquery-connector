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

import static com.google.cloud.flink.bigquery.ProtobufUtils.toDescriptor;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Field.Mode;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.storage.v1beta2.ProtoSchema;
import com.google.cloud.flink.bigquery.ProtobufUtils;
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
import org.junit.Assert;
import org.junit.Test;

public class ProtobufUtilsTest {

  @Test
  public void toProtoTypeTest() {
    FieldType fieldType;
    fieldType = ProtobufUtils.toProtoType(DataTypes.BIGINT().getLogicalType());
    Assert.assertNotNull(fieldType);
    Assert.assertEquals(fieldType.getTypeName().toString(), "BIGINT");
    Assert.assertTrue(fieldType instanceof FieldType);

    fieldType = ProtobufUtils.toProtoType(DataTypes.STRING().getLogicalType());
    Assert.assertNotNull(fieldType);
    Assert.assertEquals(fieldType.getTypeName().toString(), "VARCHAR");
    Assert.assertTrue(fieldType instanceof FieldType);

    fieldType = ProtobufUtils.toProtoType(DataTypes.BINARY(10).getLogicalType());
    Assert.assertNotNull(fieldType);
    Assert.assertEquals(fieldType.getTypeName().toString(), "BINARY");
    Assert.assertTrue(fieldType instanceof FieldType);

    fieldType = ProtobufUtils.toProtoType(DataTypes.ARRAY(DataTypes.STRING()).getLogicalType());
    Assert.assertNotNull(fieldType);
    Assert.assertEquals(fieldType.getTypeName().toString(), "ARRAY");
    Assert.assertTrue(fieldType instanceof FieldType);

    fieldType = ProtobufUtils.toProtoType(DataTypes.BOOLEAN().getLogicalType());
    Assert.assertNotNull(fieldType);
    Assert.assertEquals(fieldType.getTypeName().toString(), "BOOLEAN");
    Assert.assertTrue(fieldType instanceof FieldType);

    fieldType = ProtobufUtils.toProtoType(DataTypes.BYTES().getLogicalType());
    Assert.assertNotNull(fieldType);
    Assert.assertEquals(fieldType.getTypeName().toString(), "VARBINARY");
    Assert.assertTrue(fieldType instanceof FieldType);

    fieldType = ProtobufUtils.toProtoType(DataTypes.DATE().getLogicalType());
    Assert.assertNotNull(fieldType);
    Assert.assertEquals(fieldType.getTypeName().toString(), "DATE");
    Assert.assertTrue(fieldType instanceof FieldType);

    fieldType = ProtobufUtils.toProtoType(DataTypes.DECIMAL(10, 0).getLogicalType());
    Assert.assertNotNull(fieldType);
    Assert.assertEquals(fieldType.getTypeName().toString(), "DECIMAL");
    Assert.assertTrue(fieldType instanceof FieldType);

    fieldType = ProtobufUtils.toProtoType(DataTypes.DOUBLE().getLogicalType());
    Assert.assertNotNull(fieldType);
    Assert.assertEquals(fieldType.getTypeName().toString(), "DOUBLE");
    Assert.assertTrue(fieldType instanceof FieldType);

    fieldType = ProtobufUtils.toProtoType(DataTypes.ROW().getLogicalType());
    Assert.assertNotNull(fieldType);
    Assert.assertEquals(fieldType.getTypeName().toString(), "ROW");
    Assert.assertTrue(fieldType instanceof FieldType);

    fieldType = ProtobufUtils.toProtoType(DataTypes.TIMESTAMP().getLogicalType());
    Assert.assertNotNull(fieldType);
    Assert.assertEquals(fieldType.getTypeName().toString(), "TIMESTAMP");
    Assert.assertTrue(fieldType instanceof FieldType);

    fieldType = ProtobufUtils.toProtoType(DataTypes.TIME().getLogicalType());
    Assert.assertNotNull(fieldType);
    Assert.assertEquals(fieldType.getTypeName().toString(), "TIME");
    Assert.assertTrue(fieldType instanceof FieldType);
  }

  @Test
  public void toDescriptorRowTypeTest() throws IOException, DescriptorValidationException {
    List<RowField> fields = new ArrayList<RowField>();
    fields.add(new RowField("id", DataTypes.BIGINT().getLogicalType()));
    fields.add(new RowField("name", DataTypes.STRING().getLogicalType()));
    RowType rowType = new RowType(fields);
    Descriptor descriptor = ProtobufUtils.toDescriptor(rowType);
    Assert.assertNotNull(descriptor);
    Assert.assertTrue(descriptor instanceof Descriptor);
    Assert.assertEquals(descriptor.getFields().get(0).getName(), "id");
    Assert.assertEquals(descriptor.getFields().get(1).getName(), "name");
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
    Assert.assertNotNull(descriptor);
    Assert.assertTrue(descriptor instanceof Descriptor);
    Assert.assertEquals(descriptor.getFields().get(0).getName(), "id");
    Assert.assertEquals(descriptor.getFields().get(1).getName(), "name");
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
    Assert.assertNotNull(descriptor);
    Assert.assertTrue(descriptor instanceof Descriptor);
    Assert.assertEquals(descriptor.getFields().get(0).getName(), "id");
    Assert.assertEquals(descriptor.getFields().get(1).getName(), "name");
  }

  @Test
  public void toProtoSchemaRowTypeTest() throws IOException, DescriptorValidationException {
    List<RowField> fields = new ArrayList<RowField>();
    fields.add(new RowField("id", DataTypes.BIGINT().getLogicalType()));
    fields.add(new RowField("name", DataTypes.STRING().getLogicalType()));
    RowType rowType = new RowType(fields);
    ProtoSchema protoSchema = ProtobufUtils.toProtoSchema(rowType);
    Descriptor descriptor = ProtobufUtils.toDescriptor(rowType);
    Assert.assertNotNull(protoSchema);
    Assert.assertTrue(protoSchema instanceof ProtoSchema);
    Assert.assertNotNull(descriptor);
    Assert.assertTrue(descriptor instanceof Descriptor);
    Assert.assertEquals(descriptor.getFields().get(0).getName(), "id");
    Assert.assertEquals(descriptor.getFields().get(1).getName(), "name");
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
    Assert.assertNotNull(protoSchema);
    Assert.assertTrue(protoSchema instanceof ProtoSchema);
    Assert.assertNotNull(descriptor);
    Assert.assertTrue(descriptor instanceof Descriptor);
    Assert.assertEquals(descriptor.getFields().get(0).getName(), "id");
    Assert.assertEquals(descriptor.getFields().get(1).getName(), "name");
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
    Assert.assertNotNull(protoSchema);
    Assert.assertNotNull(descriptor);
    Assert.assertTrue(protoSchema instanceof ProtoSchema);
    Assert.assertTrue(descriptor instanceof Descriptor);
    Assert.assertEquals(descriptor.getFields().get(0).getName(), "id");
    Assert.assertEquals(descriptor.getFields().get(1).getName(), "name");
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
    Assert.assertNotNull(rowType);
    Assert.assertNotNull(descriptor);
    Assert.assertNotNull(dynamicMessage);
    Assert.assertNotNull(keySet);
    Assert.assertTrue(rowType instanceof RowType);
    Assert.assertTrue(descriptor instanceof Descriptor);
    Assert.assertTrue(dynamicMessage instanceof DynamicMessage);
    Assert.assertTrue(keySet instanceof Set);
    Assert.assertEquals(dynamicMessage.getAllFields().size(), 2);
    Assert.assertEquals(((Long) dynamicMessage.getAllFields().get(keySetArray[0])).intValue(), 100);
    Assert.assertEquals(dynamicMessage.getAllFields().get(keySetArray[1]), "John");
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
    Assert.assertNotNull(structType);
    Assert.assertNotNull(descriptor);
    Assert.assertNotNull(dynamicMessage);
    Assert.assertNotNull(keySet);
    Assert.assertTrue(structType instanceof StructuredType);
    Assert.assertTrue(descriptor instanceof Descriptor);
    Assert.assertTrue(dynamicMessage instanceof DynamicMessage);
    Assert.assertTrue(keySet instanceof Set);
    Assert.assertEquals(dynamicMessage.getAllFields().size(), 2);
    Assert.assertEquals(((Long) dynamicMessage.getAllFields().get(keySetArray[0])).intValue(), 100);
    Assert.assertEquals(dynamicMessage.getAllFields().get(keySetArray[1]), "John");
  }

  @Test
  public void LogicalTypeToProtoTypeConverterTest() {
    FieldType fieldType;
    fieldType =
        DataTypes.BIGINT()
            .getLogicalType()
            .accept(new ProtobufUtils.LogicalTypeToProtoTypeConverter());
    Assert.assertNotNull(fieldType);
    Assert.assertTrue(fieldType instanceof FieldType);
    Assert.assertEquals(fieldType.getTypeName().toString(), "BIGINT");

    fieldType =
        DataTypes.STRING()
            .getLogicalType()
            .accept(new ProtobufUtils.LogicalTypeToProtoTypeConverter());
    Assert.assertNotNull(fieldType);
    Assert.assertEquals(fieldType.getTypeName().toString(), "VARCHAR");
    Assert.assertTrue(fieldType instanceof FieldType);

    fieldType =
        DataTypes.BINARY(10)
            .getLogicalType()
            .accept(new ProtobufUtils.LogicalTypeToProtoTypeConverter());
    Assert.assertNotNull(fieldType);
    Assert.assertEquals(fieldType.getTypeName().toString(), "BINARY");
    Assert.assertTrue(fieldType instanceof FieldType);

    fieldType =
        DataTypes.ARRAY(DataTypes.STRING())
            .getLogicalType()
            .accept(new ProtobufUtils.LogicalTypeToProtoTypeConverter());
    Assert.assertNotNull(fieldType);
    Assert.assertEquals(fieldType.getTypeName().toString(), "ARRAY");
    Assert.assertTrue(fieldType instanceof FieldType);

    fieldType =
        DataTypes.BOOLEAN()
            .getLogicalType()
            .accept(new ProtobufUtils.LogicalTypeToProtoTypeConverter());
    Assert.assertNotNull(fieldType);
    Assert.assertEquals(fieldType.getTypeName().toString(), "BOOLEAN");
    Assert.assertTrue(fieldType instanceof FieldType);

    fieldType =
        DataTypes.BYTES()
            .getLogicalType()
            .accept(new ProtobufUtils.LogicalTypeToProtoTypeConverter());
    Assert.assertNotNull(fieldType);
    Assert.assertEquals(fieldType.getTypeName().toString(), "VARBINARY");
    Assert.assertTrue(fieldType instanceof FieldType);

    fieldType =
        DataTypes.DATE()
            .getLogicalType()
            .accept(new ProtobufUtils.LogicalTypeToProtoTypeConverter());
    Assert.assertNotNull(fieldType);
    Assert.assertEquals(fieldType.getTypeName().toString(), "DATE");
    Assert.assertTrue(fieldType instanceof FieldType);

    fieldType =
        DataTypes.DECIMAL(10, 0)
            .getLogicalType()
            .accept(new ProtobufUtils.LogicalTypeToProtoTypeConverter());
    Assert.assertNotNull(fieldType);
    Assert.assertEquals(fieldType.getTypeName().toString(), "DECIMAL");
    Assert.assertTrue(fieldType instanceof FieldType);

    fieldType =
        DataTypes.DOUBLE()
            .getLogicalType()
            .accept(new ProtobufUtils.LogicalTypeToProtoTypeConverter());
    Assert.assertNotNull(fieldType);
    Assert.assertEquals(fieldType.getTypeName().toString(), "DOUBLE");
    Assert.assertTrue(fieldType instanceof FieldType);

    fieldType =
        DataTypes.ROW()
            .getLogicalType()
            .accept(new ProtobufUtils.LogicalTypeToProtoTypeConverter());
    Assert.assertNotNull(fieldType);
    Assert.assertEquals(fieldType.getTypeName().toString(), "ROW");
    Assert.assertTrue(fieldType instanceof FieldType);

    fieldType =
        DataTypes.TIMESTAMP()
            .getLogicalType()
            .accept(new ProtobufUtils.LogicalTypeToProtoTypeConverter());
    Assert.assertNotNull(fieldType);
    Assert.assertEquals(fieldType.getTypeName().toString(), "TIMESTAMP");
    Assert.assertTrue(fieldType instanceof FieldType);

    fieldType =
        DataTypes.TIME()
            .getLogicalType()
            .accept(new ProtobufUtils.LogicalTypeToProtoTypeConverter());
    Assert.assertNotNull(fieldType);
    Assert.assertEquals(fieldType.getTypeName().toString(), "TIME");
    Assert.assertTrue(fieldType instanceof FieldType);
  }
}
