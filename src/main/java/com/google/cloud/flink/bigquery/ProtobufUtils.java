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

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Field.Mode;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.storage.v1beta2.ProtoSchema;
import com.google.cloud.bigquery.storage.v1beta2.ProtoSchemaConverter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.annotation.Internal;
import org.apache.flink.fnexecution.v1.FlinkFnApi;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LegacyTypeInformationType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.StructuredType;
import org.apache.flink.table.types.logical.StructuredType.StructuredAttribute;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.utils.LogicalTypeDefaultVisitor;
import org.apache.flink.types.Row;

/**
 * Utilities for converting Flink logical types, such as convert it to the related TypeSerializer or
 * ProtoType.
 */
@Internal
public final class ProtobufUtils {
  private static final String MAPTYPE_ERROR_MESSAGE = "MapType is unsupported.";
  private static final int MAX_BIGQUERY_NESTED_DEPTH = 15;
  private static final String EMPTY_STRING = "";
  private static final String RESERVED_NESTED_TYPE_NAME = "STRUCT";

  private static final ImmutableMap<Field.Mode, DescriptorProtos.FieldDescriptorProto.Label>
      BigQueryModeToProtoFieldLabel =
          new ImmutableMap.Builder<Field.Mode, DescriptorProtos.FieldDescriptorProto.Label>()
              .put(Field.Mode.NULLABLE, DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
              .put(Field.Mode.REPEATED, DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED)
              .put(Field.Mode.REQUIRED, DescriptorProtos.FieldDescriptorProto.Label.LABEL_REQUIRED)
              .build();

  private static final ImmutableMap<LegacySQLTypeName, DescriptorProtos.FieldDescriptorProto.Type>
      BigQueryToProtoType =
          new ImmutableMap.Builder<LegacySQLTypeName, DescriptorProtos.FieldDescriptorProto.Type>()
              .put(LegacySQLTypeName.BYTES, DescriptorProtos.FieldDescriptorProto.Type.TYPE_BYTES)
              .put(LegacySQLTypeName.INTEGER, DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64)
              .put(LegacySQLTypeName.BOOLEAN, DescriptorProtos.FieldDescriptorProto.Type.TYPE_BOOL)
              .put(LegacySQLTypeName.FLOAT, DescriptorProtos.FieldDescriptorProto.Type.TYPE_DOUBLE)
              .put(
                  LegacySQLTypeName.NUMERIC,
                  DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING) // #####TYPE_INT64
              .put(
                  LegacySQLTypeName.BIGNUMERIC,
                  DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64)
              .put(LegacySQLTypeName.STRING, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)
              .put(
                  LegacySQLTypeName.TIMESTAMP,
                  DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)
              .put(
                  LegacySQLTypeName.DATE,
                  DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64) // ########
              .put(
                  LegacySQLTypeName.DATETIME,
                  DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING) // ##########
              .put(
                  LegacySQLTypeName.GEOGRAPHY,
                  DescriptorProtos.FieldDescriptorProto.Type.TYPE_BYTES)
              .put(
                  LegacySQLTypeName.TIME,
                  DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING) // ##########
              .put(
                  LegacySQLTypeName.RECORD,
                  DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING) // ##########
              .build();

  public static FlinkFnApi.Schema.FieldType toProtoType(LogicalType logicalType) {
    return logicalType.accept(new ProtobufUtils.LogicalTypeToProtoTypeConverter());
  }

  public static ProtoSchema toProtoSchema(Schema schema) throws IllegalArgumentException {
    try {
      Descriptors.Descriptor descriptor = toDescriptor(schema);
      return ProtoSchemaConverter.convert(descriptor);
    } catch (Descriptors.DescriptorValidationException e) {
      throw new IllegalArgumentException("Could not build Proto-Schema from Flink schema", e);
    }
  }

  public static ProtoSchema toProtoSchema(RowType schema) throws IllegalArgumentException {
    try {
      Descriptors.Descriptor descriptor = toDescriptor(schema);
      return ProtoSchemaConverter.convert(descriptor);
    } catch (Descriptors.DescriptorValidationException e) {
      throw new IllegalArgumentException("Could not build Proto-Schema from Flink schema", e);
    }
  }

  public static ProtoSchema toProtoSchema(StructuredType schema) throws IllegalArgumentException {
    try {
      Descriptors.Descriptor descriptor = toDescriptor(schema);
      return ProtoSchemaConverter.convert(descriptor);
    } catch (Descriptors.DescriptorValidationException e) {
      throw new IllegalArgumentException("Could not build Proto-Schema from Flink schema", e);
    }
  }

  private static DescriptorProtos.FieldDescriptorProto.Type toProtoFieldType(
      LegacySQLTypeName bqType) {
    if (LegacySQLTypeName.RECORD.equals(bqType)) {
      throw new IllegalStateException(
          "Program attempted to return an atomic data-type for a RECORD");
    }
    return Preconditions.checkNotNull(
        BigQueryToProtoType.get(bqType),
        new IllegalArgumentException("Unexpected type: " + bqType.name()));
  }

  public static Descriptors.Descriptor toDescriptor(RowType schema)
      throws Descriptors.DescriptorValidationException {
    DescriptorProtos.DescriptorProto.Builder descriptorBuilder =
        DescriptorProtos.DescriptorProto.newBuilder().setName("Schema");
    ArrayList<Field> listOfFileds = new ArrayList<Field>();
    Iterator<RowField> streamdata = schema.getFields().iterator();
    while (streamdata.hasNext()) {
      RowField elem = streamdata.next();
      boolean isNullable = elem.getType().isNullable();
      Mode typeMode;
      if (isNullable) {
        typeMode = Mode.NULLABLE;
      } else {
        typeMode = Mode.REQUIRED;
      }
      if ("ROW".equals(elem.getType().getTypeRoot().toString())) {
        listOfFileds.add(
            Field.newBuilder(
                    elem.getName(),
                    StandardSQLTypeName.STRUCT,
                    FieldList.of(getListOfSubFields(elem.getType())))
                .setMode(typeMode)
                .build());
      } else if ("ARRAY".equals(elem.getType().getTypeRoot().toString())) {
        listOfFileds.add(
            Field.newBuilder(
                    elem.getName(),
                    StandardSQLTypeHandler.handle(((ArrayType) elem.getType()).getElementType()))
                .setMode(Mode.REPEATED)
                .build());
      } else {
        listOfFileds.add(
            Field.newBuilder(elem.getName(), StandardSQLTypeHandler.handle(elem.getType()))
                .setMode(typeMode)
                .build());
      }
    }
    int initialDepth = 0;
    DescriptorProtos.DescriptorProto descriptorProto =
        buildDescriptorProtoWithFields(descriptorBuilder, FieldList.of(listOfFileds), initialDepth);

    return createDescriptorFromProto(descriptorProto);
  }

  public static Descriptors.Descriptor toDescriptor(StructuredType schema)
      throws Descriptors.DescriptorValidationException {
    DescriptorProtos.DescriptorProto.Builder descriptorBuilder =
        DescriptorProtos.DescriptorProto.newBuilder().setName("Schema");
    ArrayList<Field> listOfFileds = new ArrayList<Field>();
    Iterator<StructuredAttribute> streamdata = schema.getAttributes().stream().iterator();
    while (streamdata.hasNext()) {
      StructuredAttribute elem = streamdata.next();
      if ("ROW".equals(elem.getType().getTypeRoot().toString())) {
        Field.newBuilder(
                elem.getName(),
                StandardSQLTypeName.STRUCT,
                FieldList.of(getListOfSubFields(elem.getType())))
            .setMode(Mode.NULLABLE)
            .build();
      } else {
        listOfFileds.add(
            Field.newBuilder(elem.getName(), StandardSQLTypeHandler.handle(elem.getType()))
                .setMode(Mode.NULLABLE)
                .build());
      }
    }
    int initialDepth = 0;
    DescriptorProtos.DescriptorProto descriptorProto =
        buildDescriptorProtoWithFields(descriptorBuilder, FieldList.of(listOfFileds), initialDepth);

    return createDescriptorFromProto(descriptorProto);
  }

  static ArrayList<Field> getListOfSubFields(LogicalType logicalType) {
    ArrayList<Field> listOfSubFileds = new ArrayList<Field>();
    List<String> fieldNameList = new ArrayList<String>();
    Stream.of(logicalType.toString().split(","))
        .forEach(
            typeStr -> {
              fieldNameList.add(
                  typeStr.substring(
                      typeStr.indexOf("`", 1) + 1,
                      typeStr.indexOf("`", typeStr.indexOf("`", 1) + 1)));
            });
    List<LogicalType> logicalTypeList = logicalType.getChildren();
    for (int i = 0; i < logicalTypeList.size(); i++) {
      listOfSubFileds.add(
          Field.newBuilder(
                  fieldNameList.get(i), StandardSQLTypeHandler.handle(logicalTypeList.get(i)))
              .setMode(logicalTypeList.get(i).isNullable() ? Mode.NULLABLE : Mode.REQUIRED)
              .build());
    }
    return listOfSubFileds;
  }

  private static Descriptors.Descriptor toDescriptor(Schema schema)
      throws Descriptors.DescriptorValidationException {
    DescriptorProtos.DescriptorProto.Builder descriptorBuilder =
        DescriptorProtos.DescriptorProto.newBuilder().setName("Schema");

    FieldList fields = schema.getFields();

    int initialDepth = 0;
    DescriptorProtos.DescriptorProto descriptorProto =
        buildDescriptorProtoWithFields(descriptorBuilder, fields, initialDepth);

    return createDescriptorFromProto(descriptorProto);
  }

  @VisibleForTesting
  static DescriptorProtos.DescriptorProto buildDescriptorProtoWithFields(
      DescriptorProtos.DescriptorProto.Builder descriptorBuilder, FieldList fields, int depth) {
    Preconditions.checkArgument(
        depth < MAX_BIGQUERY_NESTED_DEPTH,
        "Tried to convert a BigQuery schema that exceeded BigQuery maximum nesting depth");
    int messageNumber = 1;
    for (Field field : fields) {
      String fieldName = field.getName();
      DescriptorProtos.FieldDescriptorProto.Label fieldLabel = toProtoFieldLabel(field.getMode());

      if (field.getType().equals(LegacySQLTypeName.RECORD)) {
        String recordTypeName =
            RESERVED_NESTED_TYPE_NAME + messageNumber; // TODO: Maintain this as a reserved
        // nested-type name, which no
        // column can have.
        DescriptorProtos.DescriptorProto.Builder nestedFieldTypeBuilder =
            descriptorBuilder.addNestedTypeBuilder();
        nestedFieldTypeBuilder.setName(recordTypeName);

        descriptorBuilder.addField(
            createProtoFieldBuilder(fieldName, fieldLabel, messageNumber)
                .setTypeName(recordTypeName));
      } else {
        DescriptorProtos.FieldDescriptorProto.Type fieldType = toProtoFieldType(field.getType());
        descriptorBuilder.addField(
            createProtoFieldBuilder(fieldName, fieldLabel, messageNumber, fieldType));
      }
      messageNumber++;
    }
    return descriptorBuilder.build();
  }

  private static DescriptorProtos.FieldDescriptorProto.Label toProtoFieldLabel(Field.Mode mode) {
    return Preconditions.checkNotNull(
        BigQueryModeToProtoFieldLabel.get(mode),
        new IllegalArgumentException("A BigQuery Field Mode was invalid: " + mode.name()));
  }

  private static DescriptorProtos.FieldDescriptorProto.Builder createProtoFieldBuilder(
      String fieldName, DescriptorProtos.FieldDescriptorProto.Label fieldLabel, int messageNumber) {
    return DescriptorProtos.FieldDescriptorProto.newBuilder()
        .setName(fieldName)
        .setLabel(fieldLabel)
        .setNumber(messageNumber);
  }

  @VisibleForTesting
  static DescriptorProtos.FieldDescriptorProto.Builder createProtoFieldBuilder(
      String fieldName,
      DescriptorProtos.FieldDescriptorProto.Label fieldLabel,
      int messageNumber,
      DescriptorProtos.FieldDescriptorProto.Type fieldType) {
    return createProtoFieldBuilder(fieldName, fieldLabel, messageNumber).setType(fieldType);
  }

  private static Descriptors.Descriptor createDescriptorFromProto(
      DescriptorProtos.DescriptorProto descriptorProto)
      throws Descriptors.DescriptorValidationException {
    DescriptorProtos.FileDescriptorProto fileDescriptorProto =
        DescriptorProtos.FileDescriptorProto.newBuilder().addMessageType(descriptorProto).build();

    Descriptors.Descriptor descriptor =
        Descriptors.FileDescriptor.buildFrom(
                fileDescriptorProto, new Descriptors.FileDescriptor[] {})
            .getMessageTypes()
            .get(0);

    return descriptor;
  }

  public static DynamicMessage buildSingleRowMessage(
      RowType schema, Descriptors.Descriptor schemaDescriptor, Row row) {
    DynamicMessage.Builder messageBuilder = DynamicMessage.newBuilder(schemaDescriptor);

    for (int fieldIndex = 0; fieldIndex < schemaDescriptor.getFields().size(); fieldIndex++) {
      int protoFieldNumber = fieldIndex + 1;
      RowField flinkField = schema.getFields().get(fieldIndex);
      LogicalType flinkType = flinkField.getType();
      Object flinkValue = row.getField(fieldIndex);
      boolean nullable = flinkType.isNullable();
      Descriptors.Descriptor nestedTypeDescriptor =
          schemaDescriptor.findNestedTypeByName(RESERVED_NESTED_TYPE_NAME + protoFieldNumber);
      Object protoValue =
          convertFlinkValueToProtoRowValue(flinkType, flinkValue, nullable, nestedTypeDescriptor);

      if (protoValue == null) {
        continue;
      }

      messageBuilder.setField(schemaDescriptor.findFieldByNumber(protoFieldNumber), protoValue);
    }

    return messageBuilder.build();
  }

  public static DynamicMessage buildSingleRowMessage(
      StructuredType schema, Descriptors.Descriptor schemaDescriptor, Row row) {
    DynamicMessage.Builder messageBuilder = DynamicMessage.newBuilder(schemaDescriptor);

    for (int fieldIndex = 0; fieldIndex < schemaDescriptor.getFields().size(); fieldIndex++) {
      int protoFieldNumber = fieldIndex + 1;
      StructuredAttribute flinkField =
          schema.getAttributes().get(fieldIndex);
      LogicalType flinkType = flinkField.getType();
      Object flinkValue = row.getField(fieldIndex);
      boolean nullable = flinkType.isNullable();
      Descriptors.Descriptor nestedTypeDescriptor =
          schemaDescriptor.findNestedTypeByName(RESERVED_NESTED_TYPE_NAME + protoFieldNumber);
      Object protoValue =
          convertFlinkValueToProtoRowValue(flinkType, flinkValue, nullable, nestedTypeDescriptor);

      if (protoValue == null) {
        continue;
      }

      messageBuilder.setField(schemaDescriptor.findFieldByNumber(protoFieldNumber), protoValue);
    }

    return messageBuilder.build();
  }

  private static Object convertFlinkValueToProtoRowValue(
      LogicalType flinkType,
      Object flinkValue,
      boolean nullable,
      Descriptors.Descriptor nestedTypeDescriptor) {
    if (flinkValue == null) {
      if (!nullable) {
        throw new IllegalArgumentException("Non-nullable field was null.");
      } else {
        return null;
      }
    }

    if (flinkType instanceof ArrayType) {

      ArrayType arrayType = (ArrayType) flinkType;
      LogicalType elementType = arrayType.getElementType();
      Object[] flinkArrayData = (Object[]) flinkValue;
      boolean containsNull = arrayType.isNullable();
      List<Object> protoValue = new ArrayList<>();
      for (Object sparkElement : flinkArrayData) {
        Object converted =
            convertFlinkValueToProtoRowValue(
                elementType, sparkElement, containsNull, nestedTypeDescriptor);
        if (converted == null) {
          continue;
        }
        protoValue.add(converted);
      }
      return protoValue;
    }

    if (flinkType instanceof StructuredType) {
      return buildSingleRowMessage(
          (StructuredType) flinkType, nestedTypeDescriptor, (Row) flinkValue);
    }
    if (flinkType instanceof RowType) {
      return buildSingleRowMessage((RowType) flinkType, nestedTypeDescriptor, (Row) flinkValue);
    }
    if (flinkType instanceof TinyIntType
        || flinkType instanceof SmallIntType
        || flinkType instanceof IntType
        || flinkType instanceof BigIntType) { // || flinkType instanceof TimestampType) {
      return ((Number) flinkValue).longValue();
    }
    if (flinkType instanceof TimestampType) {
      return flinkValue.toString();
    }
    if (flinkType instanceof DateType) {
      return ((LocalDate) flinkValue).toEpochDay();
    }
    if (flinkType instanceof TimeType) {
      return ((LocalTime) flinkValue);
    }
    if (flinkType instanceof FloatType || flinkType instanceof DoubleType) {
      return ((Number) flinkValue).doubleValue();
    }

    if (flinkType instanceof DecimalType) {
      return convertDecimalToString(flinkValue);
    }

    if (flinkType instanceof BooleanType) {
      return flinkValue;
    }

    if (flinkType instanceof BinaryType) {
      return flinkValue;
    }

    if (flinkType instanceof VarCharType) {
      return new String(((String) flinkValue).getBytes(UTF_8), UTF_8);
    }

    if (flinkType instanceof MapType) {
      throw new IllegalArgumentException(MAPTYPE_ERROR_MESSAGE);
    }

    if (flinkType.toString().equalsIgnoreCase("BYTES")) {
      return flinkValue;
    }

    throw new IllegalStateException("Unexpected type: " + flinkType);
  }

  private static String convertDecimalToString(Object flinkValue) {
    BigDecimal bigDecimal = (BigDecimal) flinkValue;
    return bigDecimal.toPlainString();
  }

  /** Converter That convert the logicalType to the related Prototype. */
  public static class LogicalTypeToProtoTypeConverter
      extends LogicalTypeDefaultVisitor<FlinkFnApi.Schema.FieldType> {
    @Override
    public FlinkFnApi.Schema.FieldType visit(BooleanType booleanType) {
      return FlinkFnApi.Schema.FieldType.newBuilder()
          .setTypeName(FlinkFnApi.Schema.TypeName.BOOLEAN)
          .setNullable(booleanType.isNullable())
          .build();
    }

    @Override
    public FlinkFnApi.Schema.FieldType visit(TinyIntType tinyIntType) {
      return FlinkFnApi.Schema.FieldType.newBuilder()
          .setTypeName(FlinkFnApi.Schema.TypeName.TINYINT)
          .setNullable(tinyIntType.isNullable())
          .build();
    }

    @Override
    public FlinkFnApi.Schema.FieldType visit(SmallIntType smallIntType) {
      return FlinkFnApi.Schema.FieldType.newBuilder()
          .setTypeName(FlinkFnApi.Schema.TypeName.SMALLINT)
          .setNullable(smallIntType.isNullable())
          .build();
    }

    @Override
    public FlinkFnApi.Schema.FieldType visit(IntType intType) {
      return FlinkFnApi.Schema.FieldType.newBuilder()
          .setTypeName(FlinkFnApi.Schema.TypeName.INT)
          .setNullable(intType.isNullable())
          .build();
    }

    @Override
    public FlinkFnApi.Schema.FieldType visit(BigIntType bigIntType) {
      return FlinkFnApi.Schema.FieldType.newBuilder()
          .setTypeName(FlinkFnApi.Schema.TypeName.BIGINT)
          .setNullable(bigIntType.isNullable())
          .build();
    }

    @Override
    public FlinkFnApi.Schema.FieldType visit(FloatType floatType) {
      return FlinkFnApi.Schema.FieldType.newBuilder()
          .setTypeName(FlinkFnApi.Schema.TypeName.FLOAT)
          .setNullable(floatType.isNullable())
          .build();
    }

    @Override
    public FlinkFnApi.Schema.FieldType visit(DoubleType doubleType) {
      return FlinkFnApi.Schema.FieldType.newBuilder()
          .setTypeName(FlinkFnApi.Schema.TypeName.DOUBLE)
          .setNullable(doubleType.isNullable())
          .build();
    }

    @Override
    public FlinkFnApi.Schema.FieldType visit(BinaryType binaryType) {
      return FlinkFnApi.Schema.FieldType.newBuilder()
          .setTypeName(FlinkFnApi.Schema.TypeName.BINARY)
          .setBinaryInfo(
              FlinkFnApi.Schema.BinaryInfo.newBuilder().setLength(binaryType.getLength()))
          .setNullable(binaryType.isNullable())
          .build();
    }

    @Override
    public FlinkFnApi.Schema.FieldType visit(VarBinaryType varBinaryType) {
      return FlinkFnApi.Schema.FieldType.newBuilder()
          .setTypeName(FlinkFnApi.Schema.TypeName.VARBINARY)
          .setVarBinaryInfo(
              FlinkFnApi.Schema.VarBinaryInfo.newBuilder().setLength(varBinaryType.getLength()))
          .setNullable(varBinaryType.isNullable())
          .build();
    }

    @Override
    public FlinkFnApi.Schema.FieldType visit(CharType charType) {
      return FlinkFnApi.Schema.FieldType.newBuilder()
          .setTypeName(FlinkFnApi.Schema.TypeName.CHAR)
          .setCharInfo(FlinkFnApi.Schema.CharInfo.newBuilder().setLength(charType.getLength()))
          .setNullable(charType.isNullable())
          .build();
    }

    @Override
    public FlinkFnApi.Schema.FieldType visit(VarCharType varCharType) {
      return FlinkFnApi.Schema.FieldType.newBuilder()
          .setTypeName(FlinkFnApi.Schema.TypeName.VARCHAR)
          .setVarCharInfo(
              FlinkFnApi.Schema.VarCharInfo.newBuilder().setLength(varCharType.getLength()))
          .setNullable(varCharType.isNullable())
          .build();
    }

    @Override
    public FlinkFnApi.Schema.FieldType visit(DateType dateType) {
      return FlinkFnApi.Schema.FieldType.newBuilder()
          .setTypeName(FlinkFnApi.Schema.TypeName.DATE)
          .setNullable(dateType.isNullable())
          .build();
    }

    @Override
    public FlinkFnApi.Schema.FieldType visit(TimeType timeType) {
      return FlinkFnApi.Schema.FieldType.newBuilder()
          .setTypeName(FlinkFnApi.Schema.TypeName.TIME)
          .setTimeInfo(
              FlinkFnApi.Schema.TimeInfo.newBuilder().setPrecision(timeType.getPrecision()))
          .setNullable(timeType.isNullable())
          .build();
    }

    @Override
    public FlinkFnApi.Schema.FieldType visit(TimestampType timestampType) {
      FlinkFnApi.Schema.FieldType.Builder builder =
          FlinkFnApi.Schema.FieldType.newBuilder()
              .setTypeName(FlinkFnApi.Schema.TypeName.TIMESTAMP)
              .setNullable(timestampType.isNullable());

      FlinkFnApi.Schema.TimestampInfo.Builder timestampInfoBuilder =
          FlinkFnApi.Schema.TimestampInfo.newBuilder().setPrecision(timestampType.getPrecision());
      builder.setTimestampInfo(timestampInfoBuilder);
      return builder.build();
    }

    @Override
    public FlinkFnApi.Schema.FieldType visit(LocalZonedTimestampType localZonedTimestampType) {
      FlinkFnApi.Schema.FieldType.Builder builder =
          FlinkFnApi.Schema.FieldType.newBuilder()
              .setTypeName(FlinkFnApi.Schema.TypeName.LOCAL_ZONED_TIMESTAMP)
              .setNullable(localZonedTimestampType.isNullable());

      FlinkFnApi.Schema.LocalZonedTimestampInfo.Builder dateTimeBuilder =
          FlinkFnApi.Schema.LocalZonedTimestampInfo.newBuilder()
              .setPrecision(localZonedTimestampType.getPrecision());
      builder.setLocalZonedTimestampInfo(dateTimeBuilder.build());
      return builder.build();
    }

    @Override
    public FlinkFnApi.Schema.FieldType visit(DecimalType decimalType) {
      FlinkFnApi.Schema.FieldType.Builder builder =
          FlinkFnApi.Schema.FieldType.newBuilder()
              .setTypeName(FlinkFnApi.Schema.TypeName.DECIMAL)
              .setNullable(decimalType.isNullable());

      FlinkFnApi.Schema.DecimalInfo.Builder decimalInfoBuilder =
          FlinkFnApi.Schema.DecimalInfo.newBuilder()
              .setPrecision(decimalType.getPrecision())
              .setScale(decimalType.getScale());
      builder.setDecimalInfo(decimalInfoBuilder);
      return builder.build();
    }

    @Override
    public FlinkFnApi.Schema.FieldType visit(ArrayType arrayType) {
      FlinkFnApi.Schema.FieldType.Builder builder =
          FlinkFnApi.Schema.FieldType.newBuilder()
              .setTypeName(FlinkFnApi.Schema.TypeName.ARRAY)
              .setNullable(arrayType.isNullable());

      FlinkFnApi.Schema.FieldType elementFieldType = arrayType.getElementType().accept(this);
      builder.setCollectionElementType(elementFieldType);
      return builder.build();
    }

    @Override
    public FlinkFnApi.Schema.FieldType visit(MapType mapType) {
      FlinkFnApi.Schema.FieldType.Builder builder =
          FlinkFnApi.Schema.FieldType.newBuilder()
              .setTypeName(FlinkFnApi.Schema.TypeName.MAP)
              .setNullable(mapType.isNullable());

      FlinkFnApi.Schema.MapInfo.Builder mapBuilder =
          FlinkFnApi.Schema.MapInfo.newBuilder()
              .setKeyType(mapType.getKeyType().accept(this))
              .setValueType(mapType.getValueType().accept(this));
      builder.setMapInfo(mapBuilder.build());
      return builder.build();
    }

    @Override
    public FlinkFnApi.Schema.FieldType visit(RowType rowType) {
      FlinkFnApi.Schema.FieldType.Builder builder =
          FlinkFnApi.Schema.FieldType.newBuilder()
              .setTypeName(FlinkFnApi.Schema.TypeName.ROW)
              .setNullable(rowType.isNullable());

      FlinkFnApi.Schema.Builder schemaBuilder = FlinkFnApi.Schema.newBuilder();
      for (RowType.RowField field : rowType.getFields()) {
        schemaBuilder.addFields(
            FlinkFnApi.Schema.Field.newBuilder()
                .setName(field.getName())
                .setDescription(field.getDescription().orElse(EMPTY_STRING))
                .setType(field.getType().accept(this))
                .build());
      }
      builder.setRowSchema(schemaBuilder.build());
      return builder.build();
    }

    @Override
    protected FlinkFnApi.Schema.FieldType defaultMethod(LogicalType logicalType) {
      if (logicalType instanceof LegacyTypeInformationType) {
        Class<?> typeClass =
            ((LegacyTypeInformationType) logicalType).getTypeInformation().getTypeClass();
        if (typeClass == BigDecimal.class) {
          FlinkFnApi.Schema.FieldType.Builder builder =
              FlinkFnApi.Schema.FieldType.newBuilder()
                  .setTypeName(FlinkFnApi.Schema.TypeName.DECIMAL)
                  .setNullable(logicalType.isNullable());
          // Because we can't get precision and scale from legacy BIG_DEC_TYPE_INFO,
          // we set the precision and scale to default value compatible with python.
          FlinkFnApi.Schema.DecimalInfo.Builder decimalTypeBuilder =
              FlinkFnApi.Schema.DecimalInfo.newBuilder().setPrecision(38).setScale(18);
          builder.setDecimalInfo(decimalTypeBuilder);
          return builder.build();
        }
      }
      throw new UnsupportedOperationException(
          String.format(
              "UDF doesn't support logical type %s currently.", logicalType.asSummaryString()));
    }
  }
}
