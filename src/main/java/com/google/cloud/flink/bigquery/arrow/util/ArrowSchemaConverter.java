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
package com.google.cloud.flink.bigquery.arrow.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

/**
 * Converts an Arrow schema into Flink's type information. It uses {@link RowTypeInfo} for
 * representing objects and converts Arrow types into types that are compatible with Flink's Table &
 * SQL API.
 *
 * <p>Note: Changes in this class need to be kept in sync with the corresponding runtime classes
 * {@link ArrowRowDataDeserializationSchema}.
 */
public class ArrowSchemaConverter {

  private ArrowSchemaConverter() {}

  public static Schema convertToSchema(RowType rowType) {
    Collection<Field> fields =
        rowType.getFields().stream()
            .map(f -> convertToSchema(f.getName(), f.getType()))
            .collect(Collectors.toCollection(ArrayList::new));
    return new Schema(fields);
  }

  public static Field convertToSchema(String fieldName, LogicalType logicalType) {
    FieldType fieldType =
        new FieldType(
            logicalType.isNullable(),
            logicalType.accept(LogicalTypeToArrowTypeConverter.INSTANCE),
            null);
    List<Field> children = null;
    if (logicalType instanceof ArrayType) {
      children =
          Collections.singletonList(
              convertToSchema("element", ((ArrayType) logicalType).getElementType()));
    } else if (logicalType instanceof RowType) {
      RowType rowType = (RowType) logicalType;
      children = new ArrayList<>(rowType.getFieldCount());
      for (RowType.RowField field : rowType.getFields()) {
        children.add(convertToSchema(field.getName(), field.getType()));
      }
    }
    return new Field(fieldName, fieldType, children);
  }
}
