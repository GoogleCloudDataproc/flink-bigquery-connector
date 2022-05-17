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

import com.google.cloud.flink.bigquery.arrow.util.ArrowSchemaConverter;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.junit.Test;

public class ArrowSchemaConverterTest {
  @Test
  public void convertToSchemafromRowTypeTest() {

    List<RowField> fields = new ArrayList<RowField>();
    fields.add(new RowField("int_dataType", DataTypes.BIGINT().getLogicalType()));
    fields.add(new RowField("string_dataType", DataTypes.STRING().getLogicalType()));
    fields.add(
        new RowField("array_dataType", DataTypes.ARRAY(DataTypes.STRING()).getLogicalType()));
    fields.add(new RowField("boolean_dataType", DataTypes.BOOLEAN().getLogicalType()));
    fields.add(new RowField("bytes_dataType", DataTypes.BYTES().getLogicalType()));
    fields.add(new RowField("date_dataType", DataTypes.DATE().getLogicalType()));
    fields.add(new RowField("decimal_dataType", DataTypes.DECIMAL(10, 0).getLogicalType()));
    fields.add(new RowField("double_dataType", DataTypes.DOUBLE().getLogicalType()));
    fields.add(new RowField("row_dataType", DataTypes.ROW().getLogicalType()));
    fields.add(new RowField("timestamp_dataType", DataTypes.TIMESTAMP().getLogicalType()));
    fields.add(new RowField("time_dataType", DataTypes.TIME().getLogicalType()));
    RowType rowType = new RowType(fields);
    Schema schema = ArrowSchemaConverter.convertToSchema(rowType);
    assertThat(schema).isNotNull();
    assertThat(schema.toString())
        .isEqualTo(
            "Schema<int_dataType: Int(64, true), string_dataType: Utf8, array_dataType: List<element: Utf8>, boolean_dataType: Bool, bytes_dataType: Binary, date_dataType: Date(DAY), decimal_dataType: Decimal(10, 0, 128), double_dataType: FloatingPoint(DOUBLE), row_dataType: Struct, timestamp_dataType: Timestamp(MICROSECOND, null), time_dataType: Time(SECOND, 32)>");
  }

  @Test
  public void convertToSchemaToGetFieldTest() {

    Field field;
    field =
        ArrowSchemaConverter.convertToSchema("int_dataType", DataTypes.BIGINT().getLogicalType());
    assertThat(field).isNotNull();
    assertThat(field.toString()).isEqualTo("int_dataType: Int(64, true)");

    field =
        ArrowSchemaConverter.convertToSchema(
            "string_dataType", DataTypes.STRING().getLogicalType());
    assertThat(field).isNotNull();
    assertThat(field.toString()).isEqualTo("string_dataType: Utf8");

    field =
        ArrowSchemaConverter.convertToSchema(
            "array_dataType", DataTypes.ARRAY(DataTypes.STRING()).getLogicalType());
    assertThat(field).isNotNull();
    assertThat(field.toString()).isEqualTo("array_dataType: List<element: Utf8>");

    field =
        ArrowSchemaConverter.convertToSchema(
            "boolean_dataType", DataTypes.BOOLEAN().getLogicalType());
    assertThat(field).isNotNull();
    assertThat(field.toString()).isEqualTo("boolean_dataType: Bool");

    field =
        ArrowSchemaConverter.convertToSchema("date_dataType", DataTypes.DATE().getLogicalType());
    assertThat(field).isNotNull();
    assertThat(field.toString()).isEqualTo("date_dataType: Date(DAY)");

    field =
        ArrowSchemaConverter.convertToSchema(
            "decimal_dataType", DataTypes.DECIMAL(10, 0).getLogicalType());
    assertThat(field).isNotNull();
    assertThat(field.toString()).isEqualTo("decimal_dataType: Decimal(10, 0, 128)");

    field =
        ArrowSchemaConverter.convertToSchema(
            "double_dataType", DataTypes.DOUBLE().getLogicalType());
    assertThat(field).isNotNull();
    assertThat(field.toString()).isEqualTo("double_dataType: FloatingPoint(DOUBLE)");

    field = ArrowSchemaConverter.convertToSchema("row_dataType", DataTypes.ROW().getLogicalType());
    assertThat(field).isNotNull();
    assertThat(field.toString()).isEqualTo("row_dataType: Struct");

    field =
        ArrowSchemaConverter.convertToSchema(
            "timestamp_dataType", DataTypes.TIMESTAMP().getLogicalType());
    assertThat(field).isNotNull();
    assertThat(field.toString()).isEqualTo("timestamp_dataType: Timestamp(MICROSECOND, null)");

    field =
        ArrowSchemaConverter.convertToSchema("time_dataType", DataTypes.TIME().getLogicalType());
    assertThat(field).isNotNull();
    assertThat(field.toString()).isEqualTo("time_dataType: Time(SECOND, 32)");
  }
}
