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

import com.google.cloud.flink.bigquery.arrow.util.ArrowToRowDataConverters;
import com.google.cloud.flink.bigquery.arrow.util.ArrowToRowDataConverters.ArrowToRowDataConverter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.junit.Assert;
import org.junit.Test;

public class ArrowToRowDataConvertersTest {

  @Test
  public void createRowConverterTest() {
    List<String> selectedFields =
        Arrays.asList(
            "int_dataType",
            "string_dataType",
            "binary_dataType",
            "array_dataType",
            "boolean_dataType",
            "bytes_dataType",
            "date_dataType",
            "decimal_dataType",
            "double_dataType",
            "row_dataType",
            "timestamp_dataType",
            "time_dataType");
    List<RowField> fields = new ArrayList<RowField>();
    fields.add(new RowField("int_dataType", DataTypes.BIGINT().getLogicalType()));
    fields.add(new RowField("string_dataType", DataTypes.STRING().getLogicalType()));
    fields.add(new RowField("binary_dataType", DataTypes.BINARY(10).getLogicalType()));
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
    List<String> readSessionFieldNames =
        Arrays.asList(
            "int_dataType",
            "string_dataType",
            "binary_dataType",
            "array_dataType",
            "boolean_dataType",
            "bytes_dataType",
            "date_dataType",
            "decimal_dataType",
            "double_dataType",
            "row_dataType",
            "timestamp_dataType",
            "time_dataType");

    ArrowToRowDataConverter createRowConverter =
        ArrowToRowDataConverters.createRowConverter(rowType, readSessionFieldNames, selectedFields);
    Assert.assertNotNull(createRowConverter);
  }
}
