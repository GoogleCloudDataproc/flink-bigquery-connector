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

import java.util.List;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource.Context;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

public class BigQueryArrowFormat implements DecodingFormat<DeserializationSchema<RowData>> {
  private List<String> selectedFieldList;

  public BigQueryArrowFormat(List<String> selectedFieldList) {
    this.selectedFieldList = selectedFieldList;
  }

  @Override
  public ChangelogMode getChangelogMode() {
    return ChangelogMode.insertOnly();
  }

  @Override
  public DeserializationSchema<RowData> createRuntimeDecoder(
      Context context, DataType producedDataType) {

    final RowType rowType = (RowType) producedDataType.getLogicalType();
    final TypeInformation<RowData> rowDataTypeInfo =
        (TypeInformation<RowData>) context.createTypeInformation(producedDataType);
    return new ArrowRowDataDeserializationSchema(rowType, rowDataTypeInfo, selectedFieldList);
  }
}
