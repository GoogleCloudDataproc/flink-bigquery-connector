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
package com.google.cloud.flink.bigquery.util;

import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.common.collect.ImmutableMap;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

public class StandardSQLTypeHandler {

  static ImmutableMap<LogicalTypeRoot, StandardSQLTypeName> bqStandardSQLTypes =
      new ImmutableMap.Builder<LogicalTypeRoot, StandardSQLTypeName>()
          .put(LogicalTypeRoot.BOOLEAN, StandardSQLTypeName.BOOL)
          .put(LogicalTypeRoot.SMALLINT, StandardSQLTypeName.INT64)
          .put(LogicalTypeRoot.TINYINT, StandardSQLTypeName.INT64)
          .put(LogicalTypeRoot.INTEGER, StandardSQLTypeName.INT64)
          .put(LogicalTypeRoot.BIGINT, StandardSQLTypeName.INT64)
          .put(LogicalTypeRoot.FLOAT, StandardSQLTypeName.FLOAT64)
          .put(LogicalTypeRoot.DOUBLE, StandardSQLTypeName.FLOAT64)
          .put(LogicalTypeRoot.DECIMAL, StandardSQLTypeName.NUMERIC)
          .put(LogicalTypeRoot.VARCHAR, StandardSQLTypeName.STRING)
          .put(LogicalTypeRoot.VARBINARY, StandardSQLTypeName.BYTES)
          .put(LogicalTypeRoot.ROW, StandardSQLTypeName.STRUCT)
          .put(LogicalTypeRoot.ARRAY, StandardSQLTypeName.ARRAY)
          .put(LogicalTypeRoot.DATE, StandardSQLTypeName.DATE)
          .put(LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE, StandardSQLTypeName.TIMESTAMP)
          .put(LogicalTypeRoot.TIME_WITHOUT_TIME_ZONE, StandardSQLTypeName.TIME)
          .build();

  public static StandardSQLTypeName handle(LogicalType sqlType) {
    LogicalTypeRoot rootType = sqlType.getTypeRoot();
    return bqStandardSQLTypes.get(rootType);
  }
}
