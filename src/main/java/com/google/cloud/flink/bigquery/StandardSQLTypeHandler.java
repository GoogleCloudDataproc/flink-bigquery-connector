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

import com.google.cloud.bigquery.StandardSQLTypeName;
import org.apache.flink.table.types.logical.LogicalType;

public class StandardSQLTypeHandler {

  public static StandardSQLTypeName handle(LogicalType sqlType) {
    StandardSQLTypeName type = null;
    switch (sqlType.getTypeRoot().toString()) {
      case "BOOLEAN":
      case "BOOL":
        type = StandardSQLTypeName.BOOL;
        break;
      case "INTEGER":
      case "INT":
      case "TINYINT":
      case "SMALLINT":
      case "INT64":
      case "INT32":
      case "INT16":
      case "BIGINT":
        type = StandardSQLTypeName.INT64;
        break;
      case "FLOAT":
      case "FLOAT64":
      case "DOUBLE":
        type = StandardSQLTypeName.FLOAT64;
        break;
      case "BIGNUMERIC":
        type = StandardSQLTypeName.BIGNUMERIC;
        break;
      case "DECIMAL":
      case "NUMERIC":
        type = StandardSQLTypeName.NUMERIC;
        break;
      case "STRING":
      case "VARCHAR":
        type = StandardSQLTypeName.STRING;
        break;
      case "BYTES":
      case "VARBINARY":
        type = StandardSQLTypeName.BYTES;
        break;
      case "STRUCT":
      case "ROW":
        type = StandardSQLTypeName.STRUCT;
        break;
      case "ARRAY":
        type = StandardSQLTypeName.ARRAY;
        break;
      case "TIMESTAMP":
      case "TIMESTAMP_WITHOUT_TIME_ZONE":
        type = StandardSQLTypeName.TIMESTAMP;
        break;
      case "DATE":
        type = StandardSQLTypeName.DATE;
        break;
      case "TIME_WITHOUT_TIME_ZONE":
      case "TIME":
        type = StandardSQLTypeName.TIME;
        break;
      case "DATETIME":
        type = StandardSQLTypeName.DATETIME;
        break;
      case "GEOGRAPHY":
        type = StandardSQLTypeName.GEOGRAPHY;
        break;
      default:
        break;
    }
    return type;
  }
}
