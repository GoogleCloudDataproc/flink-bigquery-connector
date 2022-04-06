/*
 * Copyright 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http:www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.flink.bigquery;

import com.google.cloud.bigquery.StandardSQLTypeName;

public class StandardSQLTypeHandler {

  public static StandardSQLTypeName handle(String sqlType) {
    StandardSQLTypeName type = null;
    switch (sqlType) {
      case "BOOLEAN":
      case "BOOL":
        type = StandardSQLTypeName.BOOL;
        break;
      case "INT64":
        type = StandardSQLTypeName.INT64;
        break;
      case "FLOAT":
      case "FLOAT64":
        type = StandardSQLTypeName.FLOAT64;
        break;
      case "BIGNUMERIC":
        type = StandardSQLTypeName.NUMERIC;
        break;
      case "INT":
        type = StandardSQLTypeName.BIGNUMERIC;
        break;
      case "BIGINT":
      case "INTEGER":
      case "DECIMAL(10, 0)":
        type = StandardSQLTypeName.NUMERIC;
        break;
      case "NUMERIC":
      case "DECIMAL(38, 9)":
        type = StandardSQLTypeName.BIGNUMERIC;
        break;
      case "STRING":
        type = StandardSQLTypeName.STRING;
        break;
      case "BYTES":
        type = StandardSQLTypeName.BYTES;
        break;
      case "STRUCT":
        type = StandardSQLTypeName.STRUCT;
        break;
      case "ARRAY":
        type = StandardSQLTypeName.ARRAY;
        break;
      case "TIMESTAMP":
      case "TIMESTAMP(6)":
        type = StandardSQLTypeName.TIMESTAMP;
        break;
      case "DATE":
        type = StandardSQLTypeName.DATE;
        break;
      case "TIME":
      case "TIME(0)":
      case "TIME(0,32)":
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
