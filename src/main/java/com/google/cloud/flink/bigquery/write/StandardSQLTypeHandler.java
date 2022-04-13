package com.google.cloud.flink.bigquery.write;

import com.google.cloud.bigquery.StandardSQLTypeName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StandardSQLTypeHandler {

  private static final Logger log = LoggerFactory.getLogger(StandardSQLTypeHandler.class);

  public static StandardSQLTypeName handle(String logicalType) {
    StandardSQLTypeName type = null;
    switch (logicalType) {
      case "BOOLEAN":
      case "BOOL":
        type = StandardSQLTypeName.BOOL; //
        break;
      case "INT64":
        type = StandardSQLTypeName.INT64; //
        break;
      case "FLOAT":
      case "FLOAT64":
        type = StandardSQLTypeName.FLOAT64;
        break;
      case "BIGNUMERIC":
        type = StandardSQLTypeName.BIGNUMERIC; //
        break;
      case "INT":
      case "BIGINT":
      case "INTEGER":
        type = StandardSQLTypeName.INT64; //
        break;
      case "DECIMAL(10, 0)":
      case "DECIMAL(38, 9)":
      case "DECIMAL(16, 3)":
      case "NUMERIC":
        type = StandardSQLTypeName.NUMERIC; //
        break;
      case "DECIMAL(76, 38)":
        type = StandardSQLTypeName.BIGNUMERIC;
        break;
      case "STRING":
        type = StandardSQLTypeName.STRING; //
        break;
      case "BYTES":
        type = StandardSQLTypeName.BYTES; //
        break;
      case "STRUCT":
      case "ROW":
        type = StandardSQLTypeName.STRUCT; //
        break;
      case "ARRAY":
        type = StandardSQLTypeName.ARRAY;
        break;
      case "TIMESTAMP":
      case "TIMESTAMP(6)":
        type = StandardSQLTypeName.TIMESTAMP; //
        break;
      case "DATE":
        type = StandardSQLTypeName.DATE; //
        break;
      case "TIME":
      case "TIME(0)":
      case "TIME(0,32)":
        type = StandardSQLTypeName.TIME; // ######
        break;
      case "DATETIME":
        type = StandardSQLTypeName.DATETIME; //
        break;
      case "GEOGRAPHY":
        type = StandardSQLTypeName.GEOGRAPHY; //
        break;
      default:
        break;
    }
    return type;
  }
}
