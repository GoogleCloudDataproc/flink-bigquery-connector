package com.google.cloud.flink.bigquery;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.bigquery.StandardSQLTypeName;

public class StandardSQLTypeHandler {
	
	private static final Logger log = LoggerFactory.getLogger(StandardSQLTypeHandler.class);

	public static StandardSQLTypeName handle(String columnStruct) {
		StandardSQLTypeName type = null;
		String sqlType = columnStruct.split(":")[1].toString();
		log.info("SQL TYPE NAME : " + sqlType);
		switch (sqlType) {
		case "BOOL":
			type = StandardSQLTypeName.BOOL;
			break;
		case "INT64":
			type = StandardSQLTypeName.INT64;
			break;
		case "FLOAT64":
			type = StandardSQLTypeName.FLOAT64;
			break;
		case "NUMERIC":
			type = StandardSQLTypeName.NUMERIC;
			break;
		case "BIGNUMERIC":
		case "BIGINT":
		case "INTEGER":
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
			type = StandardSQLTypeName.TIMESTAMP;
			break;
		case "DATE":
			type = StandardSQLTypeName.DATE;
			break;
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
