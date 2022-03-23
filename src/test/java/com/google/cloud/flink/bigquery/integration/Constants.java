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
package com.google.cloud.flink.bigquery.integration;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;

public class Constants {	
	
	static final String LARGE_TABLE_FIELD = "is_male";
	static final String LARGE_TABLE_PROJECT_ID = "bigquery-public-data";
	static final String LARGE_TABLE_DATASET = "samples";
	static final String NON_EXISTENT_TABLE = "nonexistent";
	static final String LARGE_TABLE = "natality";
	static final TableSchema WORDCOUNT_TABLE_SCHEMA = new TableSchema.Builder().field("word", DataTypes.STRING())
			.field("word_count", DataTypes.BIGINT()).build();	

	static final TableSchema FLINK_TEST_TABLE_SCHEMA = new TableSchema.Builder()
			.field("string_datatype", DataTypes.STRING()).field("bytes_datatype", DataTypes.BYTES())
			.field("integer_datatype", DataTypes.INT()).field("float_datatype", DataTypes.FLOAT())
			.field("boolean_datatype", DataTypes.BOOLEAN()).build();	
	
	static final TableSchema LARGE_TABLE_SCHEMA = new TableSchema.Builder()
			.field("is_male", DataTypes.BOOLEAN()).build();
}
