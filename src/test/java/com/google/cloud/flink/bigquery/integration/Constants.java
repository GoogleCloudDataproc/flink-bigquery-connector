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
import org.apache.flink.table.types.DataType;

public class Constants {

	static final int BQ_NUMERIC_PRECISION = 38;
	static final int BQ_NUMERIC_SCALE = 9;
	static final DataType NUMERIC_SPARK_TYPE = DataTypes.DECIMAL(BQ_NUMERIC_PRECISION, BQ_NUMERIC_SCALE);// .createDecimalType(BQ_NUMERIC_PRECISION,
																											// BQ_NUMERIC_SCALE);
	static final String SHAKESPEARE_TABLE_DATASET = "bigquery-public-data.samples.shakespeare";
	static final String table = "wordcount_output";
	static final String projectId = "q-gcp-6750-pso-gs-flink-22-01";
	static final String dataset = "wordcount_dataset";
	static final int snapshotMillis = 0;
	static final long SHAKESPEARE_TABLE_NUM_ROWS = 164656L;
	static final String NON_EXISTENT_TABLE = "non-existent.non-existent.non-existent";

	static final String LARGE_TABLE_FIELD = "is_male";
	static final String LARGE_TABLE_PROJECT_ID = "bigquery-public-data";
	static final String LARGE_TABLE_DATASET = "samples";
	static final String LARGE_TABLE = "natality";
	static final TableSchema WORDCOUNT_TABLE_SCHEMA = new TableSchema.Builder().field("word", DataTypes.STRING())
			.field("word_count", DataTypes.BIGINT()).build();

	static final TableSchema FLINK_TEST_TABLE_SCHEMA = new TableSchema.Builder()
			.field("string_datatype", DataTypes.STRING()).field("bytes_datatype", DataTypes.BYTES())
			.field("integer_datatype", DataTypes.INT()).field("float_datatype", DataTypes.FLOAT())
			.field("boolean_datatype", DataTypes.BOOLEAN()).build();
}
