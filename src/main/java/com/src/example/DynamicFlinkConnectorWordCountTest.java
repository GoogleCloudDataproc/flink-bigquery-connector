package com.src.example;

import static org.apache.flink.table.api.Expressions.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


/**
 * 
 * It reads from Word count dataset
 *
 */
public class DynamicFlinkConnectorWordCountTest {

	public static void main(String[] args) throws Exception {
		String project_id = "q-gcp-6750-pso-gs-flink-22-01";
		String dataset="wordcount_dataset";
		String table="wordcount_output";
		Integer snapshotMillis= 0;
		if (args.length > 1) {
		     snapshotMillis = Integer.parseInt(args[1]);
		}
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1); // source only supports parallelism of 1
		final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
//		register a table in the catalog
		tEnv.executeSql("CREATE TABLE "+table+" (word STRING , word_count BIGINT)\n" 
				+ "WITH (\n"
				+ "  'connector' = 'bigquery',\n" 
				+ "  'format' = 'arrow',\n"
				+ "  'project_id' = '"+project_id+"',\n" 
				+ "  'dataset' = '"+dataset+"',\n" 
				+ "  'table' = '"+table+"',\n" 
				+ "  'snapshotMillis' = '"+snapshotMillis+"'\n" 
				+ ")");
//		final Table result = tEnv.from(table);
		final Table result = tEnv.sqlQuery("SELECT * FROM "+table);
//		print the result to the console
		Table datatable = result.where($("word_count").isGreaterOrEqual(100)).select($("word"), $("word_count"));
		TableResult tableapi = datatable.execute();
		tableapi.print();
	}
}