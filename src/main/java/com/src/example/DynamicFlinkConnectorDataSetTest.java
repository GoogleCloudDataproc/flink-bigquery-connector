package com.src.example;

import static org.apache.flink.table.api.Expressions.$;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


/**
 * 
 * It reads from Sample table for Test dataset
 *
 */
public final class DynamicFlinkConnectorDataSetTest {

	public static void main(String[] args) throws Exception {
		String project_id = "q-gcp-6750-pso-gs-flink-22-01";
		String dataset="test";
		String table="sample";
		Integer snapshotMillis= 0;
		if (args.length > 1) {
		     snapshotMillis = Integer.parseInt(args[1]);
		}
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1); // source only supports parallelism of 1
		final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
//		register a table in the catalog
		tEnv.executeSql("CREATE TABLE "+table+" (id BIGINT, name STRING)\n" 
				+ "WITH (\n"
				+ "  'connector' = 'bigquery',\n" 
				+ "  'format' = 'arrow',\n"
				+ "  'project_id' = '"+project_id+"',\n" 
				+ "  'dataset' = '"+dataset+"',\n" 
				+ "  'table' = '"+table+"',\n" 
				+ "  'snapshotMillis' = '"+snapshotMillis+"'\n" 
				+ ")");
		final Table result = tEnv.from(table);
//		Table datatable = result.select($("name"));
		Table datatable = result.select($("id"), $("name"));
//		Table datatable = result.where($("id").isGreaterOrEqual(1)).select($("id"), $("name"));
		TableResult tableapi = datatable.execute();
		tableapi.print();
	}
}