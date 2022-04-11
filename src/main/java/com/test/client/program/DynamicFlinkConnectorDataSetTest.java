package com.test.client.program;

import static org.apache.flink.table.api.Expressions.$;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


//import com.google.cloud.flink.bigquery.BigqueryQueryParser;

public final class DynamicFlinkConnectorDataSetTest  {

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(5);
		final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
		String flinkSrcTable1 = "FlinkSrcTable1";
		String flinkSrcTable2 = "FlinkSrcTable2";
		String bigqueryReadTable = "q-gcp-6750-pso-gs-flink-22-01.wordcount_dataset.wordcount_output";
	    String filter = "word_count > 500 and word=\"I\"";
	    String srcQueryString = "CREATE TABLE " + flinkSrcTable1 + " (word STRING , word_count BIGINT)";
	    //String srcQueryString = "CREATE TABLE " + flinkSrcTable + " (word STRING)";
	    tEnv.executeSql(
	        srcQueryString
	            + "\n"
	            + "WITH (\n"
	            + "  'connector' = 'bigquery',\n"
	            + "  'format' = 'arrow',\n"
	            + "  'table' = '"+ bigqueryReadTable+ "',\n"
	            //+ "  'selectedFields' = 'word,word_count',\n"
	            + "  'selectedFields' = 'word,word_count',\n"
	            //+ "  'filter' = '"+ filter+ "',\n"
	            + "  'maxParallelism' = '3',\n"
	            + "  'credentialsFile' = '" + System.getenv("GOOGLE_APPLICATION_CREDENTIALS")+ "' \n"
	            + ")");
		final Table sourceTable1 = tEnv.from(flinkSrcTable1);
		//Table datatable = sourceTable.select($("word"), $("word_count"));
		
		 String srcQueryString2 = "CREATE TABLE " + flinkSrcTable2 + " (word1 STRING)";
		    tEnv.executeSql(
		        srcQueryString2
		            + "\n"
		            + "WITH (\n"
		            + "  'connector' = 'bigquery',\n"
		            + "  'format' = 'arrow',\n"
		            + "  'table' = '"+ bigqueryReadTable+ "',\n"
		            + "  'selectedFields' = 'word',\n"
		            + "  'filter' = '"+ filter+ "',\n"
		            + "  'maxParallelism' = '10',\n"
		            + "  'credentialsFile' = '" + System.getenv("GOOGLE_APPLICATION_CREDENTIALS")+ "' \n"
		            + ")");
			final Table sourceTable2 = tEnv.from(flinkSrcTable2);
			Table resutTable = sourceTable1.join(sourceTable2).where($("word_count").isGreaterOrEqual(600)).select($("word"));
			TableResult tableapi = resutTable.execute();
			tableapi.print();
		
		//Table datatable = sourceTable.where($("word_count").isGreaterOrEqual(600)).select($("word"), $("word_count"));
		//TableResult tableapi = datatable.execute();
		//tableapi.print();
	}
}