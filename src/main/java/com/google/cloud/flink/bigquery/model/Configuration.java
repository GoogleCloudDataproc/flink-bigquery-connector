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
package com.google.cloud.flink.bigquery.model;

public class Configuration {

	private static String projectId = "";
	private static String dataset = "";
	private static String bigQueryReadTable = "";
	private static String filter = "";
	private static String credentialKeyFile = "";	
	private static String selectedfields = "";
	private static int defaultParallelism = 5;
	private static String flinkVersion = "";
	private static int snapshotMillis = 0;	
	private static String bigQueryWriteTable = "";
	private static String configString = "";
	private static String partitionRequired = ""; // 0 = true, 1 = false
	private static String partitionField = "";
	private static String bqEncodedCreateReadSessionRequest = "";
	private static int bqBackgroundThreadsPerStream = 1;
	private static String query;
	private static String path;
	// 0 = true, 1 = false

	public static String getPath() {
		return path;
	}

	public static void setPath(String path) {
		Configuration.path = path;
	}

	public String getBigQueryWriteTable() {
		return bigQueryWriteTable;
	}

	public void setBigQueryWriteTable(String targetBigQueryTable) {
		Configuration.bigQueryWriteTable = targetBigQueryTable;
	}

	public void setSnapshotMillis(int snapshotMillis) {
		Configuration.snapshotMillis = snapshotMillis;
	}

	public String getProjectId() {
		return projectId;
	}

	public void setProjectId(String projectId) {
		Configuration.projectId = projectId;
	}

	public String getDataset() {
		return dataset;
	}

	public void setDataset(String dataset) {
		Configuration.dataset = dataset;
	}

	public String getBigQueryReadTable() {
		return bigQueryReadTable;
	}

	public void setBigQueryReadTable(String bigQueryTable) {
		Configuration.bigQueryReadTable = bigQueryTable;
	}

	public String getFilter() {
		return filter;
	}	

	public String getGcpCredentialKeyFile() {
		return credentialKeyFile;
	}

	public void setGcpCredentialKeyFile(String credentialKFile) {
		Configuration.credentialKeyFile = credentialKFile;
	}

	public String getSelectedFields() {
		return selectedfields;
	}

	public void setSelectedFields(String fields) {
		Configuration.selectedfields = fields;
	}

	public int getDefaultParallelism() {
		return defaultParallelism;
	}

	public void setDefaultParallelism(int defaultParallelism) {
		Configuration.defaultParallelism = defaultParallelism;
	}

	public String getFlinkVersion() {
		return flinkVersion;
	}

	public void setFlinkVersion(String flinkVersion) {
		Configuration.flinkVersion = flinkVersion;
	}

	public int getSnapshotMillis() {
		return snapshotMillis;
	}

	public String getPartitionRequired() {
		return partitionRequired;
	}

	public void setPartitionRequired(String partitionRequired) {
		Configuration.partitionRequired = partitionRequired;
	}

	public String getPartitionField() {
		return partitionField;
	}

	public void setPartitionField(String partitionField) {
		Configuration.partitionField = partitionField;
	}

	public String getBqEncodedCreateReadSessionRequest() {
		return bqEncodedCreateReadSessionRequest;
	}

	public void setBqEncodedCreateReadSessionRequest(String bqEncodedCreateReadSessionRequest) {
		Configuration.bqEncodedCreateReadSessionRequest = bqEncodedCreateReadSessionRequest;
	}

	public int getBqBackgroundThreadsPerStream() {
		return bqBackgroundThreadsPerStream;
	}

	public void setBqBackgroundThreadsPerStream(int bqBackgroundThreadsPerStream) {
		Configuration.bqBackgroundThreadsPerStream = bqBackgroundThreadsPerStream;
	}

	public String getQuery() {
		return query;
	}

	public void setQuery(String query) {
		Configuration.query = query;
	}
	
	public void setFilter(String filter) {		
		Configuration.filter = filter;		
	}

	public String getConfigMap() {
		configString = configString + "targetBigQueryTable::" + bigQueryWriteTable + "#";
		configString = configString + "snapshotMillis::" + String.valueOf(snapshotMillis) + "#";
		configString = configString + "projectId::" + projectId + "#";
		configString = configString + "dataset::" + dataset + "#";
		configString = configString + "table::" + bigQueryReadTable + "#";
		configString = configString + "filter::" + filter + "#";
		configString = configString + "credentialKeyFile::" + credentialKeyFile + "#";
		configString = configString + "selectedfields::" + selectedfields + "#";
		configString = configString + "defaultParallelism::" + String.valueOf(defaultParallelism) + "#";
		configString = configString + "flinkVersion::" + flinkVersion + "#";
		configString = configString + "partitionRequired::" + partitionRequired + "#";
		configString = configString + "partitionField::" + partitionField + "#";
		configString = configString + "bqBackgroundThreadsPerStream::" + bqBackgroundThreadsPerStream + "#";
		configString = configString + "bqEncodedCreateReadSessionRequest::"
				+ bqEncodedCreateReadSessionRequest + "#";
		configString = configString + "query::" + query + "#";
		configString = configString + "path::" + path + "#";
		return configString.substring(0, configString.length() - 1);
	}

	
}
