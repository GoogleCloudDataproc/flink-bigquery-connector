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

import java.util.HashMap;
import java.util.Map;

public class Configuration {

	private static String projectId = "";
	private static String dataset = "";
	private static String bigQueryReadTable = "";
	private static String filter = "";
	private static String credentialKeyFile = "";
	private static String credentialKeys = "";
	private static String selectedfields = "";
	private static int defaultParallelism = 5;
	private static String flinkVersion = "";
	private static int snapshotMillis = 0;
	private static Map<String, String> configMap = new HashMap<>();
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
		this.bigQueryWriteTable = targetBigQueryTable;
	}

	public void setSnapshotMillis(int snapshotMillis) {
		this.snapshotMillis = snapshotMillis;
	}

	public String getProjectId() {
		return projectId;
	}

	public void setProjectId(String projectId) {
		this.projectId = projectId;
	}

	public String getDataset() {
		return dataset;
	}

	public void setDataset(String dataset) {
		this.dataset = dataset;
	}

	public String getBigQueryReadTable() {
		return bigQueryReadTable;
	}

	public void setBigQueryReadTable(String bigQueryTable) {
		this.bigQueryReadTable = bigQueryTable;
	}

	public String getFilter() {
		return filter;
	}

	public void setFilter(String filter) {
		this.filter = filter;
	}

	public String getGcpCredentialKeyFile() {
		return credentialKeyFile;
	}

	public void setGcpCredentialKeyFile(String credentialKFile) {
		this.credentialKeyFile = credentialKFile;
	}

	public String getSelectedFields() {
		return selectedfields;
	}

	public void setSelectedFields(String fields) {
		this.selectedfields = fields;
	}

	public int getDefaultParallelism() {
		return defaultParallelism;
	}

	public void setDefaultParallelism(int defaultParallelism) {
		this.defaultParallelism = defaultParallelism;
	}

	public String getFlinkVersion() {
		return flinkVersion;
	}

	public void setFlinkVersion(String flinkVersion) {
		this.flinkVersion = flinkVersion;
	}

	public int getSnapshotMillis() {
		return snapshotMillis;
	}

	public String getPartitionRequired() {
		return partitionRequired;
	}

	public void setPartitionRequired(String partitionRequired) {
		this.partitionRequired = partitionRequired;
	}

	public String getPartitionField() {
		return partitionField;
	}

	public void setPartitionField(String partitionField) {
		this.partitionField = partitionField;
	}

	public String getBqEncodedCreateReadSessionRequest() {
		return bqEncodedCreateReadSessionRequest;
	}

	public void setBqEncodedCreateReadSessionRequest(String bqEncodedCreateReadSessionRequest) {
		this.bqEncodedCreateReadSessionRequest = bqEncodedCreateReadSessionRequest;
	}

	public int getBqBackgroundThreadsPerStream() {
		return bqBackgroundThreadsPerStream;
	}

	public void setBqBackgroundThreadsPerStream(int bqBackgroundThreadsPerStream) {
		this.bqBackgroundThreadsPerStream = bqBackgroundThreadsPerStream;
	}

	public String getQuery() {
		return query;
	}

	public void setQuery(String query) {
		this.query = query;
	}

	public String getConfigMap() {
		this.configString = this.configString + "targetBigQueryTable::" + bigQueryWriteTable + "#";
		this.configString = this.configString + "snapshotMillis::" + String.valueOf(snapshotMillis) + "#";
		this.configString = this.configString + "projectId::" + projectId + "#";
		this.configString = this.configString + "dataset::" + dataset + "#";
		this.configString = this.configString + "table::" + bigQueryReadTable + "#";
		this.configString = this.configString + "filter::" + filter + "#";
		this.configString = this.configString + "credentialKeyFile::" + credentialKeyFile + "#";
		this.configString = this.configString + "selectedfields::" + selectedfields + "#";
		this.configString = this.configString + "defaultParallelism::" + String.valueOf(defaultParallelism) + "#";
		this.configString = this.configString + "flinkVersion::" + flinkVersion + "#";
		this.configString = this.configString + "partitionRequired::" + partitionRequired + "#";
		this.configString = this.configString + "partitionField::" + partitionField + "#";
		this.configString = this.configString + "bqBackgroundThreadsPerStream::" + bqBackgroundThreadsPerStream + "#";
		this.configString = this.configString + "bqEncodedCreateReadSessionRequest::"
				+ bqEncodedCreateReadSessionRequest + "#";
		this.configString = this.configString + "query::" + query + "#";
		this.configString = this.configString + "path::" + path + "#";
		return this.configString.substring(0, this.configString.length() - 1);
	}
}
