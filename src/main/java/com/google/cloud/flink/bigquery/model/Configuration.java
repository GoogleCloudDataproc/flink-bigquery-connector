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
	
	private String projectId;
	private String dataset;
	private String table;
	private String filter="";
	private String credentialKeyFile="";	
	private String selectedfields="";
	private int DEFAULT_PARALLELISM=5;
	private String FLINK_VERSION="";
	private int snapshotMillis=0;
	private Map<String,String> configMap = new HashMap<>(); 
	private String targetTable="";
	private String configString	="";
	private String partitionRequired=""; //0 = true, 1 = false
	private String partitionField="";	//0 = true, 1 = false
	public String getTargetTable() {
		return targetTable;
	}

	public void setTargetTable(String targetTable) {
		this.targetTable = targetTable;
		configMap.put("targetTable",targetTable);
	}

	public void setSnapshotMillis(int snapshotMillis) {
		this.snapshotMillis = snapshotMillis;
		configMap.put("snapshotMillis", String.valueOf(snapshotMillis));
	}
	
	public String getProjectId() {
		return projectId;
	}
	public void setProjectId(String projectId) {
		this.projectId = projectId;
		configMap.put("project_id", projectId);
	}
	public String getDataset() {
		return dataset;
	}
	public void setDataset(String dataset) {
		this.dataset = dataset;
		configMap.put("dataset", dataset);
	}
	public String getTable() {
		return table;
	}
	public void setTable(String table) {
		this.table = table;
		configMap.put("table", table);
	}
	public String getFilter() {
		return filter;
	}
	public void setFilter(String filter) {
		this.filter = filter;
		configMap.put("filter", filter);
	}
	public String getGcpCredentialKeyFile() {
		return credentialKeyFile;
	}
	public void setGcpCredentialKeyFile(String credentialKFile) {
		this.credentialKeyFile = credentialKFile;
		configMap.put("credentialKeyFile", credentialKFile);
	}
	public String getSelectedFields() {
		return selectedfields;
	}
	public void setSelectedFields(String fields) {
		this.selectedfields = fields;
		configMap.put("selectedfields", fields);
	}
	public int getDEFAULT_PARALLELISM() {
		return DEFAULT_PARALLELISM;
	}
	public void setDEFAULT_PARALLELISM(int dEFAULT_PARALLELISM) {
		DEFAULT_PARALLELISM = dEFAULT_PARALLELISM;
		configMap.put("DEFAULT_PARALLELISM", String.valueOf(dEFAULT_PARALLELISM));
	}

	public String getFLINK_VERSION() {
		return FLINK_VERSION;
	}
	public void setFLINK_VERSION(String fLINK_VERSION) {
		FLINK_VERSION = fLINK_VERSION;
		configMap.put("FLINK_VERSION", fLINK_VERSION);
	}
	public int getSnapshotMillis() {
		return snapshotMillis;
	}	
	
	public String getPartitionRequired() {
		return partitionRequired;
	}

	public void setPartitionRequired(String partitionRequired) {
		this.partitionRequired = partitionRequired;
		configMap.put("partitionRequired", partitionRequired);
	}

	public String getPartitionField() {
		return partitionField;
	}

	public void setPartitionField(String partitionField) {
		this.partitionField = partitionField;
		configMap.put("partitionField", partitionField);
	}
	
	public String getConfigMap() {
		this.configString=this.configString+"targetTable="+targetTable+"#";
		this.configString=this.configString+"snapshotMillis="+String.valueOf(snapshotMillis)+"#";
		this.configString=this.configString+"project_id="+projectId+"#";
		this.configString=this.configString+"dataset="+dataset+"#";
		this.configString=this.configString+"table="+table+"#";
		this.configString=this.configString+"filter="+filter+"#";
		this.configString=this.configString+"credentialKeyFile="+credentialKeyFile+"#";
		this.configString=this.configString+"selectedfields="+selectedfields+"#";
		this.configString=this.configString+"DEFAULT_PARALLELISM="+String.valueOf(DEFAULT_PARALLELISM)+"#";
		this.configString=this.configString+"FLINK_VERSION="+FLINK_VERSION+"#";
		this.configString=this.configString+"partitionRequired="+partitionRequired+"#";
		this.configString=this.configString+"partitionField="+partitionField+"#";
		return this.configString.substring(0, this.configString.length() -1);
	}
}
