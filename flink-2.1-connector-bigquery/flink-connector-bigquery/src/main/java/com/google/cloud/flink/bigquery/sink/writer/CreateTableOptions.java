/*
 * Copyright 2024 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.flink.bigquery.sink.writer;

import com.google.cloud.bigquery.TimePartitioning;

import java.util.List;

/** Options for creating new BigQuery table. */
public class CreateTableOptions {

    private final boolean enableTableCreation;
    private final String partitionField;
    private final TimePartitioning.Type partitionType;
    private final long partitionExpirationMillis;
    private final List<String> clusteredFields;
    private final String region;

    public CreateTableOptions(
            boolean enableTableCreation,
            String partitionField,
            TimePartitioning.Type partitionType,
            Long partitionExpirationMillis,
            List<String> clusteredFields,
            String region) {
        this.enableTableCreation = enableTableCreation;
        this.partitionField = partitionField;
        this.partitionType = partitionType;
        if (partitionExpirationMillis == null) {
            this.partitionExpirationMillis = 0;
        } else {
            this.partitionExpirationMillis = partitionExpirationMillis;
        }
        this.clusteredFields = clusteredFields;
        this.region = region;
    }

    public boolean enableTableCreation() {
        return enableTableCreation;
    }

    public String getPartitionField() {
        return partitionField;
    }

    public TimePartitioning.Type getPartitionType() {
        return partitionType;
    }

    public long getPartitionExpirationMillis() {
        return partitionExpirationMillis;
    }

    public List<String> getClusteredFields() {
        return clusteredFields;
    }

    public String getRegion() {
        return region;
    }
}
