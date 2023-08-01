/*
 * Copyright (C) 2023 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.flink.bigquery.services;

import org.apache.flink.annotation.Internal;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableList;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Represents the result information of a BigQuery query execution. */
@Internal
public class QueryResultInfo implements Serializable {

    /** The status of the BigQuery query execution. */
    public enum Status {
        SUCCEED,
        FAILED
    }

    private final Status status;
    private final List<String> errorMessages;
    private final String destinationProject;
    private final String destinationDataset;
    private final String destinationTable;

    private QueryResultInfo(Status status, List<String> errors) {
        this.status = status;
        this.errorMessages = errors;
        this.destinationProject = null;
        this.destinationDataset = null;
        this.destinationTable = null;
    }

    private QueryResultInfo(Status status, String project, String dataset, String table) {
        this.status = status;
        this.errorMessages = ImmutableList.of();
        this.destinationProject = project;
        this.destinationDataset = dataset;
        this.destinationTable = table;
    }

    /**
     * Creates a query result info for a failed query job.
     *
     * @param errors A list of error messages from the execution.
     * @return A query result info.
     */
    public static QueryResultInfo failed(List<String> errors) {
        return new QueryResultInfo(Status.FAILED, errors);
    }

    /**
     * Creates a query result info for a successful job.
     *
     * @param project the project for the destination table result.
     * @param dataset the dataset for the destination table result.
     * @param table the table for the destination table result.
     * @return A query result info.
     */
    public static QueryResultInfo succeed(String project, String dataset, String table) {
        return new QueryResultInfo(Status.SUCCEED, project, dataset, table);
    }

    public Status getStatus() {
        return status;
    }

    public Optional<List<String>> getErrorMessages() {
        return Optional.ofNullable(errorMessages);
    }

    public Optional<String> getDestinationProject() {
        return Optional.ofNullable(destinationProject);
    }

    public Optional<String> getDestinationDataset() {
        return Optional.ofNullable(destinationDataset);
    }

    public Optional<String> getDestinationTable() {
        return Optional.ofNullable(destinationTable);
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 97 * hash + Objects.hashCode(this.status);
        hash = 97 * hash + Objects.hashCode(this.errorMessages);
        hash = 97 * hash + Objects.hashCode(this.destinationProject);
        hash = 97 * hash + Objects.hashCode(this.destinationDataset);
        hash = 97 * hash + Objects.hashCode(this.destinationTable);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final QueryResultInfo other = (QueryResultInfo) obj;
        if (!Objects.equals(this.destinationProject, other.destinationProject)) {
            return false;
        }
        if (!Objects.equals(this.destinationDataset, other.destinationDataset)) {
            return false;
        }
        if (!Objects.equals(this.destinationTable, other.destinationTable)) {
            return false;
        }
        if (this.status != other.status) {
            return false;
        }
        return Objects.equals(this.errorMessages, other.errorMessages);
    }

    @Override
    public String toString() {
        return "QueryResultInfo{"
                + "status="
                + status
                + ", errorMessages="
                + errorMessages
                + ", destinationProject="
                + destinationProject
                + ", destinationDataset="
                + destinationDataset
                + ", destinationTable="
                + destinationTable
                + '}';
    }
}
