/*
 * Copyright (C) 2024 Google Inc.
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

package com.google.cloud.flink.bigquery.sink.serializer;

import java.io.Serializable;

/**
 * Functional interface for determining the CDC change type for a record.
 *
 * <p>This interface allows users to provide custom logic for determining whether a record
 * represents an UPSERT or DELETE operation in BigQuery's CDC mode.
 *
 * <p>BigQuery CDC supports two change types:
 *
 * <ul>
 *   <li>{@code "UPSERT"} - Insert or update a row based on the primary key
 *   <li>{@code "DELETE"} - Delete a row with the matching primary key
 * </ul>
 *
 * @param <IN> Type of records being written to BigQuery.
 */
@FunctionalInterface
public interface CdcChangeTypeProvider<IN> extends Serializable {

    /**
     * Determines the CDC change type for the given record.
     *
     * @param record The record to determine the change type for.
     * @return The change type string, either "UPSERT" or "DELETE".
     */
    String getChangeType(IN record);

    /**
     * Returns a provider that always returns "UPSERT" for all records.
     *
     * @param <IN> Type of records.
     * @return A CdcChangeTypeProvider that always returns "UPSERT".
     */
    static <IN> CdcChangeTypeProvider<IN> upsertOnly() {
        return record -> "UPSERT";
    }
}
