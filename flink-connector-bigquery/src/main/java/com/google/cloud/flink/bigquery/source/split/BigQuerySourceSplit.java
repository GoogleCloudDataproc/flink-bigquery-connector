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

package com.google.cloud.flink.bigquery.source.split;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.connector.source.SourceSplit;

import java.io.Serializable;
import java.util.Objects;

/** A {@link SourceSplit} implementation for a BigQuery Read API stream. */
@PublicEvolving
public class BigQuerySourceSplit implements SourceSplit, Serializable {

    private final String streamName;
    private final Integer offset;

    public BigQuerySourceSplit(String streamName) {
        this.streamName = streamName;
        this.offset = 0;
    }

    public BigQuerySourceSplit(String streamName, Integer offset) {
        this.streamName = streamName;
        this.offset = offset;
    }

    @Override
    public String splitId() {
        return streamName;
    }

    public String getStreamName() {
        return streamName;
    }

    public Integer getOffset() {
        return offset;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 61 * hash + Objects.hashCode(this.streamName);
        hash = 61 * hash + Objects.hashCode(this.offset);
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
        final BigQuerySourceSplit other = (BigQuerySourceSplit) obj;
        if (!Objects.equals(this.streamName, other.streamName)) {
            return false;
        }
        return Objects.equals(this.offset, other.offset);
    }

    @Override
    public String toString() {
        return "BigQuerySourceSplit{" + "streamName=" + streamName + ", offset=" + offset + '}';
    }
}
