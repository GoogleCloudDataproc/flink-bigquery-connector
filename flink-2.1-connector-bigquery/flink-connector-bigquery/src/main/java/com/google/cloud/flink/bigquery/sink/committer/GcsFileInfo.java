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

package com.google.cloud.flink.bigquery.sink.committer;

import java.io.Serializable;
import java.util.Objects;

/** A GCS file URI paired with its size in bytes. */
class GcsFileInfo implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String uri;
    private final long sizeInBytes;

    public GcsFileInfo(String uri, long sizeInBytes) {
        this.uri = uri;
        this.sizeInBytes = sizeInBytes;
    }

    public String getUri() {
        return uri;
    }

    public long getSizeInBytes() {
        return sizeInBytes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GcsFileInfo that = (GcsFileInfo) o;
        return sizeInBytes == that.sizeInBytes && Objects.equals(uri, that.uri);
    }

    @Override
    public int hashCode() {
        return Objects.hash(uri, sizeInBytes);
    }

    @Override
    public String toString() {
        return uri + " (" + sizeInBytes + " bytes)";
    }
}
