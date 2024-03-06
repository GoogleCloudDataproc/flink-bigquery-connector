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

import org.apache.flink.core.io.SimpleVersionedSerializer;

import com.google.cloud.flink.bigquery.common.utils.flink.annotations.Internal;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/** The {@link SimpleVersionedSerializer serializer} for {@link BigQuerySourceSplit}. */
@Internal
public class BigQuerySourceSplitSerializer
        implements SimpleVersionedSerializer<BigQuerySourceSplit> {

    public static final BigQuerySourceSplitSerializer INSTANCE =
            new BigQuerySourceSplitSerializer();
    // This version should be bumped after modifying the source split or the enum states.
    public static final int VERSION = 0;

    private BigQuerySourceSplitSerializer() {
        // singleton instance
    }

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(BigQuerySourceSplit obj) throws IOException {
        // VERSION 0 serialization
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(baos)) {
            serializeBigQuerySourceSplit(out, obj);
            out.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public BigQuerySourceSplit deserialize(int version, byte[] serialized) throws IOException {
        if (getVersion() != version) {
            throw new IllegalArgumentException(
                    String.format(
                            "The provided serializer version (%d) is not expected (expected : %s).",
                            version, VERSION));
        }
        // VERSION 0 deserialization
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                DataInputStream in = new DataInputStream(bais)) {
            return deserializeBigQuerySourceSplit(version, in);
        }
    }

    public void serializeBigQuerySourceSplit(DataOutputStream out, BigQuerySourceSplit split)
            throws IOException {
        out.writeUTF(split.getStreamName());
        out.writeLong(split.getOffset());
    }

    public BigQuerySourceSplit deserializeBigQuerySourceSplit(int version, DataInputStream in)
            throws IOException {
        switch (version) {
            case VERSION:
                String streamName = in.readUTF();
                long offset = in.readLong();
                return new BigQuerySourceSplit(streamName, offset);
            default:
                throw new IOException("Unknown version: " + version);
        }
    }
}
