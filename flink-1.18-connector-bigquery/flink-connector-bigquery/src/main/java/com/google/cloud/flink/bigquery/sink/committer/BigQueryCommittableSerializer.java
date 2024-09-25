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

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/** Serializer and deserializer for {@link BigQueryCommittable}. */
public class BigQueryCommittableSerializer
        implements SimpleVersionedSerializer<BigQueryCommittable> {

    @Override
    public int getVersion() {
        return 1;
    }

    @Override
    public byte[] serialize(BigQueryCommittable committable) throws IOException {
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                final DataOutputStream out = new DataOutputStream(baos)) {
            out.writeLong(committable.getProducerId());
            out.writeUTF(committable.getStreamName());
            out.writeLong(committable.getStreamOffset());
            out.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public BigQueryCommittable deserialize(int version, byte[] serialized) throws IOException {
        try (final ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                final DataInputStream in = new DataInputStream(bais)) {
            final Long producerId = in.readLong();
            final String streamName = in.readUTF();
            final long streamOffset = in.readLong();
            BigQueryCommittable committable =
                    new BigQueryCommittable(producerId, streamName, streamOffset);
            return committable;
        }
    }
}
