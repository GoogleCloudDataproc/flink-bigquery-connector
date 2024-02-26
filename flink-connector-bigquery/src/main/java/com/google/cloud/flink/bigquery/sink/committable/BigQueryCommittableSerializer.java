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

package com.google.cloud.flink.bigquery.sink.committable;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/** */
public class BigQueryCommittableSerializer
        implements SimpleVersionedSerializer<BigQueryCommittable> {

    private static final Logger LOG = LoggerFactory.getLogger(BigQueryCommittableSerializer.class);

    @Override
    public int getVersion() {
        return 1;
    }

    @Override
    public byte[] serialize(BigQueryCommittable committable) throws IOException {
        LOG.info(
                Thread.currentThread().getId()
                        + ": Call to BigQueryCommittableSerializer.serialize for committable {"
                        + committable
                        + "}");
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                final DataOutputStream out = new DataOutputStream(baos)) {
            out.writeLong(committable.getStreamOffset());
            out.writeUTF(committable.getStreamName());
            out.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public BigQueryCommittable deserialize(int version, byte[] serialized) throws IOException {
        LOG.info(
                Thread.currentThread().getId()
                        + ": Call to BigQueryCommittableSerializer.deserialize");
        try (final ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                final DataInputStream in = new DataInputStream(bais)) {
            final long streamOffset = in.readLong();
            final String streamName = in.readUTF();
            BigQueryCommittable committable = new BigQueryCommittable(streamOffset, streamName);
            LOG.info(
                    Thread.currentThread().getId()
                            + ": Deserialized committable {"
                            + committable
                            + "}");
            return committable;
        }
    }
}
