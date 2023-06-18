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

package org.apache.flink.connector.bigquery.source.enumerator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.bigquery.common.utils.BigQueryStateSerde;
import org.apache.flink.connector.bigquery.source.split.BigQuerySourceSplit;
import org.apache.flink.connector.bigquery.source.split.BigQuerySourceSplitSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/** The {@link SimpleVersionedSerializer} for the enumerator state of BigQuery source. */
@Internal
public class BigQuerySourceEnumStateSerializer
        implements SimpleVersionedSerializer<BigQuerySourceEnumState> {
    public static final BigQuerySourceEnumStateSerializer INSTANCE =
            new BigQuerySourceEnumStateSerializer();

    private BigQuerySourceEnumStateSerializer() {
        // singleton instance
    }

    @Override
    public int getVersion() {
        return BigQuerySourceSplitSerializer.CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(BigQuerySourceEnumState state) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(baos)) {
            BigQueryStateSerde.serializeList(
                    out, state.getRemaniningTableStreams(), DataOutputStream::writeUTF);

            BigQueryStateSerde.serializeList(
                    out, state.getCompletedTableStreams(), DataOutputStream::writeUTF);

            BigQueryStateSerde.serializeList(
                    out,
                    state.getRemainingSourceSplits(),
                    BigQuerySourceSplitSerializer.INSTANCE::serializeBigQuerySourceSplit);

            BigQueryStateSerde.serializeMap(
                    out,
                    state.getAssignedSourceSplits(),
                    DataOutputStream::writeUTF,
                    BigQuerySourceSplitSerializer.INSTANCE::serializeBigQuerySourceSplit);

            out.writeBoolean(state.isInitialized());

            out.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public BigQuerySourceEnumState deserialize(int version, byte[] serialized) throws IOException {
        if (getVersion() != version) {
            throw new IllegalArgumentException(
                    String.format(
                            "The provided serializer version (%d) is not expected (expected : %s).",
                            version, getVersion()));
        }
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                DataInputStream in = new DataInputStream(bais)) {
            List<String> remainingTableStreams =
                    BigQueryStateSerde.deserializeList(in, DataInput::readUTF);
            List<String> completedTableStreams =
                    BigQueryStateSerde.deserializeList(in, DataInput::readUTF);
            List<BigQuerySourceSplit> remainingScanSplits =
                    BigQueryStateSerde.deserializeList(
                            in, i -> deserializeBigQuerySourceSplit(version, i));

            Map<String, BigQuerySourceSplit> assignedScanSplits =
                    BigQueryStateSerde.deserializeMap(
                            in,
                            DataInput::readUTF,
                            i -> deserializeBigQuerySourceSplit(version, i));

            boolean initialized = in.readBoolean();

            return new BigQuerySourceEnumState(
                    remainingTableStreams,
                    completedTableStreams,
                    remainingScanSplits,
                    assignedScanSplits,
                    initialized);
        }
    }

    private static BigQuerySourceSplit deserializeBigQuerySourceSplit(
            int version, DataInputStream in) throws IOException {
        return BigQuerySourceSplitSerializer.INSTANCE.deserializeBigQuerySourceSplit(version, in);
    }
}
