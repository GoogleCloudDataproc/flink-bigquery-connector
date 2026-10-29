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

import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/** Tests for {@link BigQueryWriterStateSerializer}. */
public class BigQueryWriterStateSerializerTest {

    private static final BigQueryWriterStateSerializer INSTANCE =
            new BigQueryWriterStateSerializer();
    private static final BigQueryWriterState STATE =
            new BigQueryWriterState("foo", 1996, 24000, 23000, 1996, 4);

    @Test
    public void testSerde() throws IOException {
        byte[] ser = INSTANCE.serialize(STATE);
        BigQueryWriterState de = INSTANCE.deserialize(INSTANCE.getVersion(), ser);
        assertEquals("foo", de.getStreamName());
        assertEquals(1996, de.getStreamOffset());
        assertEquals(24000, de.getTotalRecordsSeen());
        assertEquals(23000, de.getTotalRecordsWritten());
        assertEquals(1996, de.getTotalRecordsCommitted());
        assertEquals(4, de.getCheckpointId());
    }
}
