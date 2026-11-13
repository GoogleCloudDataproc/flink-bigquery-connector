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

package com.google.cloud.flink.bigquery.sink.committer;

import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/** Tests for {@link BigQueryCommittableSerializer}. */
public class BigQueryCommittableSerializerTest {

    private static final BigQueryCommittableSerializer INSTANCE =
            new BigQueryCommittableSerializer();
    private static final BigQueryCommittable COMMITTABLE = new BigQueryCommittable(12, "foo", 1996);

    @Test
    public void testSerde() throws IOException {
        byte[] ser = INSTANCE.serialize(COMMITTABLE);
        BigQueryCommittable de = INSTANCE.deserialize(INSTANCE.getVersion(), ser);
        assertEquals(12, de.getProducerId());
        assertEquals("foo", de.getStreamName());
        assertEquals(1996, de.getStreamOffset());
    }
}
