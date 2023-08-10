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

package com.google.cloud.flink.bigquery.common.utils;

import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;
import org.apache.flink.shaded.guava30.com.google.common.collect.Maps;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import static com.google.common.truth.Truth.assertThat;

/** */
public class BigQueryStateSerdeTest {

    @Test
    public void testListSerDe() throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(baos)) {

            List<String> original = Lists.newArrayList("first", "second", "third", "fourth");
            BigQueryStateSerde.serializeList(out, original, DataOutputStream::writeUTF);
            out.flush();
            byte[] serialized = baos.toByteArray();

            try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                    DataInputStream in = new DataInputStream(bais)) {

                List<String> deserialized =
                        BigQueryStateSerde.deserializeList(in, DataInput::readUTF);

                assertThat(original).isEqualTo(deserialized);
            }
        }
    }

    @Test
    public void testMapSerDe() throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(baos)) {

            Map<String, String> original = Maps.newHashMap();
            original.put("key1", "value1");
            original.put("key2", "value2");
            original.put("key3", "value3");
            BigQueryStateSerde.serializeMap(
                    out, original, DataOutputStream::writeUTF, DataOutputStream::writeUTF);
            out.flush();
            byte[] serialized = baos.toByteArray();

            try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                    DataInputStream in = new DataInputStream(bais)) {

                Map<String, String> deserialized =
                        BigQueryStateSerde.deserializeMap(
                                in, DataInput::readUTF, DataInput::readUTF);

                assertThat(original).isEqualTo(deserialized);
            }
        }
    }
}
