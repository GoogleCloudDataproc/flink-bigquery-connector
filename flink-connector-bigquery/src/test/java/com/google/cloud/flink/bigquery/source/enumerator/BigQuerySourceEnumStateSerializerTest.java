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

package com.google.cloud.flink.bigquery.source.enumerator;

import com.google.cloud.flink.bigquery.source.split.BigQuerySourceSplit;
import com.google.cloud.flink.bigquery.source.split.BigQuerySourceSplitSerializer;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/** */
public class BigQuerySourceEnumStateSerializerTest {

    private BigQuerySourceEnumState create() {

        List<String> partitions = new ArrayList<>();

        partitions.add("20230801");

        List<String> remainingTableStreams = new ArrayList<>();

        remainingTableStreams.add("third stream");
        remainingTableStreams.add("fourth stream");
        remainingTableStreams.add("fifth stream");

        List<String> completedTableStreams = new ArrayList<>();
        completedTableStreams.add("first stream");

        List<BigQuerySourceSplit> remainingSourceSplits = new ArrayList<>();
        remainingSourceSplits.add(new BigQuerySourceSplit("second stream", 0L));

        Map<String, BigQuerySourceSplit> assignedSourceSplits = new TreeMap<>();
        assignedSourceSplits.put("key1", remainingSourceSplits.get(0));

        return new BigQuerySourceEnumState(
                partitions,
                remainingTableStreams,
                completedTableStreams,
                remainingSourceSplits,
                assignedSourceSplits,
                true);
    }

    @Test
    public void testEnumStateSerializerInitialState() throws IOException {
        BigQuerySourceEnumState initialState = BigQuerySourceEnumState.initialState();

        byte[] serialized = BigQuerySourceEnumStateSerializer.INSTANCE.serialize(initialState);

        BigQuerySourceEnumState enumState1 =
                BigQuerySourceEnumStateSerializer.INSTANCE.deserialize(
                        BigQuerySourceSplitSerializer.VERSION, serialized);

        Assert.assertEquals(initialState, enumState1);
        Assert.assertEquals(initialState.hashCode(), enumState1.hashCode());
    }

    @Test
    public void testEnumStateSerializer() throws IOException {
        BigQuerySourceEnumState enumState = create();

        byte[] serialized = BigQuerySourceEnumStateSerializer.INSTANCE.serialize(enumState);

        BigQuerySourceEnumState enumState1 =
                BigQuerySourceEnumStateSerializer.INSTANCE.deserialize(
                        BigQuerySourceSplitSerializer.VERSION, serialized);

        Assert.assertEquals(enumState, enumState1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWrongSerializerVersion() throws IOException {
        BigQuerySourceEnumState enumState = create();

        byte[] serialized = BigQuerySourceEnumStateSerializer.INSTANCE.serialize(enumState);

        BigQuerySourceEnumStateSerializer.INSTANCE.deserialize(1000, serialized);

        // should never reach here
        Assert.fail();
    }
}
