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

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/** */
public class BigQuerySourceSplitSerializerTest {

    @Test
    public void testSplitSerializer() throws IOException {
        BigQuerySourceSplit split = new BigQuerySourceSplit("some stream name", 10L);

        byte[] serialized = BigQuerySourceSplitSerializer.INSTANCE.serialize(split);

        BigQuerySourceSplit split1 =
                BigQuerySourceSplitSerializer.INSTANCE.deserialize(
                        BigQuerySourceSplitSerializer.VERSION, serialized);

        Assert.assertEquals(split, split1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWrongSerializerVersion() throws IOException {
        BigQuerySourceSplit split = new BigQuerySourceSplit("some stream name", 10L);

        byte[] serialized = BigQuerySourceSplitSerializer.INSTANCE.serialize(split);

        BigQuerySourceSplitSerializer.INSTANCE.deserialize(1000, serialized);

        // should never reach here
        assert (true);
    }
}
