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

import org.junit.Test;

import static com.google.common.truth.Truth.assertThat;

/** */
public class BigQuerySourceSplitStateTest {

    @Test
    public void testSplitStateTransformation() {

        String streamName = "somestream";
        BigQuerySourceSplit originalSplit = new BigQuerySourceSplit(streamName, 10L);
        assertThat(originalSplit.splitId()).isEqualTo(streamName);

        BigQuerySourceSplitState splitState = new BigQuerySourceSplitState(originalSplit);
        assertThat(splitState.toBigQuerySourceSplit()).isEqualTo(originalSplit);
        assertThat(splitState)
                .isEqualTo(new BigQuerySourceSplitState(splitState.toBigQuerySourceSplit()));
    }

    @Test
    public void testSplitsEquals() {

        String streamName1 = "somestream";
        BigQuerySourceSplit split1 = new BigQuerySourceSplit(streamName1, 10L);
        String streamName2 = "somestream";
        BigQuerySourceSplit split2 = new BigQuerySourceSplit(streamName2, 10L);
        assertThat(split1).isEqualTo(split2);

        BigQuerySourceSplitState splitState1 = new BigQuerySourceSplitState(split1);
        BigQuerySourceSplitState splitState2 = new BigQuerySourceSplitState(split2);
        assertThat(splitState1).isEqualTo(splitState2);

        BigQuerySourceSplit split3 = new BigQuerySourceSplit(streamName2, 11L);
        assertThat(split1).isNotEqualTo(split3);

        BigQuerySourceSplitState splitState3 = new BigQuerySourceSplitState(split3);
        assertThat(splitState1).isNotEqualTo(splitState3);
    }

    @Test
    public void testSplitStateMutation() {

        String streamName = "somestream";
        BigQuerySourceSplit originalSplit = new BigQuerySourceSplit(streamName, 10L);
        BigQuerySourceSplitState splitState = new BigQuerySourceSplitState(originalSplit);

        splitState.updateOffset();
        BigQuerySourceSplit otherSplit = new BigQuerySourceSplit(streamName, 11L);

        assertThat(splitState.toBigQuerySourceSplit()).isEqualTo(otherSplit);
        assertThat(splitState.toBigQuerySourceSplit().hashCode()).isEqualTo(otherSplit.hashCode());
        // should be different since they started from different splits
        assertThat(splitState).isNotEqualTo(new BigQuerySourceSplitState(otherSplit));
        assertThat(splitState.hashCode())
                .isNotEqualTo(new BigQuerySourceSplitState(otherSplit).hashCode());
    }
}
