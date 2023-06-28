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

import org.assertj.core.api.Assertions;
import org.junit.Test;

/** */
public class BigQuerySourceSplitStateTest {

    @Test
    public void testSplitStateTransformation() {

        String streamName = "somestream";
        BigQuerySourceSplit originalSplit = new BigQuerySourceSplit(streamName, 10);
        Assertions.assertThat(originalSplit.splitId()).isEqualTo(streamName);

        BigQuerySourceSplitState splitState = new BigQuerySourceSplitState(originalSplit);
        Assertions.assertThat(splitState.toBigQuerySourceSplit()).isEqualTo(originalSplit);
    }

    @Test
    public void testSplitStateMutation() {

        String streamName = "somestream";
        BigQuerySourceSplit originalSplit = new BigQuerySourceSplit(streamName, 10);
        BigQuerySourceSplitState splitState = new BigQuerySourceSplitState(originalSplit);

        splitState.updateOffset();
        BigQuerySourceSplit otherSplit = new BigQuerySourceSplit(streamName, 11);

        Assertions.assertThat(splitState.toBigQuerySourceSplit()).isEqualTo(otherSplit);
        Assertions.assertThat(splitState.toBigQuerySourceSplit().hashCode())
                .isEqualTo(otherSplit.hashCode());
        // should be different since they started from different splits
        Assertions.assertThat(splitState).isNotEqualTo(new BigQuerySourceSplitState(otherSplit));
        Assertions.assertThat(splitState.hashCode())
                .isNotEqualTo(new BigQuerySourceSplitState(otherSplit).hashCode());
    }
}
