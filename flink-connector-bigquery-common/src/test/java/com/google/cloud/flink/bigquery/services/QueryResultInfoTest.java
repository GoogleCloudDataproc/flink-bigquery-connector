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

package com.google.cloud.flink.bigquery.services;

import com.google.common.truth.Truth8;
import org.junit.Test;

import java.util.Arrays;

import static com.google.common.truth.Truth.assertThat;

/** */
public class QueryResultInfoTest {

    @Test
    public void testQueryResultInfoFailed() {
        QueryResultInfo qri = QueryResultInfo.failed(Arrays.asList());
        assertThat(qri.getStatus()).isEqualTo(QueryResultInfo.Status.FAILED);
        Truth8.assertThat(qri.getDestinationProject()).isEmpty();
        Truth8.assertThat(qri.getDestinationDataset()).isEmpty();
        Truth8.assertThat(qri.getDestinationTable()).isEmpty();
        Truth8.assertThat(qri.getErrorMessages()).isPresent();
    }

    @Test
    public void testQueryResultInfoSucceeded() {
        QueryResultInfo qri = QueryResultInfo.succeed("", "", "");
        assertThat(qri.getStatus()).isEqualTo(QueryResultInfo.Status.SUCCEED);
        Truth8.assertThat(qri.getDestinationProject()).isPresent();
        Truth8.assertThat(qri.getDestinationDataset()).isPresent();
        Truth8.assertThat(qri.getDestinationTable()).isPresent();
        assertThat(qri.getErrorMessages().get()).isEmpty();
    }

    @Test
    public void testNotEquals() {
        QueryResultInfo succeed = QueryResultInfo.succeed("", "", "");
        QueryResultInfo failed = QueryResultInfo.failed(Arrays.asList());
        assertThat(succeed).isNotEqualTo(failed);
    }
}
