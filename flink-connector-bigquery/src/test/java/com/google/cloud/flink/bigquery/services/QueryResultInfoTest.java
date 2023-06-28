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

import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;

import org.assertj.core.api.Assertions;
import org.junit.Test;

/** */
public class QueryResultInfoTest {

    @Test
    public void testQueryResultInfoFailed() {
        QueryResultInfo qri = QueryResultInfo.failed(Lists.newArrayList());
        Assertions.assertThat(qri.getStatus()).isEqualTo(QueryResultInfo.Status.FAILED);
        Assertions.assertThat(qri.getDestinationProject()).isEmpty();
        Assertions.assertThat(qri.getDestinationDataset()).isEmpty();
        Assertions.assertThat(qri.getDestinationTable()).isEmpty();
        Assertions.assertThat(qri.getErrorMessages()).isNotEmpty();
    }

    @Test
    public void testQueryResultInfoSucceeded() {
        QueryResultInfo qri = QueryResultInfo.succeed("", "", "");
        Assertions.assertThat(qri.getStatus()).isEqualTo(QueryResultInfo.Status.SUCCEED);
        Assertions.assertThat(qri.getDestinationProject()).isNotEmpty();
        Assertions.assertThat(qri.getDestinationDataset()).isNotEmpty();
        Assertions.assertThat(qri.getDestinationTable()).isNotEmpty();
        Assertions.assertThat(qri.getErrorMessages().get()).isEmpty();
    }

    @Test
    public void testNotEquals() {
        QueryResultInfo succeed = QueryResultInfo.succeed("", "", "");
        QueryResultInfo failed = QueryResultInfo.failed(Lists.newArrayList());
        Assertions.assertThat(succeed).isNotEqualTo(failed);
    }

    @Test
    public void testEquals() {
        QueryResultInfo succeed = QueryResultInfo.succeed("", "", "");
        QueryResultInfo another = QueryResultInfo.succeed("", "", "");
        Assertions.assertThat(succeed).isEqualTo(another);
        Assertions.assertThat(succeed.hashCode()).isEqualTo(another.hashCode());
    }
}
