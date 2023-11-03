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

import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.flink.bigquery.common.utils.BigQueryPartition;
import org.assertj.core.api.Assertions;
import org.junit.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;

/** */
public class MetadataInfoTest {

    @Test
    public void testQueryResultInfoFailed() {
        QueryResultInfo qri = QueryResultInfo.failed(Arrays.asList());
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
    public void testQueryResultInfoNotEquals() {
        QueryResultInfo succeed = QueryResultInfo.succeed("", "", "");
        QueryResultInfo failed = QueryResultInfo.failed(Arrays.asList());
        Assertions.assertThat(succeed).isNotEqualTo(failed);
    }

    @Test
    public void testQueryResultInfoEquals() {
        QueryResultInfo succeed = QueryResultInfo.succeed("", "", "");
        QueryResultInfo another = QueryResultInfo.succeed("", "", "");
        Assertions.assertThat(succeed).isEqualTo(another);
        Assertions.assertThat(succeed.hashCode()).isEqualTo(another.hashCode());
    }

    @Test
    public void testTablePartitionInfoToPartitionsWithInfo() {
        TablePartitionInfo tpInfo =
                new TablePartitionInfo(
                        "name",
                        BigQueryPartition.PartitionType.MONTH,
                        StandardSQLTypeName.DATE,
                        Instant.now());
        List<String> partitions = Arrays.asList("202301", "202302");
        List<PartitionIdWithInfo> pIdInfo = tpInfo.toPartitionsWithInfo(partitions);
        Assertions.assertThat(pIdInfo).hasSize(2);
    }

    @Test
    public void testPartitionIdWithInfoEquality() {
        TablePartitionInfo info =
                new TablePartitionInfo(
                        "temporal",
                        BigQueryPartition.PartitionType.MONTH,
                        StandardSQLTypeName.INT64,
                        Instant.EPOCH);
        PartitionIdWithInfo original = new PartitionIdWithInfo("2023", info);
        PartitionIdWithInfo next = new PartitionIdWithInfo("2022", info);
        Assertions.assertThat(original).isNotEqualTo(next);
        Assertions.assertThat(original.hashCode()).isNotEqualTo(next.hashCode());

        PartitionIdWithInfo same = new PartitionIdWithInfo("2023", info);
        Assertions.assertThat(original).isEqualTo(same);
        Assertions.assertThat(original.hashCode()).isEqualTo(same.hashCode());
    }

    @Test
    public void testPartitionIdWithInfoAndEquality() {
        TablePartitionInfo info =
                new TablePartitionInfo(
                        "temporal",
                        BigQueryPartition.PartitionType.MONTH,
                        StandardSQLTypeName.INT64,
                        Instant.EPOCH);
        PartitionIdWithInfoAndStatus original =
                new PartitionIdWithInfoAndStatus(
                        "2023", info, BigQueryPartition.PartitionStatus.COMPLETED);
        PartitionIdWithInfoAndStatus next =
                new PartitionIdWithInfoAndStatus(
                        "2023", info, BigQueryPartition.PartitionStatus.IN_PROGRESS);
        Assertions.assertThat(original).isNotEqualTo(next);
        Assertions.assertThat(original.hashCode()).isNotEqualTo(next.hashCode());

        PartitionIdWithInfoAndStatus same =
                new PartitionIdWithInfoAndStatus(
                        "2023", info, BigQueryPartition.PartitionStatus.COMPLETED);
        Assertions.assertThat(original).isEqualTo(same);
        Assertions.assertThat(original.hashCode()).isEqualTo(same.hashCode());
    }
}
