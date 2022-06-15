/*
 * Copyright 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.flink.bigquery;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.flink.bigquery.common.BigQueryDirectWriterCommitMessageContext;
import org.junit.Before;
import org.junit.Test;

public class BigQueryDirectWriterCommitMessageContextTest {
  BigQueryDirectWriterCommitMessageContext messageContext;

  @Before
  public void setup() {
    messageContext =
        new BigQueryDirectWriterCommitMessageContext(
            "Stream0", "project/dataset/table", 100L, 1, Boolean.TRUE);
  }

  @Test
  public void getWriteStreamNameTest() {
    String streamName = messageContext.getWriteStreamName();
    assertThat(streamName).isNotNull();
    assertThat(streamName).isEqualTo("Stream0");
  }

  @Test
  public void getTablePathTest() {
    String tablePath = messageContext.getTablePath();
    assertThat(tablePath).isNotNull();
    assertThat(tablePath).isEqualTo("project/dataset/table");
  }

  @Test
  public void getRowCountTest() {
    Long rowCount = messageContext.getRowCount();
    assertThat(rowCount).isNotNull();
    assertThat(rowCount).isEqualTo(100);
  }

  @Test
  public void toStringTest() {
    String messagecontextString = messageContext.toString();
    assertThat(messagecontextString).isNotNull();
    assertThat(messagecontextString)
        .isEqualTo(
            "BigQueryWriterCommitMessage{taskId=1, tableId='project/dataset/table', rowCount='100, writeStreamName='Stream0}");
  }
}
