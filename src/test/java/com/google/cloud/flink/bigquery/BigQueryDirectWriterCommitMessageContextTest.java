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
        new BigQueryDirectWriterCommitMessageContext("Stream0", "project/dataset/table", 100);
  }

  @Test
  public void getWriteStreamNameTest() {
    String streamName = messageContext.getWriteStreamName();
    assertThat(streamName).isNotNull();
    assertThat(streamName).isEqualTo("Stream0");
    assertThat(streamName instanceof String).isTrue();
  }

  @Test
  public void getTablePathTest() {
    String tablePath = messageContext.getTablePath();
    assertThat(tablePath).isNotNull();
    assertThat(tablePath).isEqualTo("project/dataset/table");
    assertThat(tablePath instanceof String).isTrue();
  }

  @Test
  public void getRowCountTest() {
    Long rowCount = messageContext.getRowCount();
    assertThat(rowCount).isNotNull();
    assertThat(rowCount).isEqualTo(100);
    assertThat(rowCount instanceof Long).isTrue();
  }

  @Test
  public void toStringTest() {
    String messagecontextString = messageContext.toString();
    assertThat(messagecontextString).isNotNull();
    assertThat(messagecontextString)
        .isEqualTo("BigQueryWriterCommitMessage{, taskId=, tableId='project/dataset/table'}");
    assertThat(messagecontextString instanceof String).isTrue();
  }
}
