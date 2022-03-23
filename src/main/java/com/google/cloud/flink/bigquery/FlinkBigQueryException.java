package com.google.cloud.flink.bigquery;

import java.io.IOException;

public class FlinkBigQueryException extends RuntimeException {

  public FlinkBigQueryException(String message, Throwable error) {
    super(message, error);
  }

  public FlinkBigQueryException(String message) throws IOException {
    super(message);
  }
}
