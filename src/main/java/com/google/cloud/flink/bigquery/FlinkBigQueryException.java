package com.google.cloud.flink.bigquery;

public class FlinkBigQueryException extends RuntimeException {

  public FlinkBigQueryException(String message, Throwable error) {
    super(message, error);
  }

  public FlinkBigQueryException(String message) {
    super(message);
  }
}
