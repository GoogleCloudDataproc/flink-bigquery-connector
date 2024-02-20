package com.google.cloud.flink.bigquery.sink.Destination;

import com.google.api.services.bigquery.model.TableSchema;

public abstract class Destination {

    public abstract TableSchema getSchema();
}
