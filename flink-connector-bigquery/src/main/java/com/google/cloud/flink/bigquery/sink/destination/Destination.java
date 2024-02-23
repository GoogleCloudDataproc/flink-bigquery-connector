package com.google.cloud.flink.bigquery.sink.destination;

import com.google.api.services.bigquery.model.TableSchema;

import java.io.IOException;

/** javadoc. */
public abstract class Destination {

    public abstract TableSchema getSchema() throws IOException;

    public abstract com.google.cloud.bigquery.storage.v1.TableSchema getStorageTableSchema();
}
