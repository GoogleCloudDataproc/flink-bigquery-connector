package com.google.cloud.flink.bigquery.sink.Destination;

import com.google.api.services.bigquery.model.TableReference;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.api.services.bigquery.model.Table;
import org.checkerframework.checker.nullness.qual.Nullable;

public class BigQueryConstantTableDestination extends Destination {
    public Table getBigQueryTable(TableReference tableRef){

    }

    public TableDestination getTable(Destination destination){
        TableDestination wrappedDestination = getTable(destination);
        Table existingTable = getBigQueryTable(wrappedDestination.getTableReference());

        if (existingTable == null) {
            return wrappedDestination;
        } else {
            throw new RuntimeException("The specified table does not exist.");
        }
    }

    @Override
    public com.google.api.services.bigquery.model.TableSchema getSchema() {

        TableDestination wrappedDestination = this.getTable();
        @Nullable Table existingTable = getBigQueryTable(wrappedDestination.getTableReference());

        // If the table does not exist, or the schema of the existing table is null or empty.
        if (existingTable == null
                || existingTable.getSchema() == null
                || existingTable.getSchema().isEmpty()) {
            throw new RuntimeException("The specified table does not exist.");
        } else {
            return existingTable.getSchema();
        }

    }
}
