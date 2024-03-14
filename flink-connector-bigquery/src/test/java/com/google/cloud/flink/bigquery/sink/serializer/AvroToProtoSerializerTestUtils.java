package com.google.cloud.flink.bigquery.sink.serializer;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.protobuf.Descriptors.Descriptor;
import org.apache.avro.Schema;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/** Common Utils for {@link AvroToProtoSerializerTest} and {@link BigQuerySchemaProviderTest}. */
public class AvroToProtoSerializerTestUtils {

    public static Schema getAvroSchemaFromFieldString(String fieldString) {
        String avroSchemaString =
                "{\"namespace\": \"project.dataset\",\n"
                        + " \"type\": \"record\",\n"
                        + " \"name\": \"table\",\n"
                        + " \"doc\": \"Translated Avro Schema for project.dataset.table\",\n"
                        + fieldString
                        + "}";

        return new Schema.Parser().parse(avroSchemaString);
    }

    public static String getRecord(String name) {
        return "{\"name\": "
                + "\""
                + name
                + "\", "
                + "\"type\": \"record\", "
                + "\"fields\": "
                + "["
                + "{\"name\": \"value\", \"type\": \"long\"},"
                + "{\"name\": \"another_value\",\"type\": \"string\"}"
                + "]"
                + "}";
    }

    private static BigQueryAvroToProtoSerializerTestResult getAndReturn(String fieldString) {
        Schema avroSchema = getAvroSchemaFromFieldString(fieldString);
        BigQuerySchemaProvider bigQuerySchemaProvider = new BigQuerySchemaProvider(avroSchema);
        Descriptor descriptor = bigQuerySchemaProvider.getDescriptor();
        return new BigQueryAvroToProtoSerializerTestResult(avroSchema, descriptor);
    }

    public static BigQueryAvroToProtoSerializerTestResult testPrimitiveTypesConversion() {
        List<TableFieldSchema> subFieldsNullable =
                Collections.singletonList(
                        new TableFieldSchema()
                                .setName("species")
                                .setType("STRING")
                                .setMode("REQUIRED"));
        List<TableFieldSchema> fields =
                Arrays.asList(
                        new TableFieldSchema()
                                .setName("number")
                                .setType("INTEGER")
                                .setMode("REQUIRED"),
                        new TableFieldSchema()
                                .setName("price")
                                .setType("FLOAT")
                                .setMode("REQUIRED"),
                        new TableFieldSchema()
                                .setName("species")
                                .setType("STRING")
                                .setMode("REQUIRED"),
                        new TableFieldSchema()
                                .setName("flighted")
                                .setType("BOOLEAN")
                                .setMode("REQUIRED"),
                        new TableFieldSchema()
                                .setName("sound")
                                .setType("BYTES")
                                .setMode("REQUIRED"),
                        new TableFieldSchema()
                                .setName("required_record_field")
                                .setType("RECORD")
                                .setMode("REQUIRED")
                                .setFields(subFieldsNullable));
        TableSchema tableSchema = new TableSchema().setFields(fields);
        BigQuerySchemaProvider bigQuerySchemaProvider = new BigQuerySchemaProvider(tableSchema);
        Descriptor descriptor = bigQuerySchemaProvider.getDescriptor();
        return new BigQueryAvroToProtoSerializerTestResult(
                bigQuerySchemaProvider.getSchema(), descriptor);
    }

    public static BigQueryAvroToProtoSerializerTestResult testLogicalTypesConversion() {
        List<TableFieldSchema> fields =
                Arrays.asList(
                        new TableFieldSchema()
                                .setName("timestamp")
                                .setType("TIMESTAMP")
                                .setMode("NULLABLE"),
                        new TableFieldSchema()
                                .setName("numeric_field")
                                .setType("NUMERIC")
                                .setMode("REQUIRED"),
                        new TableFieldSchema()
                                .setName("bignumeric_field")
                                .setType("BIGNUMERIC")
                                .setMode("NULLABLE"),
                        new TableFieldSchema()
                                .setName("geography")
                                .setType("GEOGRAPHY")
                                .setMode("REQUIRED"),
                        new TableFieldSchema().setName("Json").setType("JSON").setMode("REQUIRED"));

        TableSchema tableSchema = new TableSchema().setFields(fields);
        BigQuerySchemaProvider bigQuerySchemaProvider = new BigQuerySchemaProvider(tableSchema);
        Descriptor descriptor = bigQuerySchemaProvider.getDescriptor();
        return new BigQueryAvroToProtoSerializerTestResult(
                bigQuerySchemaProvider.getSchema(), descriptor);
    }

    public static BigQueryAvroToProtoSerializerTestResult testAllPrimitiveSchemaConversion() {
        String fieldString =
                " \"fields\": [\n"
                        + "   {\"name\": \"name\", \"type\": \"string\"},\n"
                        + "   {\"name\": \"number\", \"type\": \"long\"},\n"
                        + "   {\"name\": \"quantity\", \"type\": \"int\"},\n"
                        + "   {\"name\": \"fixed_field\", \"type\": {\"type\": "
                        + "\"fixed\", \"size\": 10,\"name\": \"hash\" }},\n"
                        + "   {\"name\": \"price\", \"type\": \"float\"},\n"
                        + "   {\"name\": \"double_field\", \"type\": \"double\"},\n"
                        + "   {\"name\": \"boolean_field\", \"type\": \"boolean\"},\n"
                        + "   {\"name\": \"enum_field\", \"type\": {\"type\":\"enum\","
                        + " \"symbols\": [\"A\", \"B\", \"C\", \"D\"], \"name\": \"ALPHABET\"}},\n"
                        + "   {\"name\": \"byte_field\", \"type\": \"bytes\"}\n"
                        + " ]\n";
        return getAndReturn(fieldString);
    }

    public static BigQueryAvroToProtoSerializerTestResult testAllLogicalSchemaConversion() {

        String fieldString =
                " \"fields\": [\n"
                        + "   {\"name\": \"ts_micros\", \"type\": {\"type\": \"long\", \"logicalType\": \"timestamp-micros\"}},\n"
                        + "   {\"name\": \"ts_millis\", \"type\": {\"type\": \"long\", \"logicalType\": \"timestamp-millis\"}},\n"
                        + "   {\"name\": \"time_micros\", \"type\": {\"type\": \"long\", \"logicalType\": \"time-micros\"}},\n"
                        + "   {\"name\": \"time_millis\", \"type\": {\"type\": \"int\", \"logicalType\": \"time-millis\"}},\n"
                        + "   {\"name\": \"lts_micros\", \"type\": {\"type\": \"long\", \"logicalType\": \"local-timestamp-micros\"}},\n"
                        + "   {\"name\": \"lts_millis\", \"type\": {\"type\": \"long\", \"logicalType\": \"local-timestamp-millis\"}},\n"
                        + "   {\"name\": \"date\", \"type\": {\"type\": \"int\", \"logicalType\": \"date\"}},\n"
                        + "   {\"name\": \"decimal\", \"type\": {\"type\": \"bytes\", \"logicalType\": \"decimal\", \"precision\": 4, \"scale\": 2}},\n"
                        + "   {\"name\": \"uuid\", \"type\": {\"type\": \"string\", \"logicalType\": \"uuid\"}},\n"
                        + "   {\"name\": \"geography\", \"type\": {\"type\": \"string\", \"logicalType\": \"geography_wkt\"}}\n"
                        + " ]\n";
        return getAndReturn(fieldString);
    }

    public static BigQueryAvroToProtoSerializerTestResult testAllUnionLogicalSchemaConversion() {
        String fieldString =
                " \"fields\": [\n"
                        + "   {\"name\": \"ts_micros\", \"type\": [\"null\", {\"type\": \"long\", \"logicalType\": \"timestamp-micros\"}]},\n"
                        + "   {\"name\": \"ts_millis\", \"type\": [\"null\",{\"type\": \"long\", \"logicalType\": \"timestamp-millis\"}]},\n"
                        + "   {\"name\": \"time_micros\", \"type\": [\"null\",{\"type\": \"long\", \"logicalType\": \"time-micros\"}]},\n"
                        + "   {\"name\": \"time_millis\", \"type\": [\"null\",{\"type\": \"int\", \"logicalType\": \"time-millis\"}]},\n"
                        + "   {\"name\": \"lts_micros\", \"type\": [\"null\",{\"type\": \"long\", \"logicalType\": \"local-timestamp-micros\"}]},\n"
                        + "   {\"name\": \"lts_millis\", \"type\": [\"null\",{\"type\": \"long\", \"logicalType\": \"local-timestamp-millis\"}]},\n"
                        + "   {\"name\": \"date\", \"type\": [\"null\",{\"type\": \"int\", \"logicalType\": \"date\"}]},\n"
                        + "   {\"name\": \"decimal\", \"type\": [\"null\",{\"type\": \"bytes\", \"logicalType\": \"decimal\", \"precision\": 4, \"scale\": 2}]},\n"
                        + "   {\"name\": \"uuid\", \"type\": [\"null\",{\"type\": \"string\", \"logicalType\": \"uuid\"}]},\n"
                        + "   {\"name\": \"geography\", \"type\": [\"null\",{\"type\": \"string\", \"logicalType\": \"geography_wkt\"}]}\n"
                        + " ]\n";
        return getAndReturn(fieldString);
    }

    public static BigQueryAvroToProtoSerializerTestResult testAllUnionPrimitiveSchemaConversion() {
        String fieldString =
                " \"fields\": [\n"
                        + "   {\"name\": \"name\", \"type\": [\"null\", \"string\"]},\n"
                        + "   {\"name\": \"number\", \"type\": [\"null\",\"long\"]},\n"
                        + "   {\"name\": \"quantity\", \"type\": [\"null\",\"int\"]},\n"
                        + "   {\"name\": \"fixed_field\", \"type\": [\"null\",{\"type\": "
                        + "\"fixed\", \"size\": 10,\"name\": \"hash\"}]},\n"
                        + "   {\"name\": \"price\", \"type\": [\"null\",\"float\"]},\n"
                        + "   {\"name\": \"double_field\", \"type\": [\"null\",\"double\"]},\n"
                        + "   {\"name\": \"boolean_field\", \"type\": [\"null\",\"boolean\"]},\n"
                        + "   {\"name\": \"enum_field\", \"type\": [\"null\",{\"type\":\"enum\", \"symbols\": [\"A\", \"B\", \"C\", \"D\"], \"name\": \"ALPHABET\"}]},\n"
                        + "   {\"name\": \"byte_field\", \"type\": [\"null\",\"bytes\"]}\n"
                        + " ]\n";
        return getAndReturn(fieldString);
    }

    public static BigQueryAvroToProtoSerializerTestResult testUnionInRecordSchemaConversation() {
        String fieldString =
                " \"fields\": [\n"
                        + "   {\"name\": \"record_with_union\", \"type\": "
                        + "{\"name\": \"record_with_union_field\","
                        + " \"type\": \"record\", \"fields\":"
                        + " [{\"name\": \"union_in_record\", \"type\": "
                        + "[\"boolean\", \"null\"], \"default\": true}]}}\n"
                        + " ]\n";
        return getAndReturn(fieldString);
    }

    public static BigQueryAvroToProtoSerializerTestResult
            testRecordOfLogicalTypeSchemaConversion() {
        String fieldString =
                " \"fields\": [\n"
                        + "{\"name\": \"record_of_logical_type\","
                        + " \"type\": "
                        + "{"
                        + "\"name\": \"record_name\", "
                        + "\"type\": \"record\","
                        + " \"fields\": "
                        + "["
                        + "   {\"name\": \"ts_micros\", \"type\": {\"type\": \"long\", \"logicalType\": \"timestamp-micros\"}},\n"
                        + "   {\"name\": \"ts_millis\", \"type\": {\"type\": \"long\", \"logicalType\": \"timestamp-millis\"}},\n"
                        + "   {\"name\": \"time_micros\", \"type\": {\"type\": \"long\", \"logicalType\": \"time-micros\"}},\n"
                        + "   {\"name\": \"time_millis\", \"type\": {\"type\": \"int\", \"logicalType\": \"time-millis\"}},\n"
                        + "   {\"name\": \"lts_micros\", \"type\": {\"type\": \"long\", \"logicalType\": \"local-timestamp-micros\"}},\n"
                        + "   {\"name\": \"lts_millis\", \"type\": {\"type\": \"long\", \"logicalType\": \"local-timestamp-millis\"}},\n"
                        + "   {\"name\": \"date\", \"type\": {\"type\": \"int\", \"logicalType\": \"date\"}},\n"
                        + "   {\"name\": \"decimal\", \"type\": {\"type\": \"bytes\", \"logicalType\": \"decimal\", \"precision\": 4, \"scale\": 2}},\n"
                        + "   {\"name\": \"uuid\", \"type\": {\"type\": \"string\", \"logicalType\": \"uuid\"}},\n"
                        + "   {\"name\": \"geography\", \"type\": {\"type\": \"string\", \"logicalType\": \"geography_wkt\"}}\n"
                        + "]"
                        + "}"
                        + "}\n"
                        + " ]\n";
        return getAndReturn(fieldString);
    }

    public static BigQueryAvroToProtoSerializerTestResult testDefaultValueSchemaConversion() {
        String fieldString =
                " \"fields\": [\n"
                        + "{\"name\": \"long_with_default\", \"type\": [\"long\", \"null\"],"
                        + " \"default\": 100}"
                        + " ]\n";
        return getAndReturn(fieldString);
    }
}
