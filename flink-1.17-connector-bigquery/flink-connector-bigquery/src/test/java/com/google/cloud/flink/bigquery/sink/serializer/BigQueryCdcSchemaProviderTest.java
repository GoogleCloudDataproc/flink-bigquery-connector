/*
 * Copyright (C) 2024 Google Inc.
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

package com.google.cloud.flink.bigquery.sink.serializer;

import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import org.apache.avro.Schema;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** Tests for {@link BigQueryCdcSchemaProvider}. */
public class BigQueryCdcSchemaProviderTest {

    @Test
    public void testCdcSchemaAugmentation() {
        BigQuerySchemaProvider baseProvider = TestBigQuerySchemas.getSimpleRecordSchema();
        BigQueryCdcSchemaProvider cdcProvider = new BigQueryCdcSchemaProvider(baseProvider);

        assertFalse(cdcProvider.schemaUnknown());

        // Verify Avro schema augmentation
        Schema avroSchema = cdcProvider.getAvroSchema();
        assertNotNull(avroSchema);

        // Check original fields are preserved
        assertNotNull(avroSchema.getField("long_field"));
        assertNotNull(avroSchema.getField("string_field"));

        // Check CDC fields are added
        Schema.Field changeTypeField =
                avroSchema.getField(BigQueryCdcSchemaProvider.CDC_CHANGE_TYPE_FIELD);
        assertNotNull(changeTypeField);
        assertTrue(changeTypeField.schema().isUnion());

        Schema.Field sequenceNumberField =
                avroSchema.getField(BigQueryCdcSchemaProvider.CDC_SEQUENCE_NUMBER_FIELD);
        assertNotNull(sequenceNumberField);
        assertTrue(sequenceNumberField.schema().isUnion());
    }

    @Test
    public void testCdcDescriptorAugmentation() {
        BigQuerySchemaProvider baseProvider = TestBigQuerySchemas.getSimpleRecordSchema();
        BigQueryCdcSchemaProvider cdcProvider = new BigQueryCdcSchemaProvider(baseProvider);

        Descriptor descriptor = cdcProvider.getDescriptor();
        assertNotNull(descriptor);

        // Check original fields are preserved
        FieldDescriptor longField = descriptor.findFieldByName("long_field");
        assertNotNull(longField);
        assertEquals(FieldDescriptor.Type.INT64, longField.getType());

        FieldDescriptor stringField = descriptor.findFieldByName("string_field");
        assertNotNull(stringField);
        assertEquals(FieldDescriptor.Type.STRING, stringField.getType());

        // Check CDC fields are added
        FieldDescriptor changeTypeField =
                descriptor.findFieldByName(BigQueryCdcSchemaProvider.CDC_CHANGE_TYPE_FIELD);
        assertNotNull(changeTypeField);
        assertEquals(FieldDescriptor.Type.STRING, changeTypeField.getType());
        assertTrue(changeTypeField.isOptional());

        FieldDescriptor sequenceNumberField =
                descriptor.findFieldByName(BigQueryCdcSchemaProvider.CDC_SEQUENCE_NUMBER_FIELD);
        assertNotNull(sequenceNumberField);
        assertEquals(FieldDescriptor.Type.STRING, sequenceNumberField.getType());
        assertTrue(sequenceNumberField.isOptional());
    }

    @Test
    public void testCdcDescriptorProtoAugmentation() {
        BigQuerySchemaProvider baseProvider = TestBigQuerySchemas.getSimpleRecordSchema();
        int baseFieldCount = baseProvider.getDescriptorProto().getFieldCount();

        BigQueryCdcSchemaProvider cdcProvider = new BigQueryCdcSchemaProvider(baseProvider);
        int cdcFieldCount = cdcProvider.getDescriptorProto().getFieldCount();

        // CDC provider should have 2 additional fields
        assertEquals(baseFieldCount + 2, cdcFieldCount);

        // Verify CDC fields in descriptor proto
        boolean foundChangeType = false;
        boolean foundSequenceNumber = false;
        for (FieldDescriptorProto field : cdcProvider.getDescriptorProto().getFieldList()) {
            if (field.getName().equals(BigQueryCdcSchemaProvider.CDC_CHANGE_TYPE_FIELD)) {
                foundChangeType = true;
                assertEquals(FieldDescriptorProto.Type.TYPE_STRING, field.getType());
                assertEquals(FieldDescriptorProto.Label.LABEL_OPTIONAL, field.getLabel());
            }
            if (field.getName().equals(BigQueryCdcSchemaProvider.CDC_SEQUENCE_NUMBER_FIELD)) {
                foundSequenceNumber = true;
                assertEquals(FieldDescriptorProto.Type.TYPE_STRING, field.getType());
                assertEquals(FieldDescriptorProto.Label.LABEL_OPTIONAL, field.getLabel());
            }
        }
        assertTrue("_change_type field should be present", foundChangeType);
        assertTrue("_change_sequence_number field should be present", foundSequenceNumber);
    }

    @Test
    public void testCdcFieldNumbers() {
        BigQuerySchemaProvider baseProvider = TestBigQuerySchemas.getSimpleRecordSchema();
        BigQueryCdcSchemaProvider cdcProvider = new BigQueryCdcSchemaProvider(baseProvider);

        // Find max field number in base schema
        int maxBaseFieldNumber = 0;
        for (FieldDescriptorProto field : baseProvider.getDescriptorProto().getFieldList()) {
            maxBaseFieldNumber = Math.max(maxBaseFieldNumber, field.getNumber());
        }

        // Verify CDC fields have consecutive field numbers after max
        Descriptor descriptor = cdcProvider.getDescriptor();
        FieldDescriptor changeTypeField =
                descriptor.findFieldByName(BigQueryCdcSchemaProvider.CDC_CHANGE_TYPE_FIELD);
        FieldDescriptor sequenceNumberField =
                descriptor.findFieldByName(BigQueryCdcSchemaProvider.CDC_SEQUENCE_NUMBER_FIELD);

        assertEquals(maxBaseFieldNumber + 1, changeTypeField.getNumber());
        assertEquals(maxBaseFieldNumber + 2, sequenceNumberField.getNumber());
    }

    @Test
    public void testCdcSchemaProviderWithUnknownSchema() {
        BigQuerySchemaProvider baseProvider = new TestSchemaProvider(null, null);
        BigQueryCdcSchemaProvider cdcProvider = new BigQueryCdcSchemaProvider(baseProvider);

        assertTrue(cdcProvider.schemaUnknown());
        assertNull(cdcProvider.getAvroSchema());
        assertNull(cdcProvider.getDescriptor());
        assertNull(cdcProvider.getDescriptorProto());
    }

    @Test
    public void testCdcSchemaProviderEquality() {
        BigQuerySchemaProvider baseProvider1 = TestBigQuerySchemas.getSimpleRecordSchema();
        BigQuerySchemaProvider baseProvider2 = TestBigQuerySchemas.getSimpleRecordSchema();

        BigQueryCdcSchemaProvider cdcProvider1 = new BigQueryCdcSchemaProvider(baseProvider1);
        BigQueryCdcSchemaProvider cdcProvider2 = new BigQueryCdcSchemaProvider(baseProvider2);

        assertEquals(cdcProvider1, cdcProvider2);
        assertEquals(cdcProvider1.hashCode(), cdcProvider2.hashCode());
    }

    @Test
    public void testCdcSchemaProviderWithComplexSchema() {
        BigQuerySchemaProvider baseProvider =
                TestBigQuerySchemas.getSchemaWithRequiredPrimitiveTypes();
        BigQueryCdcSchemaProvider cdcProvider = new BigQueryCdcSchemaProvider(baseProvider);

        // Verify all original fields are preserved
        Schema avroSchema = cdcProvider.getAvroSchema();
        assertNotNull(avroSchema.getField("number"));
        assertNotNull(avroSchema.getField("price"));
        assertNotNull(avroSchema.getField("species"));
        assertNotNull(avroSchema.getField("flighted"));
        assertNotNull(avroSchema.getField("sound"));
        assertNotNull(avroSchema.getField("required_record_field"));

        // Verify CDC fields are added
        assertNotNull(avroSchema.getField(BigQueryCdcSchemaProvider.CDC_CHANGE_TYPE_FIELD));
        assertNotNull(avroSchema.getField(BigQueryCdcSchemaProvider.CDC_SEQUENCE_NUMBER_FIELD));

        // Verify descriptor
        Descriptor descriptor = cdcProvider.getDescriptor();
        assertNotNull(descriptor.findFieldByName("number"));
        assertNotNull(descriptor.findFieldByName(BigQueryCdcSchemaProvider.CDC_CHANGE_TYPE_FIELD));
        assertNotNull(
                descriptor.findFieldByName(BigQueryCdcSchemaProvider.CDC_SEQUENCE_NUMBER_FIELD));
    }
}
