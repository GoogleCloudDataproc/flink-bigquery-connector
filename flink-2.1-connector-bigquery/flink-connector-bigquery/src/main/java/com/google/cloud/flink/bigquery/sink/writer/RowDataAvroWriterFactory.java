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

package com.google.cloud.flink.bigquery.sink.writer;

import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.formats.avro.AvroBuilder;
import org.apache.flink.formats.avro.AvroWriterFactory;
import org.apache.flink.formats.avro.RowDataToAvroConverters;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;

import java.io.IOException;
import java.io.OutputStream;

/**
 * A {@link BulkWriter.Factory} that converts {@link RowData} to Avro {@link GenericRecord} and
 * writes them to Avro files.
 *
 * <p>This replicates the pattern from Flink's {@code
 * AvroFileFormatFactory.RowDataAvroWriterFactory} (which is private). It uses:
 *
 * <ul>
 *   <li>{@link AvroSchemaConverter#convertToSchema(RowType)} for schema generation
 *   <li>{@link RowDataToAvroConverters#createConverter(RowType)} for RowData to GenericRecord
 *       conversion
 * </ul>
 */
public class RowDataAvroWriterFactory implements BulkWriter.Factory<RowData> {

    private static final long serialVersionUID = 1L;

    private final AvroWriterFactory<GenericRecord> factory;
    private final RowType rowType;

    private static class AvroDataFileWriterFactory implements AvroBuilder<GenericRecord> {

        private static final long serialVersionUID = 1L;

        private final RowType rowType;

        AvroDataFileWriterFactory(RowType rowType) {
            this.rowType = rowType;
        }

        @Override
        public DataFileWriter<GenericRecord> createWriter(OutputStream out) throws IOException {
            Schema schema = AvroSchemaConverter.convertToSchema(rowType);
            DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
            DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
            dataFileWriter.setCodec(CodecFactory.snappyCodec());
            dataFileWriter.create(schema, out);
            return dataFileWriter;
        }
    }

    public RowDataAvroWriterFactory(RowType rowType) {
        this.rowType = rowType;
        this.factory = new AvroWriterFactory<>(new AvroDataFileWriterFactory(rowType));
    }

    @Override
    public BulkWriter<RowData> create(FSDataOutputStream out) throws IOException {
        BulkWriter<GenericRecord> writer = factory.create(out);
        RowDataToAvroConverters.RowDataToAvroConverter converter =
                RowDataToAvroConverters.createConverter(rowType);
        Schema schema = AvroSchemaConverter.convertToSchema(rowType);
        return new BulkWriter<RowData>() {

            @Override
            public void addElement(RowData element) throws IOException {
                GenericRecord record = (GenericRecord) converter.convert(schema, element);
                writer.addElement(record);
            }

            @Override
            public void flush() throws IOException {
                writer.flush();
            }

            @Override
            public void finish() throws IOException {
                writer.finish();
            }
        };
    }
}
