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
import static org.junit.Assert.assertEquals;

import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class AvroDeserializationSchemaTest {

  static String jsonStringAvroSchema;
  static AvroDeserializationSchema<?> avroDeserializationSchema;

  @Before
  public void setUp() {

    jsonStringAvroSchema =
        "{\"type\":\"record\",\"name\":\"__root__\",\"fields\":[{\"name\":\"numeric_datatype\",\"type\":[\"null\",{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":38,\"scale\":9}]},{\"name\":\"string_datatype\",\"type\":[\"null\",\"string\"]}]}";
    Parser avroSchemaParser = new Schema.Parser();
    Schema avroSchema = avroSchemaParser.parse(jsonStringAvroSchema);
    avroDeserializationSchema =
        new AvroDeserializationSchema<GenericRecord>(GenericRecord.class, avroSchema);
  }

  @Test
  public void deserializeTest() throws IOException {

    ReadRowsResponse response = Mockito.mock(ReadRowsResponse.class);
    assertThat(avroDeserializationSchema.deserialize(response.toByteArray())).isNull();
  }

  @Test
  public void testIsEndOfStream() {
    assertThat(avroDeserializationSchema.isEndOfStream(null)).isTrue();
  }

  @Test
  public void testgetProducedType() {
    assertEquals(
        avroDeserializationSchema.getProducedType().toString(),
        "GenericRecord(\"{\"type\":\"record\",\"name\":\"__root__\",\"fields\":[{\"name\":\"numeric_datatype\",\"type\":[\"null\",{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":38,\"scale\":9}]},{\"name\":\"string_datatype\",\"type\":[\"null\",\"string\"]}]}\")");
    assertThat(avroDeserializationSchema.getProducedType())
        .isInstanceOf(GenericRecordAvroTypeInfo.class);
  }
}
