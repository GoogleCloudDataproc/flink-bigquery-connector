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
import static org.junit.Assert.assertThrows;

import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import java.io.IOException;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class ArrowDeserializationSchemaTest {

  static String jsonStringArrowSchema;
  static ArrowDeserializationSchema<VectorSchemaRoot> arrowDeserializationSchema;

  @Before
  public void setUp() {

    jsonStringArrowSchema = "Schema<word: Utf8, word_count: Int(64, true)>";
    arrowDeserializationSchema =
        new ArrowDeserializationSchema<VectorSchemaRoot>(
            VectorSchemaRoot.class, jsonStringArrowSchema, null);
  }

  @Test
  public void deserializeTest() throws IOException {

    ReadRowsResponse response = Mockito.mock(ReadRowsResponse.class);
    assertThrows(
        "Deserializing message is empty",
        NullPointerException.class,
        () -> {
          arrowDeserializationSchema.deserialize(response.toByteArray());
        });
  }

  @Test
  public void testIsEndOfStream() {

    VectorSchemaRoot root = Mockito.mock(VectorSchemaRoot.class);
    assertThat(arrowDeserializationSchema.isEndOfStream(null)).isTrue();
    assertThat(arrowDeserializationSchema.isEndOfStream(root)).isFalse();
  }

  @Test
  public void getProducedType() {
    assertThat(arrowDeserializationSchema.getProducedType()).isNull();
  }
}
