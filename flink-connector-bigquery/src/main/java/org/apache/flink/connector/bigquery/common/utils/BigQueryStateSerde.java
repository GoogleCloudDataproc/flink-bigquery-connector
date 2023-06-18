/*
 * Copyright (C) 2023 Google Inc.
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

package org.apache.flink.connector.bigquery.common.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.function.BiConsumerWithException;
import org.apache.flink.util.function.FunctionWithException;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** A utility class with some helper method for serde in the BigQuery source and its state. */
@Internal
public class BigQueryStateSerde {

    /** Private constructor for utility class. */
    private BigQueryStateSerde() {}

    /**
     * Serializes a list of data and writes it into a data output stream.
     *
     * @param <T> The type of the list's elements.
     * @param out The data output stream.
     * @param list The data to be serialized.
     * @param serializer The serialization function of the list's elements.
     * @throws IOException In case of serialization or stream write problems.
     */
    public static <T> void serializeList(
            DataOutputStream out,
            List<T> list,
            BiConsumerWithException<DataOutputStream, T, IOException> serializer)
            throws IOException {
        out.writeInt(list.size());
        for (T t : list) {
            serializer.accept(out, t);
        }
    }

    /**
     * De-serializes a list from the data input stream.
     *
     * @param <T> The type of the list's elements.
     * @param in the data input stream.
     * @param deserializer the de-serialization function for the list's elements.
     * @return A fully initialized list with elements de-serialized from the data input stream.
     * @throws IOException In case of de-serialization or read problems.
     */
    public static <T> List<T> deserializeList(
            DataInputStream in, FunctionWithException<DataInputStream, T, IOException> deserializer)
            throws IOException {
        int size = in.readInt();
        List<T> list = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            T t = deserializer.apply(in);
            list.add(t);
        }

        return list;
    }

    /**
     * Serializes a map of data and writes it into a data output stream.
     *
     * @param <K> The type of the map's keys.
     * @param <V> The type of the map's values.
     * @param out The data output stream.
     * @param map The data output stream.
     * @param keySerializer Serialization function for the map's keys.
     * @param valueSerializer Serialization function for the map's values.
     * @throws IOException In case of serialization or stream write problems.
     */
    public static <K, V> void serializeMap(
            DataOutputStream out,
            Map<K, V> map,
            BiConsumerWithException<DataOutputStream, K, IOException> keySerializer,
            BiConsumerWithException<DataOutputStream, V, IOException> valueSerializer)
            throws IOException {
        out.writeInt(map.size());
        for (Map.Entry<K, V> entry : map.entrySet()) {
            keySerializer.accept(out, entry.getKey());
            valueSerializer.accept(out, entry.getValue());
        }
    }

    /**
     * Serializes a list from the data input stream.
     *
     * @param <K> The type of the map's keys.
     * @param <V> The type of the map's values.
     * @param in the data input stream.
     * @param keyDeserializer De-serialization function for the map's keys.
     * @param valueDeserializer De-serialization function for the map's values.
     * @return A fully initialized map instance, with elements read from the data input stream.
     * @throws IOException In case of de-serialization or read problems.
     */
    public static <K, V> Map<K, V> deserializeMap(
            DataInputStream in,
            FunctionWithException<DataInputStream, K, IOException> keyDeserializer,
            FunctionWithException<DataInputStream, V, IOException> valueDeserializer)
            throws IOException {
        int size = in.readInt();
        Map<K, V> result = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            K key = keyDeserializer.apply(in);
            V value = valueDeserializer.apply(in);
            result.put(key, value);
        }
        return result;
    }
}
