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

package com.google.cloud.flink.bigquery.common.utils;

import com.google.auth.Credentials;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.flink.bigquery.common.utils.flink.annotations.Internal;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Base64;
import java.util.Optional;

/** A utility class to supply credentials given the multiple possible configuration sources. */
@Internal
public class GoogleCredentialsSupplier {
    private GoogleCredentialsSupplier() {}

    /**
     * Supplies a Google {@link Credentials} object, given the possible configurations.
     *
     * @param accessToken The actual access token as a string.
     * @param credentialsKey The actual key encoded in a Base64 based string.
     * @param credentialsFile The location of the credentials file.
     * @return A fully initialized {@link Credentials} object.
     */
    public static Credentials supplyCredentialsFromSources(
            Optional<String> accessToken,
            Optional<String> credentialsKey,
            Optional<String> credentialsFile) {
        Credentials credentials;
        if (accessToken.isPresent()) {
            credentials = createCredentialsFromAccessToken(accessToken.get());
        } else if (credentialsKey.isPresent()) {
            credentials = createCredentialsFromKey(credentialsKey.get());
        } else if (credentialsFile.isPresent()) {
            credentials = createCredentialsFromFile(credentialsFile.get());
        } else {
            credentials = createDefaultCredentials();
        }
        return credentials;
    }

    private static Credentials createCredentialsFromAccessToken(String accessToken) {
        return GoogleCredentials.create(new AccessToken(accessToken, null));
    }

    private static Credentials createCredentialsFromKey(String key) {
        try {
            // Replaced BaseEncoding.base64() [of com.google.common.io.BaseEncoding]
            // with Base64.getDecoder() since flink does not allow common.io methods
            // to coexist and prefers usage of flink-shaded-guava methods instead.
            // But that would cause dependency on flink, so replaced with java.utils.Base64
            // Both support RFC-4648 (https://www.ietf.org/rfc/rfc4648.txt)
            // Links:
            // 1. BaseEncoding:
            // https://guava.dev/releases/17.0/api/docs/com/google/common/io/BaseEncoding.html#base64()
            // 2. Base64:
            // https://docs.oracle.com/javase/8/docs/api/java/util/Base64.html
            return GoogleCredentials.fromStream(
                    new ByteArrayInputStream(Base64.getDecoder().decode(key)));
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to create Credentials from key", e);
        }
    }

    private static Credentials createCredentialsFromFile(String file) {
        try {
            return GoogleCredentials.fromStream(new FileInputStream(file));
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to create Credentials from file", e);
        }
    }

    private static Credentials createDefaultCredentials() {
        try {
            return GoogleCredentials.getApplicationDefault();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to create default Credentials", e);
        }
    }
}
