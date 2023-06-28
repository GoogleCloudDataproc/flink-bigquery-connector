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

package com.google.cloud.flink.bigquery.common.config;

import org.apache.flink.annotation.PublicEvolving;

import com.google.auth.Credentials;
import com.google.auto.value.AutoValue;
import com.google.cloud.flink.bigquery.common.utils.GoogleCredentialsSupplier;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Objects;
import java.util.Optional;

/** An options object that covers the possible {@link Credentials} configurations. */
@AutoValue
@PublicEvolving
public abstract class CredentialsOptions implements Serializable {

    @Nullable
    public abstract String getCredentialsFile();

    @Nullable
    public abstract String getCredentialsKey();

    @Nullable
    public abstract String getAccessToken();

    /**
     * Returns the Google Credentials created given the provided configuration.
     *
     * @return The Google Credentials instance.
     */
    public Credentials getCredentials() {
        return GoogleCredentialsSupplier.supplyCredentialsFromSources(
                Optional.ofNullable(getAccessToken()),
                Optional.ofNullable(getCredentialsKey()),
                Optional.ofNullable(getCredentialsFile()));
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 61 * hash + Objects.hashCode(getCredentialsFile());
        hash = 61 * hash + Objects.hashCode(getCredentialsKey());
        hash = 61 * hash + Objects.hashCode(getAccessToken());
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final CredentialsOptions other = (CredentialsOptions) obj;
        if (!Objects.equals(this.getCredentialsFile(), other.getCredentialsFile())) {
            return false;
        }
        if (!Objects.equals(this.getCredentialsKey(), other.getCredentialsKey())) {
            return false;
        }
        return Objects.equals(this.getAccessToken(), other.getAccessToken());
    }

    /**
     * Creates a builder class for the {@link CredentialsOptions} class.
     *
     * @return A builder class.
     */
    public static CredentialsOptions.Builder builder() {
        return new AutoValue_CredentialsOptions.Builder();
    }

    /** A builder class for the {@link CredentialsOptions} class. */
    @AutoValue.Builder
    public abstract static class Builder {

        /**
         * Sets the credentials using a file system location.
         *
         * @param credentialsFile the path of the credentials file.
         * @return this builder's instance
         */
        public abstract Builder setCredentialsFile(String credentialsFile);

        /**
         * Sets the credentials using a credentials key, encoded in Base64.
         *
         * @param credentialsKey The credentials key.
         * @return this builder's instance
         */
        public abstract Builder setCredentialsKey(String credentialsKey);

        /**
         * Sets the credentials using a GCP access token.
         *
         * @param credentialsToken The GCP access token.
         * @return this builder's instance
         */
        public abstract Builder setAccessToken(String credentialsToken);

        /**
         * Builds a fully initialized {@link CredentialsOptions} instance.
         *
         * @return The {@link CredentialsOptions} instance.
         */
        public abstract CredentialsOptions build();
    }
}
