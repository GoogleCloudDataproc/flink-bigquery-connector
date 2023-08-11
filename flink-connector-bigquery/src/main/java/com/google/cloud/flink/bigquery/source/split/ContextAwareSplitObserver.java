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

package com.google.cloud.flink.bigquery.source.split;

import org.apache.flink.api.connector.source.SplitEnumeratorContext;

/**
 * Defines the behavior for those observers interested on getting notifications of the recently
 * discovered splits.
 */
public interface ContextAwareSplitObserver {

    /**
     * Returns a split enumerator context object.
     *
     * @return A split enumerator context object.
     */
    SplitEnumeratorContext<BigQuerySourceSplit> context();

    /** Called as soon new splits has been discovered and stored in the assigner's state. */
    void notifyDiscovery();
}
