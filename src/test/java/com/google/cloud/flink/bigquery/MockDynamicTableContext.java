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

import java.util.Map;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.factories.DynamicTableFactory;

public class MockDynamicTableContext implements DynamicTableFactory.Context {

  private ObjectIdentifier objectIdentifier;
  private ResolvedCatalogTable catalogTable;
  private ReadableConfig configuration;
  private ClassLoader classLoader;
  private boolean isTemporary;

  public MockDynamicTableContext(
      ObjectIdentifier objectIdentifier,
      ResolvedCatalogTable catalogTable,
      Map<String, String> enrichmentOptions,
      ReadableConfig configuration,
      ClassLoader classLoader,
      boolean isTemporary) {
    this.objectIdentifier = objectIdentifier;
    this.catalogTable = catalogTable;
    this.configuration = configuration;
    this.classLoader = classLoader;
    this.isTemporary = isTemporary;
  }

  @Override
  public ObjectIdentifier getObjectIdentifier() {

    return objectIdentifier;
  }

  @Override
  public ResolvedCatalogTable getCatalogTable() {

    return catalogTable;
  }

  @Override
  public ReadableConfig getConfiguration() {

    return configuration;
  }

  @Override
  public ClassLoader getClassLoader() {

    return classLoader;
  }

  public boolean isTemporary() {

    return isTemporary;
  }
}
