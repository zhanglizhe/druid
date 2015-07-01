/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.metadata;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.repackaged.com.google.common.base.Throwables;
import com.google.common.base.Supplier;
import com.google.inject.Inject;
import com.metamx.common.lifecycle.Lifecycle;
import io.druid.guice.annotations.Json;

public class SQLMetadataNamespaceManagerProvider implements MetadataNamespaceManagerProvider
{
  private final Lifecycle lifecycle;
  private final SQLMetadataConnector connector;
  private final ObjectMapper jsonMapper;
  private final Supplier<MetadataNamespaceManagerConfig> config;
  private final Supplier<MetadataStorageTablesConfig> dbTables;

  @Inject
  public SQLMetadataNamespaceManagerProvider(
      final Lifecycle lifecycle,
      final SQLMetadataConnector connector,
      final @Json ObjectMapper jsonMapper,
      final Supplier<MetadataNamespaceManagerConfig> config,
      final Supplier<MetadataStorageTablesConfig> dbTables
  )
  {
    this.lifecycle = lifecycle;
    this.connector = connector;
    this.jsonMapper = jsonMapper;
    this.config = config;
    this.dbTables = dbTables;
  }

  @Override
  public MetadataNamespaceManager get()
  {
    try {
      lifecycle.addMaybeStartHandler(
          new Lifecycle.Handler()
          {
            @Override
            public void start() throws Exception
            {
              connector.createNamespacesTable();
            }

            @Override
            public void stop()
            {
              // NOOP
            }
          }
      );
      return new MetadataNamespaceManager.SQLMetadataNamespaceManager(jsonMapper, config, dbTables, connector);
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
