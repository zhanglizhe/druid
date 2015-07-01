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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.repackaged.com.google.common.base.Strings;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import com.metamx.common.logger.Logger;
import io.druid.concurrent.Execs;
import io.druid.guice.ManageLifecycle;
import io.druid.guice.annotations.Json;
import org.joda.time.DateTime;
import org.skife.jdbi.v2.FoldController;
import org.skife.jdbi.v2.Folder3;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.HandleCallback;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 *
 */
public interface MetadataNamespaceManager
{
  void start();

  void stop();

  void addOrUpdateNamespace(String namespace, Map<String, Object> namespaceSpec);

  void disableNamespace(String namespace);

  List<Map<String, Object>> knownNamespaces();

  Map<String, Object> getNamespace(String namespace);

  boolean isLoadedOnce();

  void registerPostPollRunnable(Runnable runnable);

  @ManageLifecycle
  public static class SQLMetadataNamespaceManager implements MetadataNamespaceManager
  {
    private static final Logger log = new Logger(SQLMetadataNamespaceManager.class);
    private final ObjectMapper jsonMapper;
    private final Supplier<MetadataNamespaceManagerConfig> config;
    private final Supplier<MetadataStorageTablesConfig> dbTables;
    private final SQLMetadataConnector connector;
    private final Object lock = new Object();
    private volatile boolean started = false;
    private final AtomicBoolean loadedOnce = new AtomicBoolean(false);
    private final AtomicReference<Map<String, Map<String, Object>>> knownNamespaces = new AtomicReference<Map<String, Map<String, Object>>>(
        new ConcurrentHashMap<String, Map<String, Object>>()
    );
    private ListeningScheduledExecutorService exec;
    private ListenableFuture<?> future = null;
    private AtomicReference<Runnable> postPollRunnable = new AtomicReference<>(null);

    @Inject
    public SQLMetadataNamespaceManager(
        @Json ObjectMapper jsonMapper,
        Supplier<MetadataNamespaceManagerConfig> config,
        Supplier<MetadataStorageTablesConfig> dbTables,
        SQLMetadataConnector connector
    )
    {
      this.jsonMapper = jsonMapper;
      this.config = config;
      this.dbTables = dbTables;
      this.connector = connector;
      this.exec = createExecService();
    }

    private static final ListeningScheduledExecutorService createExecService()
    {
      return MoreExecutors.listeningDecorator(Execs.scheduledSingleThreaded("DatabaseSegmentManager-Exec--%d"));
    }

    @Override
    public void start()
    {
      synchronized (lock) {
        if (started) {
          return;
        }

        future = exec.scheduleWithFixedDelay(
            new Runnable()
            {
              @Override
              public void run()
              {
                poll();
                final Runnable runnable = postPollRunnable.get();
                if (runnable != null) {
                  runnable.run();
                }
              }
            },
            0,
            config.get().getPollDuration().toStandardDuration().getMillis(),
            TimeUnit.MILLISECONDS
        );

        started = true;
      }
    }

    @Override
    public void stop()
    {
      synchronized (lock) {
        if (!started) {
          return;
        }

        if (future != null) {
          if (!future.isDone() && !future.cancel(true)) {
            log.warn("Unable to cancel future! Trying to restart entire executor service");
            exec.shutdownNow();
            exec = createExecService();
          }
          future = null;
        }

        knownNamespaces.set(new ConcurrentHashMap<String, Map<String, Object>>());
        started = false;
      }
    }

    @Override
    public void addOrUpdateNamespace(final String namespace, final Map<String, Object> namespaceSpec)
    {
      Preconditions.checkArgument(!Strings.isNullOrEmpty(namespace), "blank namespace");
      connector.getDBI().withHandle(
          new HandleCallback<Boolean>()
          {
            @Override
            public Boolean withHandle(Handle handle) throws Exception
            {
              handle.begin();
              try {
                int rowCount = handle
                    .createStatement(
                        String.format(
                            "UPDATE %s SET used = false WHERE namespace = :namespace",
                            dbTables.get().getNamespacesTable()
                        )
                    )
                    .bind("namespace", namespace)
                    .execute();
                log.debug("Disabled %d rows", rowCount);
                rowCount = handle
                    .createStatement(
                        String.format(
                            "INSERT INTO %s (id, namespace, updated_date, used, payload) VALUES (:id, :namespace, :updated_date, :used, :payload)",
                            dbTables.get().getNamespacesTable()
                        )
                    )
                    .bind("id", UUID.randomUUID().toString())
                    .bind("namespace", namespace)
                    .bind("updated_date", DateTime.now().toString())
                    .bind("used", true)
                    .bind(
                        "payload",
                        jsonMapper.writeValueAsBytes(namespaceSpec)
                    ).execute();
                log.debug("Inserted %d new rows", rowCount);
                handle.commit();
              }
              catch (Exception e) {
                log.warn("Rolling back");
                handle.rollback();
                throw e;
              }
              if (handle.isInTransaction()) {
                log.debug("Still in transaction");
              }
              return null;
            }
          }
      );
      knownNamespaces.get().put(namespace, namespaceSpec);
    }

    @Override
    public void disableNamespace(final String namespace)
    {
      Preconditions.checkArgument(!Strings.isNullOrEmpty(namespace), "blank namespace");
      connector.getDBI().withHandle(
          new HandleCallback<Boolean>()
          {
            @Override
            public Boolean withHandle(Handle handle) throws Exception
            {
              return handle.update(
                  String.format(
                      "UPDATE %s SET used = false WHERE namespace = :namespace",
                      dbTables.get().getNamespacesTable()
                  ), namespace
              ) > 0;
            }
          }
      );
      knownNamespaces.get().remove(namespace);
    }

    @Override
    public List<Map<String, Object>> knownNamespaces()
    {
      return ImmutableList.copyOf(knownNamespaces.get().values());
    }

    @Override
    public Map<String, Object> getNamespace(String namespace)
    {
      Preconditions.checkArgument(!Strings.isNullOrEmpty(namespace), "blank namespace");
      return knownNamespaces.get().get(namespace);
    }

    @Override
    public boolean isLoadedOnce()
    {
      return loadedOnce.get();
    }

    @Override
    public void registerPostPollRunnable(Runnable runnable)
    {
      postPollRunnable.set(Preconditions.checkNotNull(runnable, "runnable"));
    }


    protected void poll()
    {
      knownNamespaces.set(
          connector.getDBI().withHandle(
              new HandleCallback<ConcurrentMap<String, Map<String, Object>>>()
              {
                @Override
                public ConcurrentMap<String, Map<String, Object>> withHandle(Handle handle) throws Exception
                {
                  final ConcurrentMap<String, Map<String, Object>> map = handle
                      .createQuery(
                          String.format(
                              "SELECT payload FROM %s WHERE used ORDER BY updated_date DESC",
                              dbTables.get().getNamespacesTable()
                          )
                      )
                      .map(
                          new ResultSetMapper<Map<String, Map<String, Object>>>()
                          {
                            @Override
                            public Map<String, Map<String, Object>> map(int index, ResultSet r, StatementContext ctx)
                                throws SQLException
                            {
                              try {
                                Map<String, Object> map = jsonMapper.readValue(
                                    r.getBytes("payload"), new TypeReference<Map<String, Object>>()
                                    {
                                    }
                                );

                                return ImmutableMap.<String, Map<String, Object>>of(
                                    map.get("namespace").toString(),
                                    map
                                );
                              }
                              catch (IOException e) {
                                throw Throwables.propagate(e);
                              }
                            }
                          }
                      ).fold(
                          new ConcurrentHashMap<String, Map<String, Object>>(),
                          new Folder3<ConcurrentHashMap<String, Map<String, Object>>, Map<String, Map<String, Object>>>()
                          {
                            @Override
                            public ConcurrentHashMap<String, Map<String, Object>> fold(
                                ConcurrentHashMap<String, Map<String, Object>> accumulator,
                                Map<String, Map<String, Object>> rs,
                                FoldController control,
                                StatementContext ctx
                            ) throws SQLException
                            {
                              accumulator.putAll(rs);
                              return accumulator;
                            }
                          }
                      );
                  if (log.isDebugEnabled()) {
                    log.debug("Polled and found %d namespaces in database", map.size());
                  }
                  return map;
                }
              }
          )
      );
      loadedOnce.set(true);
    }
  }


}
