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

package io.druid.server.namespace.cache;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.google.api.client.http.HttpStatusCodes;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import com.metamx.common.ISE;
import com.metamx.common.StreamUtils;
import com.metamx.common.StringUtils;
import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.common.logger.Logger;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.Request;
import com.metamx.http.client.response.ClientResponse;
import com.metamx.http.client.response.HttpResponseHandler;
import com.metamx.http.client.response.SequenceInputStreamResponseHandler;
import io.druid.audit.AuditInfo;
import io.druid.common.config.JacksonConfigManager;
import io.druid.concurrent.Execs;
import io.druid.guice.annotations.Global;
import io.druid.guice.annotations.Smile;
import io.druid.server.DruidNode;
import io.druid.server.listener.announcer.ListenerDiscoverer;
import io.druid.server.listener.resource.ListenerResource;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.joda.time.Duration;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 *
 */
public class LookupCoordinatorManager
{
  public static final String LOOKUP_CONFIG_KEY = "lookups";
  // Doesn't have to be the same, but it makes things easy to look at
  public static final String LOOKUP_LISTEN_ANNOUNCE_KEY = LOOKUP_CONFIG_KEY;
  private static final Logger LOG = new Logger(LookupCoordinatorManager.class);

  private final ListeningScheduledExecutorService executorService;
  private final ListenerDiscoverer listenerDiscoverer;
  private final HttpClient httpClient;
  private final ObjectMapper smileMapper;
  private final JacksonConfigManager configManager;
  private final Object startStopSync = new Object();
  // Updated by config watching service
  private AtomicReference<Map<String, Map<String, Object>>> lookupMapConfigRef;
  private volatile Map<String, Map<String, Object>> prior_update = null;
  private volatile boolean started = false;

  @Inject
  public LookupCoordinatorManager(
      final @Global HttpClient httpClient,
      final ListenerDiscoverer listenerDiscoverer,
      final @Smile ObjectMapper smileMapper,
      final JacksonConfigManager configManager
  )
  {
    this.listenerDiscoverer = listenerDiscoverer;
    this.configManager = configManager;
    this.httpClient = httpClient;
    this.smileMapper = smileMapper;
    executorService = MoreExecutors.listeningDecorator(
        Executors.newScheduledThreadPool(10, Execs.makeThreadFactory("LookupCoordinatorManager--%s"))
    );
  }

  void updateAllOnHost(final URL url, Map<String, Map<String, Object>> knownLookups)
      throws IOException, InterruptedException, ExecutionException
  {
    final AtomicInteger returnCode = new AtomicInteger(0);
    final AtomicReference<String> reasonString = new AtomicReference<>(null);
    final byte[] bytes;
    try {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Loading up %d lookups to %s", knownLookups.size(), url);
      }
      bytes = smileMapper.writeValueAsBytes(knownLookups);
    }
    catch (JsonProcessingException e) {
      throw Throwables.propagate(e);
    }

    try (final InputStream result = httpClient.go(
        new Request(HttpMethod.POST, url)
            .addHeader(HttpHeaders.Names.ACCEPT, SmileMediaTypes.APPLICATION_JACKSON_SMILE)
            .addHeader(HttpHeaders.Names.CONTENT_TYPE, SmileMediaTypes.APPLICATION_JACKSON_SMILE)
            .setContent(bytes),
        makeResponseHandler(returnCode, reasonString),
        Duration.millis(60_000) // TODO: configurable
    ).get()) {
      if (!HttpStatusCodes.isSuccess(returnCode.get())) {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
          StreamUtils.copyAndClose(result, baos);
        }
        catch (IOException e2) {
          LOG.warn(e2, "Error reading response");
        }

        throw new IOException(
            String.format(
                "Bad update request [%d] : [%s]  Response: [%s]",
                returnCode.get(),
                reasonString.get(),
                StringUtils.fromUtf8(baos.toByteArray())
            )
        );
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Status: %s reason: [%s]", returnCode.get(), reasonString.get());
        }
      }
    }
  }

  // Overridden in unit tests
  HttpResponseHandler<InputStream, InputStream> makeResponseHandler(
      final AtomicInteger returnCode,
      final AtomicReference<String> reasonString
  )
  {
    return new SequenceInputStreamResponseHandler()
    {
      @Override
      public ClientResponse<InputStream> handleResponse(HttpResponse response)
      {
        returnCode.set(response.getStatus().getCode());
        reasonString.set(response.getStatus().getReasonPhrase());
        return super.handleResponse(response);
      }
    };
  }

  void updateAll(final Map<String, Map<String, Object>> knownNamespaces)
      throws IOException, InterruptedException, ExecutionException
  {
    if (knownNamespaces == null) {
      LOG.info("No config for lookup namespaces found");
      return;
    }
    if (knownNamespaces.isEmpty()) {
      LOG.info("No known namespaces. Skipping update");
      return;
    }
    final Collection<URL> urls = getAllHostsAnnounceEndpoint();
    final List<ListenableFuture<?>> futures = new ArrayList<>(urls.size());
    for (final URL url : urls) {
      futures.add(executorService.submit(new Runnable()
      {
        @Override
        public void run()
        {
          try {
            updateAllOnHost(url, knownNamespaces);
          }
          catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("Update on [%s] interrupted", url);
            throw Throwables.propagate(e);
          }
          catch (IOException | ExecutionException e) {
            // Don't raise as ExecutionException. Just log and continue
            LOG.warn(e, "Error submitting to [%s]", url);
          }
        }
      }));
    }
    final ListenableFuture allFuture = Futures.allAsList(futures);
    try {
      allFuture.get(60_000, TimeUnit.MILLISECONDS);
    }
    catch (TimeoutException e) {
      LOG.warn("Timeout in updating hosts! Attempting to cancel");
      // This should cause Interrupted exceptions on the offending ones
      allFuture.cancel(true);
    }
  }

  Collection<URL> getAllHostsAnnounceEndpoint()
  {
    return ImmutableList.copyOf(
        Collections2.filter(
            Collections2.transform(
                listenerDiscoverer.getNodes(LOOKUP_LISTEN_ANNOUNCE_KEY),
                new Function<DruidNode, URL>()
                {
                  @Nullable
                  @Override
                  public URL apply(DruidNode input)
                  {
                    if (input == null) {
                      LOG.warn("null entry in namespaces");
                      return null;
                    }
                    try {
                      return getLookupsURL(input);
                    }
                    catch (MalformedURLException e) {
                      LOG.warn(e, "Skipping node. Malformed URL from `%s`", input);
                      return null;
                    }
                  }
                }
            ),
            new Predicate<URL>()
            {
              @Override
              public boolean apply(@Nullable URL input)
              {
                return input != null;
              }
            }
        )
    );
  }

  static URL getLookupsURL(DruidNode druidNode) throws MalformedURLException
  {
    // TODO: https?
    return new URL("http", druidNode.getHost(), druidNode.getPort(), ListenerResource.BASE_PATH + "/" + LOOKUP_LISTEN_ANNOUNCE_KEY);
  }

  public void updateLookup(final String lookupName, Map<String, Object> spec, final AuditInfo auditInfo)
  {
    updateLookups(ImmutableMap.of(lookupName, spec), auditInfo);
  }

  public void updateLookups(final Map<String, Map<String, Object>> spec, AuditInfo auditInfo)
  {
    final Map<String, Map<String, Object>> prior = getKnownLookups();
    if (prior == null) {
      // To prevent accidentally erasing configs if we haven't updated our cache of the values
      throw new ISE("Not initialized. If this is the first namespace, post an empty list to initialize");
    }
    final Map<String, Map<String, Object>> update = new HashMap<>(prior);
    update.putAll(spec);
    configManager.set(LOOKUP_CONFIG_KEY, update, auditInfo);
  }

  public Map<String, Map<String, Object>> getKnownLookups()
  {
    if (!started) {
      throw new ISE("Not started");
    }
    return lookupMapConfigRef.get();
  }

  public boolean deleteLookup(@NotNull final String lookup, AuditInfo auditInfo)
  {
    final Map<String, Map<String, Object>> prior = getKnownLookups();
    if (prior == null) {
      LOG.warn("Requested delete lookup [%s]. But no namespaces exist!", lookup);
      return false;
    }
    final Map<String, Map<String, Object>> update = new HashMap<>(prior);
    if (update.remove(lookup) != null) {
      configManager.set(LOOKUP_CONFIG_KEY, update, auditInfo);
      return true;
    } else {
      return false;
    }
  }

  /**
   * Try to find a lookupName spec for the specified lookupName.
   *
   * @param lookupName The lookupName to look for
   *
   * @return The lookupName spec if found or null if not found or if no namespaces at all are found
   */
  public
  @Nullable
  Map<String, Object> getLookup(@NotNull final String lookupName)
  {
    final Map<String, Map<String, Object>> prior = getKnownLookups();
    if (prior == null) {
      LOG.warn("Requested lookupName [%s]. But no namespaces exist!", lookupName);
      return null;
    }
    return prior.get(lookupName);
  }


  @LifecycleStart
  public void start()
  {
    synchronized (startStopSync) {
      if (started) {
        return;
      }
      if (executorService.isShutdown()) {
        throw new ISE("Cannot restart after stop!");
      }
      lookupMapConfigRef = configManager.watch(
          LOOKUP_CONFIG_KEY,
          new TypeReference<Map<String, Map<String, Object>>>()
          {
          },
          null
      );
      executorService.scheduleWithFixedDelay(
          new Runnable()
          {
            @Override
            public void run()
            {
              final Map<String, Map<String, Object>> knownNamespaces = lookupMapConfigRef.get();
              if (knownNamespaces == prior_update) {
                LOG.debug("No update, skipping");
                return;
              }
              // Sanity check for if we are shutting down
              if (Thread.currentThread().isInterrupted()) {
                LOG.info("Not updating namespace lookups because process was interrupted");
                return;
              }
              if (!started) {
                LOG.info("Not started. Returning");
                return;
              }
              try {
                updateAll(knownNamespaces);
                prior_update = knownNamespaces;
              }
              catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw Throwables.propagate(e);
              }
              catch (Exception e) {
                LOG.error(e, "Error populating namespaces. Will try again soon");
              }
            }
          },
          0,
          // TODO: configurable
          30_000,
          TimeUnit.MILLISECONDS
      );
      started = true;
      LOG.debug("Started");
    }
  }

  @LifecycleStop
  public void stop()
  {
    synchronized (startStopSync) {
      if (!started) {
        LOG.warn("Not started, ignoring stop request");
        return;
      }
      started = false;
      executorService.shutdownNow();
      // NOTE: we can't un-watch the configuration key
      LOG.debug("Stopped");
    }
  }
}
