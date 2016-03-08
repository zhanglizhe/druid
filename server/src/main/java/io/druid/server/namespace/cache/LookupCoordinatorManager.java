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
import com.google.common.collect.Sets;
import com.google.common.net.HostAndPort;
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
import io.druid.query.extraction.LookupExtractionModule;
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
  private final static Function<HostAndPort, URL> HOST_TO_URL = new Function<HostAndPort, URL>()
  {
    @Nullable
    @Override
    public URL apply(HostAndPort input)
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
  };

  private final ListeningScheduledExecutorService executorService;
  private final ListenerDiscoverer listenerDiscoverer;
  private final HttpClient httpClient;
  private final ObjectMapper smileMapper;
  private final JacksonConfigManager configManager;
  private final Object startStopSync = new Object();
  // Updated by config watching service
  private AtomicReference<Map<String, Map<String, Map<String, Object>>>> lookupMapConfigRef;
  private volatile Map<String, Map<String, Map<String, Object>>> prior_update = ImmutableMap.of();
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

  void deleteOnHost(final URL url)
      throws ExecutionException, InterruptedException, IOException
  {
    final AtomicInteger returnCode = new AtomicInteger(0);
    final AtomicReference<String> reasonString = new AtomicReference<>(null);
    LOG.debug("Dropping %s", url);

    try (final InputStream result = httpClient.go(
        new Request(HttpMethod.DELETE, url)
            .addHeader(HttpHeaders.Names.ACCEPT, SmileMediaTypes.APPLICATION_JACKSON_SMILE),
        makeResponseHandler(returnCode, reasonString),
        Duration.millis(1_000) // TODO: configurable
    ).get()) {
      // 404 is ok here, that means it was already deleted
      if (!HttpStatusCodes.isSuccess(returnCode.get()) || HttpStatusCodes.STATUS_CODE_NOT_FOUND != returnCode.get()) {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
          StreamUtils.copyAndClose(result, baos);
        }
        catch (IOException e2) {
          LOG.warn(e2, "Error reading response");
        }

        throw new IOException(
            String.format(
                "Bad lookup delete request [%d] : [%s]  Response: [%s]",
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
        Duration.millis(1_000) // TODO: configurable
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

  void deleteAllOnTier(final String tier, final Collection<String> dropLookups)
      throws ExecutionException, InterruptedException, IOException
  {
    if (dropLookups.isEmpty()) {
      LOG.debug("Nothing to drop");
      return;
    }
    final Collection<URL> urls = getAllHostsAnnounceEndpoint(tier);
    final List<ListenableFuture<?>> futures = new ArrayList<>(urls.size());
    for (final URL url : urls) {
      futures.add(executorService.submit(new Runnable()
      {
        @Override
        public void run()
        {
          for (final String drop : dropLookups) {
            final URL lookupURL;
            try {
              lookupURL = new URL(
                  url.getProtocol(),
                  url.getHost(),
                  url.getPort(),
                  String.format("%s/%s", url.getFile(), drop)
              );
            }
            catch (MalformedURLException e) {
              throw new ISE(e, "Error creating url for [%s]/[%s]", url, drop);
            }
            try {
              deleteOnHost(lookupURL);
            }
            catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              LOG.warn("Delete [%s] interrupted", lookupURL);
              throw Throwables.propagate(e);
            }
            catch (IOException | ExecutionException e) {
              // Don't raise as ExecutionException. Just log and continue
              LOG.error(e, "Error submitting to [%s]", lookupURL);
            }
          }
        }
      }));
    }
    final ListenableFuture allFuture = Futures.allAsList(futures);
    try {
      allFuture.get(10_000, TimeUnit.MILLISECONDS);
    }
    catch (TimeoutException e) {
      // This should cause Interrupted exceptions on the offending ones
      allFuture.cancel(true);
      throw new ExecutionException("Timeout in updating hosts! Attempting to cancel", e);
    }
  }

  void updateAllNewOnTier(final String tier, final Map<String, Map<String, Object>> knownLookups)
      throws InterruptedException, ExecutionException, IOException
  {
    final Collection<URL> urls = Collections2.transform(
        listenerDiscoverer.getNewNodes(LookupExtractionModule.getTierListenerPath(tier)),
        HOST_TO_URL
    );
    if (urls.isEmpty() || knownLookups.isEmpty()) {
      LOG.debug("Nothing new to report");
      return;
    }
    updateNodes(urls, knownLookups);
  }

  void updateAllOnTier(final String tier, final Map<String, Map<String, Object>> knownLookups)
      throws InterruptedException, ExecutionException, IOException
  {
    updateNodes(getAllHostsAnnounceEndpoint(tier), knownLookups);
  }

  void updateNodes(Collection<URL> urls, final Map<String, Map<String, Object>> knownLookups)
      throws IOException, InterruptedException, ExecutionException
  {
    if (knownLookups == null) {
      LOG.debug("No config for lookup namespaces found");
      return;
    }
    if (knownLookups.isEmpty()) {
      LOG.debug("No known namespaces. Skipping update");
      return;
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Updating %d lookups on %d nodes", knownLookups.size(), urls.size());
    }
    final List<ListenableFuture<?>> futures = new ArrayList<>(urls.size());
    for (final URL url : urls) {
      futures.add(executorService.submit(new Runnable()
      {
        @Override
        public void run()
        {
          try {
            updateAllOnHost(url, knownLookups);
          }
          catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("Update on [%s] interrupted", url);
            throw Throwables.propagate(e);
          }
          catch (IOException | ExecutionException e) {
            // Don't raise as ExecutionException. Just log and continue
            LOG.error(e, "Error submitting to [%s]", url);
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

  Collection<URL> getAllHostsAnnounceEndpoint(final String tier) throws IOException
  {
    return ImmutableList.copyOf(
        Collections2.filter(
            Collections2.transform(
                listenerDiscoverer.getNodes(LookupExtractionModule.getTierListenerPath(tier)),
                HOST_TO_URL
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

  public void updateLookup(
      final String tier,
      final String lookupName,
      Map<String, Object> spec,
      final AuditInfo auditInfo
  )
  {
    updateLookups(
        ImmutableMap.<String, Map<String, Map<String, Object>>>of(tier, ImmutableMap.of(lookupName, spec)),
        auditInfo
    );
  }

  public synchronized void updateLookups(final Map<String, Map<String, Map<String, Object>>> spec, AuditInfo auditInfo)
  {
    final Map<String, Map<String, Map<String, Object>>> prior = getKnownLookups();
    if (prior == null && !spec.isEmpty()) {
      // To prevent accidentally erasing configs if we haven't updated our cache of the values
      throw new ISE("Not initialized. If this is the first namespace, post an empty map to initialize");
    }
    final Map<String, Map<String, Map<String, Object>>> update = prior == null ? spec : new HashMap<>(prior);
    update.putAll(spec);
    configManager.set(LOOKUP_CONFIG_KEY, update, auditInfo);
  }

  public Map<String, Map<String, Map<String, Object>>> getKnownLookups()
  {
    if (!started) {
      throw new ISE("Not started");
    }
    return lookupMapConfigRef.get();
  }

  public synchronized boolean deleteLookup(final String tier, final String lookup, AuditInfo auditInfo)
  {
    final Map<String, Map<String, Map<String, Object>>> prior = getKnownLookups();
    if (prior == null) {
      LOG.warn("Requested delete lookup [%s]/[%s]. But no lookups exist!", tier, lookup);
      return false;
    }
    final Map<String, Map<String, Map<String, Object>>> update = new HashMap<>(prior);
    final Map<String, Map<String, Object>> tierMap = update.get(tier);
    if (tierMap == null) {
      LOG.warn("Requested delete of lookup [%s]/[%s] but tier does not exist!", tier, lookup);
      return false;
    }

    if (!tierMap.containsKey(lookup)) {
      LOG.warn("Requested delete of lookup [%s]/[%s] but lookup does not exist!", tier, lookup);
      return false;
    }

    final Map<String, Map<String, Object>> newTierMap = new HashMap<>(tierMap);
    newTierMap.remove(lookup);
    update.put(tier, newTierMap);
    configManager.set(LOOKUP_CONFIG_KEY, update, auditInfo);
    return true;
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
  Map<String, Object> getLookup(@NotNull final String tier, @NotNull final String lookupName)
  {
    final Map<String, Map<String, Map<String, Object>>> prior = getKnownLookups();
    if (prior == null) {
      LOG.warn("Requested tier [%s] lookupName [%s]. But no namespaces exist!", tier, lookupName);
      return null;
    }
    final Map<String, Map<String, Object>> tierLookups = prior.get(tier);
    if (tierLookups == null) {
      LOG.warn("Tier [%s] does not exist", tier);
      return null;
    }
    return tierLookups.get(lookupName);
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
          new TypeReference<Map<String, Map<String, Map<String, Object>>>>()
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
              final Map<String, Map<String, Map<String, Object>>> allLookupTiers = lookupMapConfigRef.get();
              // Sanity check for if we are shutting down
              if (Thread.currentThread().isInterrupted()) {
                LOG.info("Not updating namespace lookups because process was interrupted");
                return;
              }
              if (!started) {
                LOG.info("Not started. Returning");
                return;
              }
              for (final String tier : allLookupTiers.keySet()) {
                try {
                  final Map<String, Map<String, Object>> allLookups = allLookupTiers.get(tier);
                  final Map<String, Map<String, Object>> oldLookups = prior_update.get(tier);
                  final Collection<String> drops;
                  if (oldLookups == null) {
                    drops = ImmutableList.of();
                  } else {
                    drops = Sets.difference(oldLookups.keySet(), allLookups.keySet());
                  }
                  if (allLookupTiers == prior_update) {
                    LOG.debug("No updates");
                    updateAllNewOnTier(tier, allLookups);
                  } else {
                    updateAllOnTier(tier, allLookups);
                    deleteAllOnTier(tier, drops);
                  }
                }
                catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  throw Throwables.propagate(e);
                }
                catch (Exception e) {
                  LOG.error(e, "Error updating namespaces for tier [%s]. Will try again soon", tier);
                }
              }
              prior_update = allLookupTiers;
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

  static URL getLookupsURL(HostAndPort druidNode) throws MalformedURLException
  {
    return new URL(
        "http",
        druidNode.getHostText(),
        druidNode.getPortOrDefault(-1),
        ListenerResource.BASE_PATH + "/" + LOOKUP_LISTEN_ANNOUNCE_KEY
    );
  }
}
