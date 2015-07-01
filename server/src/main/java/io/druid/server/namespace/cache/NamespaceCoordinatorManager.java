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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.google.api.client.http.HttpStatusCodes;
import com.google.api.client.repackaged.com.google.common.base.Strings;
import com.google.api.client.repackaged.com.google.common.base.Throwables;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.io.CharSource;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.metamx.common.StringUtils;
import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.common.logger.Logger;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.Request;
import com.metamx.http.client.io.AppendableByteArrayInputStream;
import com.metamx.http.client.response.ClientResponse;
import com.metamx.http.client.response.InputStreamResponseHandler;
import io.druid.concurrent.Execs;
import io.druid.guice.annotations.Global;
import io.druid.guice.annotations.Smile;
import io.druid.metadata.MetadataNamespaceManager;
import io.druid.server.coordination.DruidServerMetadata;
import io.druid.server.initialization.ZkPathsConfig;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.utils.ZKPaths;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponse;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 *
 */
public class NamespaceCoordinatorManager
{
  private static final Logger log = new Logger(NamespaceCoordinatorManager.class);

  public static class NamespaceCoordinatorManagerProvider implements Provider<NamespaceCoordinatorManager>
  {

    private final CuratorFramework curatorFramework;
    private final ZkPathsConfig zkPathsConfig;
    private final HttpClient httpClient;
    private final ObjectMapper smileMapper;
    private final MetadataNamespaceManager metadataNamespaceManager;
    private final ObjectMapper mapper;

    @Inject
    public NamespaceCoordinatorManagerProvider(
        final @Global HttpClient httpClient,
        final ZkPathsConfig zkPathsConfig,
        final CuratorFramework curatorFramework,
        final ObjectMapper mapper,
        final @Smile ObjectMapper smileMapper,
        final MetadataNamespaceManager metadataNamespaceManager
    )
    {
      this.httpClient = httpClient;
      this.zkPathsConfig = zkPathsConfig;
      this.curatorFramework = curatorFramework;
      this.smileMapper = smileMapper;
      this.metadataNamespaceManager = metadataNamespaceManager;
      this.mapper = mapper;
    }

    @Override
    public NamespaceCoordinatorManager get()
    {
      return new NamespaceCoordinatorManager(
          httpClient,
          zkPathsConfig,
          curatorFramework,
          mapper,
          smileMapper,
          metadataNamespaceManager
      );
    }
  }

  private final ListeningExecutorService executorService;
  private final PathChildrenCache pathChildrenCache;
  private final PathChildrenCacheListener pathChildrenCacheListener;
  private final AtomicBoolean started = new AtomicBoolean(false);
  private final CuratorFramework curatorFramework;
  private final String zkPath;
  private final ObjectMapper mapper;
  private final MetadataNamespaceManager metadataNamespaceManager;
  private final HttpClient httpClient;
  private final ObjectMapper smileMapper;

  @Inject
  public NamespaceCoordinatorManager(
      final @Global HttpClient httpClient,
      final ZkPathsConfig zkPathsConfig,
      final CuratorFramework curatorFramework,
      final ObjectMapper mapper,
      final @Smile ObjectMapper smileMapper,
      final MetadataNamespaceManager metadataNamespaceManager
  )
  {
    this.mapper = mapper;
    this.zkPath = zkPathsConfig.getNamespacePath();
    this.curatorFramework = curatorFramework;
    this.metadataNamespaceManager = metadataNamespaceManager;
    this.httpClient = httpClient;
    this.smileMapper = smileMapper;
    executorService = MoreExecutors.listeningDecorator(Execs.singleThreaded("NamespaceCoordinatorManager-%d"));
    pathChildrenCache = new PathChildrenCache(
        curatorFramework,
        zkPath,
        true,
        true,
        executorService
    );
    pathChildrenCacheListener = new PathChildrenCacheListener()
    {
      @Override
      public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception
      {
        if (!metadataNamespaceManager.isLoadedOnce()) {
          // Skip, we'll get it on the next poll
          return;
        }
        log.debug("Event type [%s] received on path [%s]", event.getType(), zkPath);
        switch (event.getType()) {
          case CHILD_ADDED:
          case CONNECTION_RECONNECTED:
            updateAllOnHost(
                getNamespaceURL(
                    mapper.readValue(
                        event.getData().getData(),
                        DruidServerMetadata.class
                    )
                )
            );
            break;
          case CHILD_UPDATED:
            // WTF? why?
            break;
          case CHILD_REMOVED:
            // NOOP
            break;
          case CONNECTION_SUSPENDED:
            // NOOP
            break;
          case CONNECTION_LOST:
            // NOOP
            break;
          case INITIALIZED:
            // NOOP
            break;
        }
      }
    };
  }

  private void updateAllOnHost(final URL url) throws IOException
  {

    final AtomicInteger returnCode = new AtomicInteger(0);
    final AtomicReference<String> reasonString = new AtomicReference<>(null);
    final byte[] bytes;
    try {
      final List<Map<String, Object>> knownNamespaces = metadataNamespaceManager.knownNamespaces();
      log.debug("Loading up %d namespaces", knownNamespaces.size());
      bytes = smileMapper.writeValueAsBytes(knownNamespaces);
    }
    catch (JsonProcessingException e) {
      throw Throwables.propagate(e);
    }
    final InputStream result;
    try {
      result = httpClient.go(
          new Request(HttpMethod.POST, url)
              .addHeader(HttpHeaders.Names.ACCEPT, SmileMediaTypes.APPLICATION_JACKSON_SMILE)
              .addHeader(HttpHeaders.Names.CONTENT_TYPE, SmileMediaTypes.APPLICATION_JACKSON_SMILE)
              .setContent(bytes),
          new InputStreamResponseHandler()
          {
            @Override
            public ClientResponse<AppendableByteArrayInputStream> handleResponse(HttpResponse response)
            {
              returnCode.set(response.getStatus().getCode());
              reasonString.set(response.getStatus().getReasonPhrase());
              return super.handleResponse(response);
            }
          }
      ).get();
    }
    catch (InterruptedException | ExecutionException e) {
      if (e instanceof InterruptedException) {
        log.debug("interrupted");
        Thread.currentThread().interrupt();
      }
      throw Throwables.propagate(e);
    }
    if (!HttpStatusCodes.isSuccess(returnCode.get())) {
      final byte[] resultBytes = new byte[1024];
      int read = 0;
      try {
        while (read < resultBytes.length && read >= 0) {
          final int lastRead = result.read(resultBytes);
          if (lastRead < 0) {
            break;
          }
          read += lastRead;
        }
      }catch(IOException e2){
        log.warn(e2, "Error reading response");
      }

      throw new IOException(
          String.format(
              "Bad update request [%d] : [%s]  Response: [%s]",
              returnCode.get(),
              reasonString.get(),
              StringUtils.fromUtf8(resultBytes)
          )
      );
    } else {
      log.debug("Status: %s reason: [%s]", returnCode.get(), reasonString.get());
    }
  }

  public Collection<URL> getAllHostsAnnounceEndpoint()
  {
    try {
      return
          Collections2.filter(
              Collections2.transform(
                  curatorFramework.getChildren().forPath(zkPath),
                  new Function<String, URL>()
                  {
                    @Nullable
                    @Override
                    public URL apply(String input)
                    {
                      if (Strings.isNullOrEmpty(input)) {
                        log.warn("Empty child for zk path [%s]", zkPath);
                        return null;
                      }
                      final String path = ZKPaths.makePath(zkPath, input);
                      final byte[] data;
                      try {
                        data = curatorFramework.getData().decompressed().forPath(path);
                      }
                      catch (Exception e) {
                        log.error(e, "Error on path [%s]", path);
                        return null;
                      }
                      if (data == null) {
                        // race condition
                        return null;
                      }
                      try {
                        return getNamespaceURL(
                            mapper.readValue(
                                data,
                                DruidServerMetadata.class
                            )
                        );
                      }
                      catch (IOException e) {
                        log.error(e, "Error parsing path [%s] data [%s]", path, StringUtils.fromUtf8(data));
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
          );
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private static URL getNamespaceURL(DruidServerMetadata druidServerMetadata) throws MalformedURLException
  {
    return URI.create(
        String.format(
            "http://%s/druid/announcement/v1/namespace",
            druidServerMetadata.getHost()
        )
    ).toURL();
  }

  @LifecycleStart
  public void start()
  {
    if (started.get()) {
      return;
    }
    pathChildrenCache.getListenable().addListener(pathChildrenCacheListener);
    try {
      pathChildrenCache.start();
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
    metadataNamespaceManager.registerPostPollRunnable(
        new Runnable()
        {
          @Override
          public void run()
          {
            for (URL url : getAllHostsAnnounceEndpoint()) {
              try {
                updateAllOnHost(url);
              }
              catch (IOException e) {
                log.error(e, "Failed on endpoint [%s]", url);
              }
            }
          }
        }
    );
    started.set(true);
  }

  @LifecycleStop
  public void stop()
  {
    started.set(false);
    pathChildrenCache.getListenable().removeListener(pathChildrenCacheListener);
    try {
      pathChildrenCache.close();
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  public void add(final Map<String, Object> map)
  {

  }
}
