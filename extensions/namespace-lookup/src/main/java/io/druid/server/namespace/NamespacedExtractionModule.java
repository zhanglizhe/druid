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

package io.druid.server.namespace;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.api.client.repackaged.com.google.common.base.Throwables;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Key;
import com.google.inject.Provider;
import com.google.inject.Provides;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.name.Named;
import com.metamx.common.IAE;
import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.common.logger.Logger;
import io.druid.curator.announcement.Announcer;
import io.druid.guice.Jerseys;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.LazySingleton;
import io.druid.guice.LifecycleModule;
import io.druid.guice.ManageLifecycle;
import io.druid.guice.PolyBind;
import io.druid.guice.annotations.Json;
import io.druid.initialization.DruidModule;
import io.druid.query.extraction.NamespacedExtractor;
import io.druid.query.extraction.namespace.ExtractionNamespace;
import io.druid.query.extraction.namespace.ExtractionNamespaceFunctionFactory;
import io.druid.query.extraction.namespace.JDBCExtractionNamespace;
import io.druid.query.extraction.namespace.URIExtractionNamespace;
import io.druid.server.coordination.DruidServerMetadata;
import io.druid.server.initialization.NamespaceLookupConfig;
import io.druid.server.namespace.announcer.listener.AnnouncementListenerResource;
import io.druid.server.initialization.ZkPathsConfig;
import io.druid.server.namespace.cache.NamespaceExtractionCacheManager;
import io.druid.server.namespace.cache.OffHeapNamespaceExtractionCacheManager;
import io.druid.server.namespace.cache.OnHeapNamespaceExtractionCacheManager;
import org.apache.curator.utils.ZKPaths;

import javax.annotation.Nullable;
import javax.ws.rs.core.Response;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 *
 */
public class NamespacedExtractionModule implements DruidModule
{
  private static final Logger log = new Logger(NamespacedExtractionModule.class);
  private static final String TYPE_PREFIX = "druid.query.extraction.namespace.cache.type";
  private final ConcurrentMap<String, Function<String, String>> fnCache = new ConcurrentHashMap<>();

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return ImmutableList.<Module>of(
        new SimpleModule("DruidNamespacedExtractionModule")
        {
          @Override
          public void setupModule(SetupContext context)
          {
            context.registerSubtypes(NamespacedExtractor.class);
            context.registerSubtypes(ExtractionNamespace.class);
          }
        }
    );
  }

  public static MapBinder<Class<? extends ExtractionNamespace>, ExtractionNamespaceFunctionFactory<?>> getNamespaceFactoryMapBinder(
      final Binder binder
  )
  {
    return MapBinder.newMapBinder(
        binder,
        new TypeLiteral<Class<? extends ExtractionNamespace>>()
        {
        },
        new TypeLiteral<ExtractionNamespaceFunctionFactory<?>>()
        {
        }
    );
  }

  @Override
  public void configure(Binder binder)
  {
    PolyBind.createChoiceWithDefault(
        binder,
        TYPE_PREFIX,
        Key.get(NamespaceExtractionCacheManager.class),
        Key.get(OnHeapNamespaceExtractionCacheManager.class),
        "onheap"
    ).in(LazySingleton.class);

    PolyBind
        .optionBinder(binder, Key.get(NamespaceExtractionCacheManager.class))
        .addBinding("offheap")
        .to(OffHeapNamespaceExtractionCacheManager.class)
        .in(LazySingleton.class);

    getNamespaceFactoryMapBinder(binder)
        .addBinding(JDBCExtractionNamespace.class)
        .to(JDBCExtractionNamespaceFunctionFactory.class)
        .in(LazySingleton.class);
    getNamespaceFactoryMapBinder(binder)
        .addBinding(URIExtractionNamespace.class)
        .to(URIExtractionNamespaceFunctionFactory.class)
        .in(LazySingleton.class);

    AnnouncementListenerResource
        .getPOSTHandlerMap(binder)
        .addBinding("namespace")
        .toProvider(NamespaceAnnouncementPostHandlerProvider.class)
        .in(LazySingleton.class);
    AnnouncementListenerResource
        .getDELETEHandlerMap(binder)
        .addBinding("namespace")
        .toProvider(NamespaceAnnouncementDeleteHandlerProvider.class)
        .in(LazySingleton.class);
    AnnouncementListenerResource
        .getGETHandlerMap(binder)
        .addBinding("namespace")
        .toProvider(NamespaceAnnouncementGetHandlerProvider.class)
        .in(LazySingleton.class);

    JsonConfigProvider.bind(binder, "druid.zk.paths", NamespaceLookupConfig.class);

    LifecycleModule.register(binder, NamespaceProcessorAnnouncer.class);

    Jerseys.addResource(binder, AnnouncementListenerResource.class);
  }

  public static class NamespaceAnnouncementGetHandlerProvider
      implements Provider<AnnouncementListenerResource.AnnouncementIDHandler>
  {
    @Inject
    public NamespaceAnnouncementGetHandlerProvider(
        final NamespaceExtractionCacheManager manager
    )
    {
      this.manager = manager;
    }

    private final NamespaceExtractionCacheManager manager;

    @Override
    public AnnouncementListenerResource.AnnouncementIDHandler get()
    {
      return new AnnouncementListenerResource.AnnouncementIDHandler()
      {
        @Override
        public Response handle(String id)
        {
          if (manager.getKnownNamespaces().contains(id)) {
            return Response.ok().build();
          } else {
            return Response.status(Response.Status.NOT_FOUND).build();
          }
        }
      };
    }
  }

  public static class NamespaceAnnouncementDeleteHandlerProvider
      implements Provider<AnnouncementListenerResource.AnnouncementIDHandler>
  {
    private final NamespaceExtractionCacheManager manager;

    @Inject
    public NamespaceAnnouncementDeleteHandlerProvider(
        final NamespaceExtractionCacheManager manager
    )
    {
      this.manager = manager;
    }

    @Override
    public AnnouncementListenerResource.AnnouncementIDHandler get()
    {
      return new AnnouncementListenerResource.AnnouncementIDHandler()
      {
        @Override
        public Response handle(String id)
        {
          if (manager.delete(id)) {
            return Response.ok().build();
          } else {
            return Response.status(Response.Status.NOT_FOUND).build();
          }
        }
      };
    }
  }

  public static class NamespaceAnnouncementPostHandlerProvider
      implements Provider<AnnouncementListenerResource.AnnouncementPOSTHandler>
  {
    private final NamespaceExtractionCacheManager manager;

    @Inject
    public NamespaceAnnouncementPostHandlerProvider(
        final NamespaceExtractionCacheManager manager
    )
    {
      this.manager = manager;
    }

    @Override
    public AnnouncementListenerResource.AnnouncementPOSTHandler get()
    {
      return new AnnouncementListenerResource.AbstractAnnouncementPOSTHandler<ExtractionNamespace, Void>(
          new TypeReference<ExtractionNamespace>()
          {
          }
      )
      {
        @Override
        public Void handle(final List<ExtractionNamespace> inputObject) throws Exception
        {
          manager.scheduleOrUpdate(inputObject);
          return null;
        }
      };
    }
  }

  @Provides
  @Named("namespaceVersionMap")
  @LazySingleton
  public ConcurrentMap<String, String> getVersionMap()
  {
    return new ConcurrentHashMap<>();
  }

  @Provides
  @Named("namespaceExtractionFunctionCache")
  public ConcurrentMap<String, Function<String, String>> getFnCache()
  {
    return fnCache;
  }

  @Provides
  @Named("dimExtractionNamespace")
  @LazySingleton
  public Function<String, Function<String, String>> getFunctionMaker(
      @Named("namespaceExtractionFunctionCache")
      final ConcurrentMap<String, Function<String, String>> fnCache
  )
  {
    return new Function<String, Function<String, String>>()
    {
      @Nullable
      @Override
      public Function<String, String> apply(final String namespace)
      {
        Function<String, String> fn = fnCache.get(namespace);
        if (fn == null) {
          throw new IAE("Namespace [%s] not found", namespace);
        }
        return fn;
      }
    };
  }

  @ManageLifecycle
  public static class NamespaceProcessorAnnouncer
  {
    private final Announcer announcer;
    private final String path;
    private final byte[] payload;

    @Inject
    public NamespaceProcessorAnnouncer(
        final Announcer announcer,
        final DruidServerMetadata node,
        final NamespaceLookupConfig namespaceLookupConfig,
        final @Json ObjectMapper mapper
    )
    {
      this.announcer = announcer;
      this.path = ZKPaths.makePath(namespaceLookupConfig.getNamespacesPath(), node.getName());
      try {
        this.payload = mapper.writeValueAsBytes(node);
      }
      catch (JsonProcessingException e) {
        throw Throwables.propagate(e);
      }
    }

    @LifecycleStart
    public void start()
    {
      announcer.announce(path, payload);
    }

    @LifecycleStop
    public void stop()
    {
      announcer.unannounce(path);
    }
  }
}
