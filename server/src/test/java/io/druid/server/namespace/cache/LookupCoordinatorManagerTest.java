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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.SettableFuture;
import com.metamx.common.ISE;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.Request;
import com.metamx.http.client.response.HttpResponseHandler;
import com.metamx.http.client.response.SequenceInputStreamResponseHandler;
import io.druid.audit.AuditInfo;
import io.druid.common.config.JacksonConfigManager;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.server.DruidNode;
import io.druid.server.listener.announcer.ListenerDiscoverer;
import io.druid.server.listener.resource.ListenerResource;
import org.easymock.EasyMock;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class LookupCoordinatorManagerTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();
  private final ObjectMapper mapper = new DefaultObjectMapper();
  private final ListenerDiscoverer discoverer = EasyMock.createStrictMock(ListenerDiscoverer.class);
  private final HttpClient client = EasyMock.createStrictMock(HttpClient.class);
  private final JacksonConfigManager configManager = EasyMock.createStrictMock(JacksonConfigManager.class);

  private static final String SINGLE_LOOKUP_NAME = "lookupName";
  private static final Map<String, Object> SINGLE_LOOKUP_SPEC = ImmutableMap.<String, Object>of(
      "some property",
      "some value"
  );
  private static final Map<String, Map<String, Object>> SINGLE_LOOKUP_MAP = ImmutableMap.<String, Map<String, Object>>of(
      SINGLE_LOOKUP_NAME,
      SINGLE_LOOKUP_SPEC
  );

  @Test
  public void testUpdateAllOnHost() throws Exception
  {
    final HttpResponseHandler<InputStream, InputStream> responseHandler = EasyMock.createStrictMock(HttpResponseHandler.class);

    final URL url = LookupCoordinatorManager.getLookupsURL(new DruidNode("some service", "localhost", -1));
    final SettableFuture<InputStream> future = SettableFuture.create();
    future.set(new ByteArrayInputStream(new byte[0]));
    EasyMock.expect(client.go(
        EasyMock.<Request>anyObject(),
        EasyMock.<SequenceInputStreamResponseHandler>anyObject(),
        EasyMock.<Duration>anyObject()
    )).andReturn(future).once();

    EasyMock.replay(client, responseHandler);

    final LookupCoordinatorManager manager = new LookupCoordinatorManager(
        client,
        discoverer,
        mapper,
        configManager
    )
    {
      @Override
      HttpResponseHandler<InputStream, InputStream> makeResponseHandler(
          final AtomicInteger returnCode,
          final AtomicReference<String> reasonString
      )
      {
        returnCode.set(200);
        reasonString.set("");
        return responseHandler;
      }
    };
    manager.updateAllOnHost(
        url,
        SINGLE_LOOKUP_MAP
    );

    EasyMock.verify(client, responseHandler);
  }


  @Test
  public void testUpdateAllOnHostException() throws Exception
  {
    final HttpResponseHandler<InputStream, InputStream> responseHandler = EasyMock.createStrictMock(HttpResponseHandler.class);

    final URL url = LookupCoordinatorManager.getLookupsURL(new DruidNode("some service", "localhost", -1));
    final SettableFuture<InputStream> future = SettableFuture.create();
    future.set(new ByteArrayInputStream(new byte[0]));
    EasyMock.expect(client.go(
        EasyMock.<Request>anyObject(),
        EasyMock.<SequenceInputStreamResponseHandler>anyObject(),
        EasyMock.<Duration>anyObject()
    )).andReturn(future).once();

    EasyMock.replay(client, responseHandler);

    final LookupCoordinatorManager manager = new LookupCoordinatorManager(
        client,
        discoverer,
        mapper,
        configManager
    )
    {
      @Override
      HttpResponseHandler<InputStream, InputStream> makeResponseHandler(
          final AtomicInteger returnCode,
          final AtomicReference<String> reasonString
      )
      {
        returnCode.set(500);
        reasonString.set("");
        return responseHandler;
      }
    };
    expectedException.expect(new BaseMatcher<Throwable>()
    {
      @Override
      public boolean matches(Object o)
      {
        return o instanceof IOException && ((IOException) o).getMessage().startsWith("Bad update request");
      }

      @Override
      public void describeTo(Description description)
      {

      }
    });
    try {
      manager.updateAllOnHost(
          url,
          SINGLE_LOOKUP_MAP
      );
    }
    finally {
      EasyMock.verify(client, responseHandler);
    }
  }

  @Test
  public void testParseErrorUpdateAllOnHost() throws Exception
  {

    final AtomicReference<List<Map<String, Object>>> configVal = new AtomicReference<>(null);

    final URL url = LookupCoordinatorManager.getLookupsURL(new DruidNode("some service", "localhost", -1));

    EasyMock.reset(configManager);
    EasyMock.expect(configManager.watch(EasyMock.anyString(), EasyMock.<TypeReference>anyObject()))
            .andReturn(configVal);

    final JsonProcessingException ex = EasyMock.createStrictMock(JsonProcessingException.class);

    final ObjectMapper mapper = EasyMock.createStrictMock(ObjectMapper.class);
    EasyMock.expect(mapper.writeValueAsBytes(EasyMock.eq(SINGLE_LOOKUP_MAP))).andThrow(ex);

    expectedException.expectCause(new BaseMatcher<Throwable>()
    {
      @Override
      public boolean matches(Object o)
      {
        return ex == o;
      }

      @Override
      public void describeTo(Description description)
      {

      }
    });

    EasyMock.replay(mapper);

    final LookupCoordinatorManager manager = new LookupCoordinatorManager(
        client,
        discoverer,
        mapper,
        configManager
    );
    try {
      manager.updateAllOnHost(
          url,
          SINGLE_LOOKUP_MAP
      );
    }
    finally {
      EasyMock.verify(mapper);
    }
  }


  @Test
  public void testUpdateAll() throws Exception
  {
    final List<URL> urls = ImmutableList.of(new URL("http://foo.bar"));
    final LookupCoordinatorManager manager = new LookupCoordinatorManager(
        client,
        discoverer,
        mapper,
        configManager
    )
    {
      @Override
      Collection<URL> getAllHostsAnnounceEndpoint()
      {
        return urls;
      }

      @Override
      void updateAllOnHost(final URL url, Map<String, Map<String, Object>> updatedLookups)
          throws IOException, InterruptedException, ExecutionException
      {
        if (!urls.get(0).equals(url) || updatedLookups != SINGLE_LOOKUP_MAP) {
          throw new RuntimeException("Not matched");
        }
      }
    };
    // Should be no-ops
    manager.updateAll(null);
    manager.updateAll(ImmutableMap.<String, Map<String, Object>>of());
    manager.updateAll(SINGLE_LOOKUP_MAP);
  }


  @Test
  public void testUpdateAllIOException() throws Exception
  {
    final IOException ex = new IOException("test exception");
    final List<URL> urls = ImmutableList.of(new URL("http://foo.bar"));
    final LookupCoordinatorManager manager = new LookupCoordinatorManager(
        client,
        discoverer,
        mapper,
        configManager
    )
    {
      @Override
      Collection<URL> getAllHostsAnnounceEndpoint()
      {
        return urls;
      }

      @Override
      void updateAllOnHost(final URL url, Map<String, Map<String, Object>> updatedLookups)
          throws IOException, InterruptedException, ExecutionException
      {
        throw ex;
      }
    };
    // Should log and pass io exception
    manager.updateAll(SINGLE_LOOKUP_MAP);
  }

  @Test
  public void testUpdateAllInterrupted() throws Exception
  {
    final InterruptedException ex = new InterruptedException("interruption test");
    final List<URL> urls = ImmutableList.of(new URL("http://foo.bar"));
    final LookupCoordinatorManager manager = new LookupCoordinatorManager(
        client,
        discoverer,
        mapper,
        configManager
    )
    {
      @Override
      Collection<URL> getAllHostsAnnounceEndpoint()
      {
        return urls;
      }

      @Override
      void updateAllOnHost(final URL url, Map<String, Map<String, Object>> knownNamespaces)
          throws IOException, InterruptedException, ExecutionException
      {
        throw ex;
      }
    };
    expectedException.expectCause(new BaseMatcher<Throwable>()
    {
      @Override
      public boolean matches(Object o)
      {
        return o instanceof RuntimeException && ((RuntimeException) o).getCause() == ex;
      }

      @Override
      public void describeTo(Description description)
      {

      }
    });
    try {
      manager.updateAll(SINGLE_LOOKUP_MAP);
    }
    finally {
      // Clear status
      Thread.interrupted();
    }
  }

  @Test
  public void testGetAllHostsAnnounceEndpoint() throws Exception
  {
    final LookupCoordinatorManager manager = new LookupCoordinatorManager(
        client,
        discoverer,
        mapper,
        configManager
    );

    EasyMock.expect(discoverer.getNodes(EasyMock.eq(LookupCoordinatorManager.LOOKUP_LISTEN_ANNOUNCE_KEY)))
            .andReturn(ImmutableList.<DruidNode>of())
            .once();
    EasyMock.replay(discoverer);
    Assert.assertEquals(ImmutableList.of(), manager.getAllHostsAnnounceEndpoint());
    EasyMock.verify(discoverer);
    EasyMock.reset(discoverer);

    EasyMock.expect(discoverer.getNodes(EasyMock.eq(LookupCoordinatorManager.LOOKUP_LISTEN_ANNOUNCE_KEY)))
            .andReturn(Collections.<DruidNode>singletonList(null))
            .once();
    EasyMock.replay(discoverer);
    Assert.assertEquals(ImmutableList.of(), manager.getAllHostsAnnounceEndpoint());
    EasyMock.verify(discoverer);
    EasyMock.reset(discoverer);

    final DruidNode node = new DruidNode("some service", "someHost", 8888);
    EasyMock.expect(discoverer.getNodes(EasyMock.eq(LookupCoordinatorManager.LOOKUP_LISTEN_ANNOUNCE_KEY)))
            .andReturn(ImmutableList.of(node, new DruidNode("some service", "someHost", -2)))
            .once();
    EasyMock.replay(discoverer);
    Assert.assertEquals(
        ImmutableList.of(LookupCoordinatorManager.getLookupsURL(node)),
        manager.getAllHostsAnnounceEndpoint()
    );
    EasyMock.verify(discoverer);
    EasyMock.reset(discoverer);
  }

  @Test
  public void testGetNamespaceURL() throws Exception
  {
    final String path = ListenerResource.BASE_PATH + "/" + LookupCoordinatorManager.LOOKUP_LISTEN_ANNOUNCE_KEY;
    Assert.assertEquals(
        new URL("http", "someHost", 1, path),
        LookupCoordinatorManager.getLookupsURL(
            new DruidNode("servicename", "someHost", 1)
        )
    );
    Assert.assertEquals(
        new URL("http", "someHost", -1, path),
        LookupCoordinatorManager.getLookupsURL(
            new DruidNode("servicename", "someHost", -1)
        )
    );

    Assert.assertEquals(
        new URL("http", "::1", -1, path),
        LookupCoordinatorManager.getLookupsURL(
            new DruidNode("servicename", "::1", -1)
        )
    );
    Assert.assertEquals(
        new URL("http", "[::1]", -1, path),
        LookupCoordinatorManager.getLookupsURL(
            new DruidNode("servicename", "[::1]", -1)
        )
    );
    Assert.assertEquals(
        new URL("http", "::1", -1, path),
        LookupCoordinatorManager.getLookupsURL(
            new DruidNode("servicename", "::1", -1)
        )
    );
  }

  @Test
  public void testGetNamespaceBadURL() throws Exception
  {
    expectedException.expect(MalformedURLException.class);
    LookupCoordinatorManager.getLookupsURL(
        new DruidNode("servicename", "localhost", -2)
    );
  }

  @Test
  public void testUpdateLookupAdds() throws Exception
  {
    final LookupCoordinatorManager manager = new LookupCoordinatorManager(
        client,
        discoverer,
        mapper,
        configManager
    )
    {
      @Override
      public Map<String, Map<String, Object>> getKnownLookups()
      {
        return ImmutableMap.of();
      }
    };
    final AuditInfo auditInfo = new AuditInfo("author", "comment", "localhost");
    EasyMock.reset(configManager);
    EasyMock.expect(configManager.set(
        EasyMock.eq(LookupCoordinatorManager.LOOKUP_CONFIG_KEY),
        EasyMock.eq(SINGLE_LOOKUP_MAP),
        EasyMock.eq(auditInfo)
    )).andReturn(true).once();
    EasyMock.replay(configManager);
    manager.updateLookup(SINGLE_LOOKUP_NAME, SINGLE_LOOKUP_SPEC, auditInfo);
    EasyMock.verify(configManager);
  }


  @Test
  public void testUpdateNamespaceFailsUnitialized() throws Exception
  {
    final LookupCoordinatorManager manager = new LookupCoordinatorManager(
        client,
        discoverer,
        mapper,
        configManager
    )
    {
      @Override
      public Map<String, Map<String, Object>> getKnownLookups()
      {
        return null;
      }
    };
    final AuditInfo auditInfo = new AuditInfo("author", "comment", "localhost");
    expectedException.expect(ISE.class);
    manager.updateLookups(SINGLE_LOOKUP_MAP, auditInfo);
  }

  @Test
  public void testUpdateNamespaceUpdates() throws Exception
  {
    final Map<String, Object> ignore = ImmutableMap.<String, Object>of("prop", "old");
    final LookupCoordinatorManager manager = new LookupCoordinatorManager(
        client,
        discoverer,
        mapper,
        configManager
    )
    {
      @Override
      public Map<String, Map<String, Object>> getKnownLookups()
      {
        return ImmutableMap.<String, Map<String, Object>>of(
            "foo", ImmutableMap.<String, Object>of("prop", "old"),
            "ignore", ignore
        );
      }
    };
    final Map<String, Object> newSpec = ImmutableMap.<String, Object>of(
        "prop",
        "new"
    );
    final Map<String, Map<String, Object>> namespace = ImmutableMap.<String, Map<String, Object>>of(
        "foo",
        newSpec
    );
    final AuditInfo auditInfo = new AuditInfo("author", "comment", "localhost");
    EasyMock.reset(configManager);
    EasyMock.expect(configManager.set(
        EasyMock.eq(LookupCoordinatorManager.LOOKUP_CONFIG_KEY),
        EasyMock.eq(ImmutableMap.of(
            "foo", newSpec,
            "ignore", ignore
        )),
        EasyMock.eq(auditInfo)
    )).andReturn(true).once();
    EasyMock.replay(configManager);
    manager.updateLookups(namespace, auditInfo);
    EasyMock.verify(configManager);
  }

  @Test
  public void testDeleteNamespace() throws Exception
  {
    final Map<String, Object> ignore = ImmutableMap.<String, Object>of("namespace", "ignore");
    final Map<String, Object> namespace = ImmutableMap.<String, Object>of("namespace", "foo");
    final LookupCoordinatorManager manager = new LookupCoordinatorManager(
        client,
        discoverer,
        mapper,
        configManager
    )
    {
      @Override
      public Map<String, Map<String, Object>> getKnownLookups()
      {
        return ImmutableMap.of(
            "foo", namespace,
            "ignore", ignore
        );
      }
    };
    final AuditInfo auditInfo = new AuditInfo("author", "comment", "localhost");
    EasyMock.reset(configManager);
    EasyMock.expect(configManager.set(
        EasyMock.eq(LookupCoordinatorManager.LOOKUP_CONFIG_KEY),
        EasyMock.eq(ImmutableMap.of(
            "ignore", ignore
        )),
        EasyMock.eq(auditInfo)
    )).andReturn(true).once();
    EasyMock.replay(configManager);
    Assert.assertTrue(manager.deleteLookup("foo", auditInfo));
    EasyMock.verify(configManager);
  }

  @Test
  public void testDeleteNamespaceIgnoresMissing() throws Exception
  {
    final Map<String, Object> ignore = ImmutableMap.<String, Object>of("namespace", "ignore");
    final LookupCoordinatorManager manager = new LookupCoordinatorManager(
        client,
        discoverer,
        mapper,
        configManager
    )
    {
      @Override
      public Map<String, Map<String, Object>> getKnownLookups()
      {
        return ImmutableMap.of(
            "ignore", ignore
        );
      }
    };
    final AuditInfo auditInfo = new AuditInfo("author", "comment", "localhost");
    Assert.assertFalse(manager.deleteLookup("foo", auditInfo));
  }

  @Test
  public void testDeleteNamespaceIgnoresNotReady() throws Exception
  {
    final Map<String, Object> ignore = ImmutableMap.<String, Object>of();
    final LookupCoordinatorManager manager = new LookupCoordinatorManager(
        client,
        discoverer,
        mapper,
        configManager
    )
    {
      @Override
      public Map<String, Map<String, Object>> getKnownLookups()
      {
        return null;
      }
    };
    final AuditInfo auditInfo = new AuditInfo("author", "comment", "localhost");
    Assert.assertFalse(manager.deleteLookup("foo", auditInfo));
  }

  @Test
  public void testGetNamespace() throws Exception
  {
    final Map<String, Object> namespace = ImmutableMap.<String, Object>of("namespace", "foo");
    final LookupCoordinatorManager manager = new LookupCoordinatorManager(
        client,
        discoverer,
        mapper,
        configManager
    )
    {
      @Override
      public Map<String, Map<String, Object>> getKnownLookups()
      {
        return ImmutableMap.of("foo", namespace);
      }
    };
    Assert.assertEquals(namespace, manager.getLookup("foo"));
    Assert.assertNull(manager.getLookup("does not exit"));
  }


  @Test
  public void testGetNamespaceIgnoresMalformed() throws Exception
  {
    final Map<String, Object> namespace = ImmutableMap.<String, Object>of("namespace", "foo");
    final LookupCoordinatorManager manager = new LookupCoordinatorManager(
        client,
        discoverer,
        mapper,
        configManager
    )
    {
      @Override
      public Map<String, Map<String, Object>> getKnownLookups()
      {
        return ImmutableMap.of(
            "foo", namespace,
            "bar", ImmutableMap.<String, Object>of()
        );
      }
    };
    Assert.assertEquals(namespace, manager.getLookup("foo"));
    Assert.assertNull(manager.getLookup("does not exit"));
  }

  @Test
  public void testGetNamespaceIgnoresNotReady() throws Exception
  {
    final LookupCoordinatorManager manager = new LookupCoordinatorManager(
        client,
        discoverer,
        mapper,
        configManager
    )
    {
      @Override
      public Map<String, Map<String, Object>> getKnownLookups()
      {
        return null;
      }
    };
    Assert.assertNull(manager.getLookup("foo"));
  }

  @Test
  public void testStart() throws Exception
  {
    final AtomicReference<List<Map<String, Object>>> namespaceRef = new AtomicReference<>(null);

    EasyMock.reset(configManager);
    EasyMock.expect(configManager.watch(
        EasyMock.eq(LookupCoordinatorManager.LOOKUP_CONFIG_KEY),
        EasyMock.<TypeReference>anyObject(),
        EasyMock.<AtomicReference>isNull()
    )).andReturn(namespaceRef).once();
    EasyMock.replay(configManager);

    final LookupCoordinatorManager manager = new LookupCoordinatorManager(
        client,
        discoverer,
        mapper,
        configManager
    );
    manager.start();
    manager.start();
    Assert.assertNull(manager.getKnownLookups());
    EasyMock.verify(configManager);
  }

  @Test
  public void testStop() throws Exception
  {
    final AtomicReference<List<Map<String, Object>>> namespaceRef = new AtomicReference<>(null);

    EasyMock.reset(configManager);
    EasyMock.expect(configManager.watch(
        EasyMock.eq(LookupCoordinatorManager.LOOKUP_CONFIG_KEY),
        EasyMock.<TypeReference>anyObject(),
        EasyMock.<AtomicReference>isNull()
    )).andReturn(namespaceRef).once();
    EasyMock.replay(configManager);

    final LookupCoordinatorManager manager = new LookupCoordinatorManager(
        client,
        discoverer,
        mapper,
        configManager
    );
    manager.start();
    manager.stop();
    manager.stop();
    EasyMock.verify(configManager);
  }

  @Test
  public void testStartTooMuch() throws Exception
  {
    final AtomicReference<List<Map<String, Object>>> namespaceRef = new AtomicReference<>(null);

    EasyMock.reset(configManager);
    EasyMock.expect(configManager.watch(
        EasyMock.eq(LookupCoordinatorManager.LOOKUP_CONFIG_KEY),
        EasyMock.<TypeReference>anyObject(),
        EasyMock.<AtomicReference>isNull()
    )).andReturn(namespaceRef).once();
    EasyMock.replay(configManager);

    final LookupCoordinatorManager manager = new LookupCoordinatorManager(
        client,
        discoverer,
        mapper,
        configManager
    );
    manager.start();
    manager.stop();
    expectedException.expect(new BaseMatcher<Throwable>()
    {
      @Override
      public boolean matches(Object o)
      {
        return o instanceof ISE && ((ISE) o).getMessage().equals("Cannot restart after stop!");
      }

      @Override
      public void describeTo(Description description)
      {

      }
    });
    try {
      manager.start();
    }
    finally {
      EasyMock.verify(configManager);
    }
  }
}
