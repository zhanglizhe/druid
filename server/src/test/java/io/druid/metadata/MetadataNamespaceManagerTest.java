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
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import io.druid.jackson.DefaultObjectMapper;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

public class MetadataNamespaceManagerTest
{
  private final MetadataStorageTablesConfig tablesConfig = MetadataStorageTablesConfig.fromBase("namespaceTest");
  private final String testNamespace = "testNamespace";
  private final Map<String, Object> testMap = ImmutableMap.<String, Object>of(
      "namespace", testNamespace,
      "foo", true,
      "bar", 0,
      "baz", "bat"
  );
  private TestDerbyConnector connector;
  private MetadataNamespaceManager.SQLMetadataNamespaceManager manager;
  private ObjectMapper jsonMapper;

  @Before
  public void setUp()
  {
    connector = new TestDerbyConnector(
        Suppliers.ofInstance(new MetadataStorageConnectorConfig()),
        Suppliers.ofInstance(tablesConfig)
    );
    jsonMapper = new DefaultObjectMapper();
    manager = new MetadataNamespaceManager.SQLMetadataNamespaceManager(
        jsonMapper,
        Suppliers.ofInstance(new MetadataNamespaceManagerConfig()),
        Suppliers.ofInstance(tablesConfig),
        connector
    );
    connector.createNamespacesTable();
    manager.start();
  }

  @After
  public void tearDown()
  {
    manager.stop();
    manager = null;
    jsonMapper = null;
    connector.tearDown();
    connector = null;
  }

  @Test
  public void simplePollTest()
  {
    Assert.assertNull(manager.getNamespace(testNamespace));
    manager.addOrUpdateNamespace(testNamespace, testMap);
    manager.poll();
    Assert.assertEquals(testMap, manager.getNamespace(testNamespace));
  }

  @Test
  public void pollMissingTest()
  {
    Assert.assertNull(manager.getNamespace(testNamespace));
    manager.poll();
    Assert.assertNull(manager.getNamespace(testNamespace));
    manager.addOrUpdateNamespace(testNamespace, testMap);
    manager.poll();
    Assert.assertEquals(testMap, manager.getNamespace(testNamespace));
  }

  @Test
  public void testDelete()
  {
    Assert.assertNull(manager.getNamespace(testNamespace));
    manager.addOrUpdateNamespace(testNamespace, testMap);
    manager.poll();
    Assert.assertEquals(testMap, manager.getNamespace(testNamespace));
    manager.disableNamespace(testNamespace);
    Assert.assertNull(manager.getNamespace(testNamespace));
    manager.poll();
    Assert.assertNull(manager.getNamespace(testNamespace));
  }

  @Test(timeout = 100)
  public void testLoadedOnce()
  {
    while (!manager.isLoadedOnce()) {
      ;
    }
  }

  @Test
  public void testStopEmpty()
  {
    Assert.assertTrue(manager.knownNamespaces().isEmpty());
    manager.addOrUpdateNamespace(testNamespace, testMap);
    Assert.assertFalse(manager.knownNamespaces().isEmpty());
    manager.stop();
    Assert.assertTrue(manager.knownNamespaces().isEmpty());
  }

  @Test
  public void testLoadedNamespaces()
  {
    Assert.assertTrue(manager.knownNamespaces().isEmpty());
    manager.addOrUpdateNamespace(testNamespace, testMap);
    Assert.assertFalse(manager.knownNamespaces().isEmpty());
    manager.poll();
    Assert.assertFalse(manager.knownNamespaces().isEmpty());
  }

  @Test(timeout = 100)
  public void testRunnable() throws InterruptedException
  {
    final CountDownLatch latch = new CountDownLatch(1);
    manager.registerPostPollRunnable(
        new Runnable()
        {
          @Override
          public void run()
          {
            latch.countDown();
          }
        }
    );
    manager.poll();
    latch.await();
  }

  @Test
  public void testStartStop()
  {
    manager.stop();
    manager.start();
    manager.stop();
    manager.start();
    manager.stop();
    manager.start();
    manager.stop();
    manager.start();
  }

  @Test
  public void testStartStopWithData()
  {
    manager.stop();
    manager.start();
    manager.stop();
    manager.start();
    manager.stop();
    manager.start();
    manager.stop();
    manager.start();
  }

  @Test(timeout = 100)
  public void testStartFlood() throws ExecutionException, InterruptedException
  {
    manager.stop();
    final int NUM_THREADS = 10;
    final AtomicLong postCalls = new AtomicLong(0L);
    manager.registerPostPollRunnable(
        new Runnable()
        {
          @Override
          public void run()
          {
            postCalls.incrementAndGet();
          }
        }
    );
    final ListeningExecutorService service = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(NUM_THREADS));
    final CountDownLatch latch = new CountDownLatch(1);
    final List<ListenableFuture<?>> futureList = new ArrayList<>(NUM_THREADS);
    for (int i = 0; i < NUM_THREADS; ++i) {
      futureList.add(
          service.submit(
              new Runnable()
              {
                @Override
                public void run()
                {
                  try {
                    latch.await();
                  }
                  catch (InterruptedException e) {
                    throw Throwables.propagate(e);
                  }
                  manager.start();
                }
              }
          )
      );
    }
    latch.countDown();
    Futures.allAsList(futureList).get();
    Assert.assertTrue(postCalls.get() < 3);
  }

  @Test(timeout = 100)
  public void testStopFlood() throws ExecutionException, InterruptedException
  {
    final int NUM_THREADS = 10;
    final ListeningExecutorService service = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(NUM_THREADS));
    final CountDownLatch latch = new CountDownLatch(1);
    final List<ListenableFuture<?>> futureList = new ArrayList<>(NUM_THREADS);
    for (int i = 0; i < NUM_THREADS; ++i) {
      futureList.add(
          service.submit(
              new Runnable()
              {
                @Override
                public void run()
                {
                  try {
                    latch.await();
                  }
                  catch (InterruptedException e) {
                    throw Throwables.propagate(e);
                  }
                  manager.stop();
                }
              }
          )
      );
    }
    latch.countDown();
    Futures.allAsList(futureList).get();
  }
}
