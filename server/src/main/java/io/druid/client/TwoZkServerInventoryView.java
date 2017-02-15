/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;
import com.metamx.common.Pair;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;
import io.druid.concurrent.LifecycleLock;
import io.druid.curator.CuratorModule;
import io.druid.curator.inventory.InventoryManagerConfig;
import io.druid.guice.ManageLifecycle;
import io.druid.server.coordination.DruidServerMetadata;
import io.druid.server.initialization.ZkPathsConfig;
import io.druid.timeline.DataSegment;
import org.apache.curator.framework.CuratorFramework;

import java.io.IOException;
import java.util.concurrent.Executor;

/**
 */
@ManageLifecycle
public class TwoZkServerInventoryView implements FilteredServerInventoryView, AbstractServerInventoryView
{
  private final LifecycleLock lifecycleLock = new LifecycleLock();
  private final BatchServerInventoryView cloud1;
  private final BatchServerInventoryView cloud2;

  @Inject
  public TwoZkServerInventoryView(
      final ZkPathsConfig zkPaths,
      final TwoZkConfig twoZkConfig,
      final ObjectMapper jsonMapper,
      final Predicate<Pair<DruidServerMetadata, DataSegment>> defaultFilter,
      final Lifecycle lifecycle
  )
  {
    CuratorFramework curator1 = CuratorModule.makeCuratorAndRegisterLifecycle(twoZkConfig.getService1(), lifecycle);
    cloud1 = new BatchServerInventoryView(zkPaths, curator1, jsonMapper, defaultFilter);

    CuratorFramework curator2 = CuratorModule.makeCuratorAndRegisterLifecycle(twoZkConfig.getService2(), lifecycle);
    cloud2 = new BatchServerInventoryView(zkPaths, curator2, jsonMapper, defaultFilter);
  }

  public void registerSegmentCallback(
      final Executor exec,
      final SegmentCallback callback,
      final Predicate<Pair<DruidServerMetadata, DataSegment>> filter
  )
  {
    cloud1.registerSegmentCallback(exec, callback, filter);
    cloud2.registerSegmentCallback(exec, callback, filter);
  }

  @Override
  @LifecycleStart
  public void start() throws Exception
  {
    if (!lifecycleLock.canStart())
      return;
    try {
      cloud1.start();
      cloud2.start();
    }
    finally {
      lifecycleLock.started();
    }
  }

  @Override
  @LifecycleStop
  public void stop() throws IOException
  {
    if (!lifecycleLock.canStop())
      return;
    cloud1.stop();
    cloud2.stop();
  }

  @Override
  public boolean isStarted()
  {
    return lifecycleLock.isStarted();
  }

  @Override
  public DruidServer getInventoryValue(String containerKey)
  {
    DruidServer druidServer = cloud1.getInventoryValue(containerKey);
    if (druidServer != null) {
      return druidServer;
    }
    return cloud2.getInventoryValue(containerKey);
  }

  @Override
  public Iterable<DruidServer> getInventory()
  {
    return Iterables.concat(cloud1.getInventory(), cloud2.getInventory());
  }

  @Override
  public void registerServerCallback(Executor exec, ServerCallback callback)
  {
    cloud1.registerServerCallback(exec, callback);
    cloud2.registerServerCallback(exec, callback);
  }

  @Override
  public void registerSegmentCallback(Executor exec, SegmentCallback callback)
  {
    cloud1.registerSegmentCallback(exec, callback);
    cloud2.registerSegmentCallback(exec, callback);
  }

  @Override
  public InventoryManagerConfig getInventoryManagerConfig()
  {
    // InventoryManagerConfig should be equal in both zk clusters, so it doesn't matter if we delegate to cloud1 or
    // cloud2
    return cloud1.getInventoryManagerConfig();
  }
}
