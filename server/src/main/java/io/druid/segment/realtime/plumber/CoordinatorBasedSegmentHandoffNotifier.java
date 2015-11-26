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

package io.druid.segment.realtime.plumber;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.metamx.common.Pair;
import com.metamx.common.logger.Logger;
import io.druid.client.SegmentLoadInfo;
import io.druid.client.coordinator.CoordinatorClient;
import io.druid.concurrent.Execs;
import io.druid.query.SegmentDescriptor;
import io.druid.server.coordination.DruidServerMetadata;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class CoordinatorBasedSegmentHandoffNotifier implements SegmentHandoffNotifier
{
  private static final Logger log = new Logger(CoordinatorBasedSegmentHandoffNotifier.class);

  private final Map<SegmentDescriptor, Pair<Executor, Runnable>> handOffCallbacks = Maps.newConcurrentMap();
  private final CoordinatorClient coordinatorClient;
  private volatile ScheduledExecutorService scheduledExecutor;
  private final long pollDurationMillis;
  private final String dataSource;

  public CoordinatorBasedSegmentHandoffNotifier(
      String dataSource,
      CoordinatorClient coordinatorClient,
      CoordinatorBasedSegmentHandoffNotifierConfig config
  )
  {
    this.dataSource = dataSource;
    this.coordinatorClient = coordinatorClient;
    this.pollDurationMillis = config.getPollDuration().getMillis();
  }

  @Override
  public void registerSegmentHandoffCallback(
      SegmentDescriptor descriptor, Executor exec, Runnable handOffRunnable
  )
  {
    log.info("Adding SegmentHandoffCallback for dataSource[%s] Segment[%s]", dataSource, descriptor);
    handOffCallbacks.put(descriptor, new Pair<>(exec, handOffRunnable));
  }

  @Override
  public void start()
  {
    scheduledExecutor = Execs.scheduledSingleThreaded("coordinator_handoff_scheduled_%d");
    scheduledExecutor.scheduleAtFixedRate(
        new Runnable()
        {
          @Override
          public void run()
          {
            Iterator<Map.Entry<SegmentDescriptor, Pair<Executor, Runnable>>> itr = handOffCallbacks.entrySet()
                                                                                                   .iterator();
            while (itr.hasNext()) {
              Map.Entry<SegmentDescriptor, Pair<Executor, Runnable>> entry = itr.next();
              SegmentDescriptor descriptor = entry.getKey();
              try {
                List<SegmentLoadInfo> serverView = coordinatorClient.fetchServerView(
                    dataSource,
                    descriptor.getInterval(),
                    true
                );

                if (isHandOffComplete(serverView, entry.getKey())) {
                  log.info("Segment Handoff complete for dataSource[%s] Segment[%s]", dataSource, descriptor);
                  entry.getValue().lhs.execute(entry.getValue().rhs);
                  itr.remove();
                } else {
                  log.info(
                      "Segment Handoff not complete for dataSource[%s] Segment[%s]. Will try again after [%d]secs",
                      dataSource,
                      descriptor,
                      pollDurationMillis
                  );

                }
              }
              catch (Exception e) {
                log.error(
                    e,
                    "Exception while checking handoff for dataSource[%s] Segment[%s], Will try again after [%d]secs",
                    dataSource,
                    descriptor,
                    pollDurationMillis
                );
              }
            }
          }
        }, 0L, pollDurationMillis, TimeUnit.MILLISECONDS
    );
  }


  static boolean isHandOffComplete(List<SegmentLoadInfo> serverView, SegmentDescriptor descriptor)
  {
    for (SegmentLoadInfo segmentLoadInfo : serverView) {
      if (segmentLoadInfo.getSegment().getInterval().contains(descriptor.getInterval())
          && segmentLoadInfo.getSegment().getShardSpec().getPartitionNum()
             == descriptor.getPartitionNumber()
          && segmentLoadInfo.getSegment().getVersion().compareTo(descriptor.getVersion()) >= 0
          && Iterables.any(
          segmentLoadInfo.getServers(), new Predicate<DruidServerMetadata>()
          {
            @Override
            public boolean apply(DruidServerMetadata input)
            {
              return input.isAssignable();
            }
          }
      )) {
        return true;
      }
    }
    return false;
  }

  @Override
  public void stop()
  {
    scheduledExecutor.shutdown();
  }

  // Used in tests
  Map<SegmentDescriptor, Pair<Executor, Runnable>> getHandOffCallbacks()
  {
    return handOffCallbacks;
  }
}
