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

package io.druid.concurrent;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

public class LifecycleLock
{
  private enum State { NOT_STARTED, STARTED, STOPPED }

  private final AtomicReference<State> state = new AtomicReference<>(State.NOT_STARTED);
  private final CountDownLatch canStop = new CountDownLatch(1);

  public boolean canStart()
  {
    return state.compareAndSet(State.NOT_STARTED, State.STARTED);
  }

  public void started()
  {
    canStop.countDown();
  }

  public boolean isStarted()
  {
    return state.get() == State.STARTED;
  }

  public boolean canStop()
  {
    try {
      canStop.await();
    }
    catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    return state.compareAndSet(State.STARTED, State.STOPPED);
  }
}
