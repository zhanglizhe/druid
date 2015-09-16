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

package io.druid.collections;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.metamx.common.logger.Logger;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Simple load balancing pool that always returns the least used resource
 *
 * @param <T>
 */
public class LoadBalancingPool<T> implements Supplier<ResourceHolder<T>>
{
  private static final Logger log = new Logger(LoadBalancingPool.class);

  private final Supplier<T> generator;

  private final Queue<CountingHolder> objects;

  private final int maxCapacity;
  private final AtomicInteger size = new AtomicInteger(0);

  public LoadBalancingPool(int maxCapacity, Supplier<T> generator)
  {
    Preconditions.checkArgument(maxCapacity > 0, "capacity must be greater than 0");
    Preconditions.checkNotNull(generator);

    this.generator = generator;
    this.maxCapacity = maxCapacity;
    this.objects = new PriorityBlockingQueue<>(maxCapacity);
  }

  public ResourceHolder<T> get()
  {
    CountingHolder obj;
    if(size.getAndIncrement() > maxCapacity) {
      // at capacity just take things from the queue
      size.decrementAndGet();

      // things never stay out of the queue for long, so we'll get it eventually
      do { obj = objects.remove(); } while(obj == null);
      obj.count.incrementAndGet();
      objects.offer(obj);
      return obj;
    }
    else {
      // otherwise make new things
      obj = new CountingHolder(generator.get());
      obj.count.incrementAndGet();
      objects.offer(obj);
      return obj;
    }
  }

  private class CountingHolder implements ResourceHolder<T>, Comparable<CountingHolder>
  {
    private AtomicInteger count = new AtomicInteger(0);
    private final T object;

    public CountingHolder(final T object)
    {
      this.object = object;
    }

    @Override
    public T get()
    {
      return object;
    }

    /**
     * Not idempotent, should only be called once when done using the resource
     *
     * @throws IOException
     */
    @Override
    public void close() throws IOException
    {
      objects.remove(this);
      count.decrementAndGet();
      objects.offer(this);
    }

    @Override
    public int compareTo(CountingHolder o)
    {
      return Integer.compare(count.get(), o.count.get());
    }


    @Override
    protected void finalize() throws Throwable
    {
      try {
        final int shouldBeZero = count.get();
        if (shouldBeZero != 0) {
          log.warn("Expected 0 resource count, got [%d]! Object was[%s].", shouldBeZero, object);
        }
      }
      finally {
        super.finalize();
      }
    }
  }
}
