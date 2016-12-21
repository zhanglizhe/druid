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

package io.druid.collections;

import com.google.common.base.Supplier;
import com.metamx.common.ISE;
import com.metamx.common.logger.Logger;
import sun.misc.Cleaner;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 */
public class StupidPool<T>
{
  private static final Logger log = new Logger(StupidPool.class);

  private final Supplier<T> generator;

  private final Queue<ObjectResourceHolder> objects = new ConcurrentLinkedQueue<>();
  /**
   * {@link ConcurrentLinkedQueue}'s size() is O(n) queue traversal apparently for the sake of being 100%
   * wait-free, that is not required by {@code StupidPool}. In {@code poolSize} we account the queue size
   * ourselves, to avoid traversal of {@link #objects} in {@link #tryReturnToPool}.
   */
  private final AtomicLong poolSize = new AtomicLong(0);

  //note that this is just the max entries in the cache, pool can still create as many buffers as needed.
  private final int objectsCacheMaxCount;

  public StupidPool(
      Supplier<T> generator
  )
  {
    this.generator = generator;
    this.objectsCacheMaxCount = Integer.MAX_VALUE;
  }

  public StupidPool(
      Supplier<T> generator,
      int objectsCacheMaxCount
  )
  {
    this.generator = generator;
    this.objectsCacheMaxCount = objectsCacheMaxCount;
  }

  public ResourceHolder<T> take()
  {
    ObjectResourceHolder resourceHolder = objects.poll();
    if (resourceHolder == null) {
      T object = generator.get();
      ObjectId objectId = new ObjectId();
      ObjectLeakNotifier notifier = new ObjectLeakNotifier();
      return new ObjectResourceHolder(object, objectId, Cleaner.create(objectId, notifier), notifier);
    } else {
      poolSize.decrementAndGet();
      return resourceHolder;
    }
  }

  /** For tests */
  long poolSize() {
    return poolSize.get();
  }

  private void tryReturnToPool(T object, ObjectId objectId, Cleaner cleaner, ObjectLeakNotifier notifier)
  {
    long currentPoolSize;
    do {
      currentPoolSize = poolSize.get();
      if (currentPoolSize >= objectsCacheMaxCount) {
        notifier.disable();
        // important to use the objectId (we use it in logging message) after notifier.disable(), otherwise VM may
        // already decide that the objectId is unreachable and run Cleaner, that would be reported as a false-positive
        // "leak". Ideally reachabilityFence(objectId) should be used here
        log.debug("cache num entries is exceeding max limit [%s], objectId [%s]", objectsCacheMaxCount, objectId);
        return;
      }
    } while (!poolSize.compareAndSet(currentPoolSize, currentPoolSize + 1));
    if (!objects.offer(new ObjectResourceHolder(object, objectId, cleaner, notifier))) {
      poolSize.decrementAndGet();
      notifier.disable();
      log.warn(new ISE("Queue offer failed"), "Could not offer object [%s] back into the queue, objectId [%s]", object, objectId);
    }
  }

  private class ObjectResourceHolder implements ResourceHolder<T>
  {
    private final AtomicReference<T> objectRef;
    private volatile ObjectId objectId;
    private final Cleaner cleaner;
    private final ObjectLeakNotifier notifier;

    ObjectResourceHolder(
        final T object,
        final ObjectId objectId,
        final Cleaner cleaner,
        final ObjectLeakNotifier notifier
    )
    {
      this.objectRef = new AtomicReference<>(object);
      this.objectId = objectId;
      this.cleaner = cleaner;
      this.notifier = notifier;
    }

    // WARNING: it is entirely possible for a caller to hold onto the object and call ObjectResourceHolder.close,
    // Then still use that object even though it will be offered to someone else in StupidPool.take
    @Override
    public T get()
    {
      final T object = objectRef.get();
      if (object == null) {
        throw new ISE("Already Closed!");
      }

      return object;
    }

    @Override
    public void close()
    {
      final T object = objectRef.get();
      if (object != null && objectRef.compareAndSet(object, null)) {
        ObjectId objectId = this.objectId;
        this.objectId = null;
        tryReturnToPool(object, objectId, cleaner, notifier);
      }
    }
  }

  private class ObjectLeakNotifier implements Runnable
  {
    final AtomicBoolean leak = new AtomicBoolean(true);

    @Override
    public void run()
    {
      try {
        if (leak.getAndSet(false)) {
          log.warn("Not closed! Object leaked from pool[%s]. Allowing gc to prevent leak.", StupidPool.this);
        }
      }
      // Exceptions must not be thrown in Cleaner.clean(), which calls this ObjectReclaimer.run() method
      catch (Exception e) {
        try {
          log.error(e, "Exception in ObjectLeakNotifier.run()");
        }
        catch (Exception ignore) {
          // ignore
        }
      }
    }

    public void disable()
    {
      leak.set(false);
    }
  }

  private static class ObjectId
  {
  }
}
