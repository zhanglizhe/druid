/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.client.cache;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.Weigher;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import com.metamx.emitter.service.ServiceEmitter;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 */
public class MapCache implements Cache
{
  public static Cache create(long sizeInBytes)
  {
    return new MapCache(sizeInBytes);
  }

  private final com.google.common.cache.Cache<ByteBuffer, byte[]> baseCache;
  private final ConcurrentMap<String, byte[]> namespaceId;
  private final AtomicInteger ids;

  MapCache(
      long sizeInBytes
  )
  {
    namespaceId = Maps.newConcurrentMap();
    ids = new AtomicInteger();
    baseCache = CacheBuilder.newBuilder()
        .maximumWeight(sizeInBytes)
            // TODO: set concurrency equal to number of cores or number of processing threads.
        .concurrencyLevel(10)
        .weigher(
            new Weigher<ByteBuffer, byte[]>()
            {
              @Override
              public int weigh(ByteBuffer key, byte[] value)
              {
                return key.limit() + value.length;
              }
            }
        )
        .build();
    baseCache.stats();
  }

  @Override
  public CacheStats getStats()
  {
    com.google.common.cache.CacheStats stats = baseCache.stats();
    return new CacheStats(
        stats.hitCount(),
        stats.missCount(),
        baseCache.size(),
        0,
        stats.evictionCount(),
        0,
        0
    );
  }

  @Override
  public byte[] get(NamedKey key)
  {
    return baseCache.getIfPresent(computeKey(getNamespaceId(key.namespace), key.key));
  }

  @Override
  public void put(NamedKey key, byte[] value)
  {
      baseCache.put(computeKey(getNamespaceId(key.namespace), key.key), value);
  }

  @Override
  public Map<NamedKey, byte[]> getBulk(Iterable<NamedKey> keys)
  {
    Map<NamedKey, byte[]> retVal = Maps.newHashMap();
    for (NamedKey key : keys) {
      final byte[] value = get(key);
      if (value != null) {
        retVal.put(key, value);
      }
    }
    return retVal;
  }

  @Override
  public void close(String namespace)
  {
    byte[] idBytes;
    idBytes = getNamespaceId(namespace);
    if (idBytes == null) {
      return;
    }

    namespaceId.remove(namespace);
    Set<ByteBuffer> keys = baseCache.asMap().keySet();
    List<ByteBuffer> toRemove = Lists.newLinkedList();
    for (ByteBuffer key : keys) {
      if (key.get(0) == idBytes[0]
          && key.get(1) == idBytes[1]
          && key.get(2) == idBytes[2]
          && key.get(3) == idBytes[3]) {
        toRemove.add(key);
      }
    }

    baseCache.invalidateAll(toRemove);
  }

  private byte[] getNamespaceId(final String identifier)
  {
    byte[] idBytes = namespaceId.get(identifier);
    if (idBytes != null) {
      return idBytes;
    }

    idBytes = Ints.toByteArray(ids.getAndIncrement());
    byte[] oldBytes = namespaceId.putIfAbsent(identifier, idBytes);
    return oldBytes != null ? oldBytes : idBytes;
  }

  private ByteBuffer computeKey(byte[] idBytes, byte[] key)
  {
    final ByteBuffer retVal = ByteBuffer.allocate(key.length + 4).put(idBytes).put(key);
    retVal.rewind();
    return retVal;
  }

  public boolean isLocal()
  {
    return true;
  }

  @Override
  public void doMonitor(ServiceEmitter emitter)
  {
    // No special monitoring
  }
}
