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

package io.druid.benchmark;


import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.infra.ThreadParams;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
public class ConcurrentMapBenchmark
{
  static final int THREAD_SLICE = 1_000;
  private ConcurrentMap<Long, Long> map;
  private ConcurrentMap<Long, Long> mapDouble;
  private ConcurrentMap<Long, Long> mapHalf;

  @Setup
  public void setup(BenchmarkParams params)
  {
    final int threads = params.getThreads();
    final int capacity = 16 * THREAD_SLICE * threads;
    map = new ConcurrentHashMap<>(capacity, 0.75f, threads);
    mapDouble = new ConcurrentHashMap<>(capacity, 0.75f, threads * 2);
    mapHalf = new ConcurrentHashMap<>(capacity, 0.75f, threads / 2);
  }

  @State(Scope.Thread)
  public static class Nums
  {
    private List<Long> nums;

    @Setup
    public void setup(ThreadParams threadParams)
    {
      nums = new ArrayList<Long>(THREAD_SLICE);
      for (long i = 0; i < THREAD_SLICE; ++i) {
        nums.add(((long) THREAD_SLICE) * threadParams.getThreadIndex() + i);
      }
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void hammerMapGetPutThreadCount(Nums nums, Blackhole blackhole)
  {
    hammerMapGetPut(nums, blackhole, map);
  }

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void hammerMapGetPutDouble(Nums nums, Blackhole blackhole)
  {
    hammerMapGetPut(nums, blackhole, mapDouble);
  }


  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void hammerMapGetPutHalf(Nums nums, Blackhole blackhole)
  {
    hammerMapGetPut(nums, blackhole, mapHalf);
  }

  public void hammerMapGetPut(Nums nums, Blackhole blackhole, Map<Long, Long> map)
  {
    for (long i : nums.nums) {
      blackhole.consume(map.get(i));
      blackhole.consume(map.put(i, i));
    }
  }
}
