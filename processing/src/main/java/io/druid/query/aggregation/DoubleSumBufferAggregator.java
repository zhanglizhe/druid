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

package io.druid.query.aggregation;

import io.druid.segment.FloatColumnSelector;

import java.nio.ByteBuffer;

/**
 */
public class DoubleSumBufferAggregator implements BufferAggregator, BlockBufferAggregator
{
  private final FloatColumnSelector selector;

  public DoubleSumBufferAggregator(
      FloatColumnSelector selector
  )
  {
    this.selector = selector;
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    buf.putDouble(position, 0.0d);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    buf.putDouble(position, buf.getDouble(position) + (double) selector.get());
  }

  @Override
  public void aggregateBlock(ByteBuffer buf, int position)
  {
    final float[] block = selector.getBlock();

    final int n = block.length;
    final int ub = (n / 8) * 8 - 1;
    final int extra = n - (n % 8);

    double acc = 0;
    double sum = 0;
    for(int i = 0; i < ub; i += 8)
    {
      acc += (double) block[i]
             + (double) block[i + 1]
             + (double) block[i + 2]
             + (double) block[i + 3];

      sum += (double) block[i + 4]
             + (double) block[i + 5]
             + (double) block[i + 6]
             + (double) block[i + 7];
    }
    sum += acc;
    for(int i = extra; i < n; ++i)
    {
      sum += block[i];
    }
    buf.putDouble(position, buf.getDouble(position) + sum);
  }

  @Override
  public void parallelAggregate(ByteBuffer buf, int[] positions)
  {
    final int n = positions.length;
    final int ub = (n / 4) * 4 - 1;
    final int extra = n - (n % 4);

    final double val = (double) selector.get();
    for(int i = 0; i < ub; i += 4)
    {
      final double v1 = buf.getDouble(positions[i]);
      final double v2 = buf.getDouble(positions[i + 1]);
      final double v3 = buf.getDouble(positions[i + 2]);
      final double v4 = buf.getDouble(positions[i + 3]);
      final double sum1 = v1 + val;
      final double sum2 = v2 + val;
      final double sum3 = v3 + val;
      final double sum4 = v4 + val;
      buf.putDouble(positions[i], sum1);
      buf.putDouble(positions[i], sum2);
      buf.putDouble(positions[i], sum3);
      buf.putDouble(positions[i], sum4);
    }
    for(int i = extra; i < n; ++i)
    {
      buf.putDouble(positions[i], buf.getDouble(positions[i]) + val);
    }
  }

  @Override
  public Object get(ByteBuffer buf, int position)
  {
    return buf.getDouble(position);
  }

  @Override
  public float getFloat(ByteBuffer buf, int position)
  {
    return (float) buf.getDouble(position);
  }


  @Override
  public long getLong(ByteBuffer buf, int position)
  {
    return (long) buf.getDouble(position);
  }

  @Override
  public void close()
  {
    // no resources to cleanup
  }
}
