/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
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
    final int extra = n - n % 4;
    final int ub = (n / 4) * 4 - 1;

    final double val = (double) selector.get();
    for(int i = 0; i < ub; i += 4)
    {
      final double v1 = buf.getDouble(positions[i]    );
      final double v2 = buf.getDouble(positions[i + 1]);
      final double v3 = buf.getDouble(positions[i + 2]);
      final double v4 = buf.getDouble(positions[i + 3]);
      // this gets optimized into a single AVX instruction
      final double sum1 = v1 + val;
      final double sum2 = v2 + val;
      final double sum3 = v3 + val;
      final double sum4 = v4 + val;
      buf.putDouble(positions[i]    , sum1);
      buf.putDouble(positions[i + 1], sum2);
      buf.putDouble(positions[i + 2], sum3);
      buf.putDouble(positions[i + 3], sum4);
    }
    for(int i = extra; i < n; ++i) {
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
