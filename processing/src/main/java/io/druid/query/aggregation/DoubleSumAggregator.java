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

import com.google.common.collect.Ordering;
import com.google.common.primitives.Doubles;
import io.druid.segment.FloatColumnSelector;

import java.nio.ByteBuffer;
import java.util.Comparator;

/**
 */
public class DoubleSumAggregator implements Aggregator, BlockAggregator
{
  static final Comparator COMPARATOR = new Ordering()
  {
    @Override
    public int compare(Object o, Object o1)
    {
      return Doubles.compare(((Number) o).doubleValue(), ((Number) o1).doubleValue());
    }
  }.nullsFirst();

  static double combineValues(Object lhs, Object rhs)
  {
    return ((Number) lhs).doubleValue() + ((Number) rhs).doubleValue();
  }

  private final FloatColumnSelector selector;
  private final String name;

  private double sum;

  public DoubleSumAggregator(String name, FloatColumnSelector selector)
  {
    this.name = name;
    this.selector = selector;

    this.sum = 0;
  }

  @Override
  public void aggregate()
  {
    sum += selector.get();
  }

  @Override
  public void aggregateBlock()
  {
    final float[] block = selector.getBlock();

    final int n = block.length;
    final int ub = (n / 8) * 8 - 1;
    final int extra = n - (n % 8);

    double acc = 0;
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
  }

  @Override
  public void reset()
  {
    sum = 0;
  }

  @Override
  public Object get()
  {
    return sum;
  }

  @Override
  public float getFloat()
  {
    return (float) sum;
  }

  @Override
  public String getName()
  {
    return this.name;
  }

  @Override
  public Aggregator clone()
  {
    return new DoubleSumAggregator(name, selector);
  }

  @Override
  public void close()
  {
    // no resources to cleanup
  }
}
