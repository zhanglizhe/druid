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

package io.druid.segment;

import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.collections.bitmap.ImmutableBitmap;
import com.metamx.collections.bitmap.MutableBitmap;
import com.metamx.collections.bitmap.WrappedImmutableRoaringBitmap;
import com.metamx.collections.bitmap.WrappedRoaringBitmap;
import io.druid.segment.data.Offset;
import io.druid.segment.data.RoaringBitmapSerdeFactory;
import it.uniroma3.mat.extendedset.intset.EmptyIntIterator;
import org.roaringbitmap.IntIterator;

/**
 */
public class BitmapOffset extends Offset
{
  private static final int INVALID_VALUE = -1;
  private static final BitmapFactory ROARING_BITMAP_FACTORY = new RoaringBitmapSerdeFactory(false).getBitmapFactory();

  private static String factorizeFullness(double fullness)
  {
    if (fullness < 0.01) {
      return "[0, 0.01)";
    } else if (fullness < 0.08) {
      return "[0.01, 0.08)";
    } else if (fullness < 0.3) {
      return "[0.08, 0.3)";
    } else if (fullness <= 0.7) {
      return "[0.3, 0.7]";
    } else if (fullness <= 0.92) {
      return "(0.7, 0.92]";
    } else if (fullness <= 0.99) {
      return "(0.92, 0.99]";
    } else {
      return "(0.99, 1]";
    }
  }

  final IntIterator itr;
  final String fullness;

  int val;

  public static IntIterator getReverseBitmapOffsetIterator(ImmutableBitmap bitmapIndex)
  {
    ImmutableBitmap roaringBitmap = bitmapIndex;
    if (!(bitmapIndex instanceof WrappedImmutableRoaringBitmap)) {
      final MutableBitmap bitmap = ROARING_BITMAP_FACTORY.makeEmptyMutableBitmap();
      final IntIterator iterator = bitmapIndex.iterator();
      while (iterator.hasNext()) {
        bitmap.add(iterator.next());
      }
      roaringBitmap = ROARING_BITMAP_FACTORY.makeImmutableBitmap(bitmap);
    }
    return ((WrappedImmutableRoaringBitmap) roaringBitmap).getBitmap().getReverseIntIterator();
  }

  public static BitmapOffset of(ImmutableBitmap bitmapIndex, boolean descending, double fullness)
  {
    if (bitmapIndex instanceof WrappedImmutableRoaringBitmap || bitmapIndex instanceof WrappedRoaringBitmap ||
        descending) {
      return new RoaringBitmapOffset(bitmapIndex, descending, fullness);
    } else {
      return new BitmapOffset(bitmapIndex, descending, fullness);
    }
  }

  private BitmapOffset(ImmutableBitmap bitmapIndex, boolean descending, double fullness)
  {
    this.itr = newIterator(bitmapIndex, descending);
    this.fullness = factorizeFullness(fullness);
    increment();
  }

  private IntIterator newIterator(ImmutableBitmap bitmapIndex, boolean descending)
  {
    if (!descending) {
      return bitmapIndex.iterator();
    } else {
      return getReverseBitmapOffsetIterator(bitmapIndex);
    }
  }

  private BitmapOffset(String fullness, IntIterator itr, int val)
  {
    this.fullness = fullness;
    this.itr = itr;
    this.val = val;
  }

  @Override
  public void increment()
  {
    if (itr.hasNext()) {
      val = itr.next();
    } else {
      val = INVALID_VALUE;
    }
  }

  @Override
  public boolean withinBounds()
  {
    return val > INVALID_VALUE;
  }

  @Override
  public Offset clone()
  {
    return new BitmapOffset(fullness, itr.clone(), val);
  }

  @Override
  public int getOffset()
  {
    return val;
  }

  @Override
  public String getOffsetType()
  {
    return getClass().getName() + "["
           + "itr=" + itr.getClass().getName() +
           ", fullness=" + fullness +
           "]";
  }

  public static class RoaringBitmapOffset extends BitmapOffset
  {

    public RoaringBitmapOffset(ImmutableBitmap bitmapIndex, boolean descending, double fullness)
    {
      super(bitmapIndex, descending, fullness);
    }

    RoaringBitmapOffset(String fullness, IntIterator itr, int val)
    {
      super(fullness, itr, val);
    }

    @Override
    public Offset clone()
    {
      return new RoaringBitmapOffset(fullness, itr.hasNext() ? itr.clone() : EmptyIntIterator.instance(), val);
    }
  }
}
