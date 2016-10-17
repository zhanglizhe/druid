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

package io.druid.common.guava;

import com.google.common.base.Preconditions;
import com.metamx.common.guava.Accumulator;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Yielder;
import com.metamx.common.guava.YieldingAccumulator;

import java.io.Closeable;
import java.io.IOException;

public final class MoreSequences
{
  /**
   * Fixed version of {@link com.metamx.common.guava.Sequences#withBaggage(Sequence, Closeable)}, closes the baggage
   * after the yielder of the wrapped sequence.
   */
  public static <T> Sequence<T> withBaggage(Sequence<T> sequence, final Closeable baggage)
  {
    Preconditions.checkNotNull(baggage, "baggage");
    return wrap(sequence, new SequenceWrapper()
    {
      @Override
      public void open()
      {
        // do nothing
      }

      @Override
      public void close()
      {
        try {
          baggage.close();
        }
        catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    });
  }

  public static <T> Sequence<T> wrap(final Sequence<T> sequence, final SequenceWrapper wrapper)
  {
    Preconditions.checkNotNull(wrapper, "wrapper");
    return new Sequence<T>()
    {
      @Override
      public <OutType> OutType accumulate(OutType outType, Accumulator<OutType, T> accumulator)
      {
        wrapper.open();
        //noinspection unused
        try (SequenceWrapper toClose = wrapper) {
          return sequence.accumulate(outType, accumulator);
        }
      }

      @Override
      public <OutType> Yielder<OutType> toYielder(OutType initValue, YieldingAccumulator<OutType, T> accumulator)
      {
        wrapper.open();
        try {
          return makeYielder(sequence.toYielder(initValue, accumulator));
        }
        catch (Throwable throwable) {
          try {
            wrapper.close();
          }
          catch (Throwable t) {
            throwable.addSuppressed(t);
          }
          throw throwable;
        }
      }

      private <OutType> Yielder<OutType> makeYielder(final Yielder<OutType> yielder)
      {
        return new Yielder<OutType>()
        {
          @Override
          public OutType get()
          {
            return yielder.get();
          }

          @Override
          public Yielder<OutType> next(OutType initValue)
          {
            return makeYielder(yielder.next(initValue));
          }

          @Override
          public boolean isDone()
          {
            return yielder.isDone();
          }

          @Override
          public void close() throws IOException
          {
            //noinspection unused
            try (SequenceWrapper toClose = wrapper) {
              yielder.close();
            }
          }
        };
      }
    };

  }
}
