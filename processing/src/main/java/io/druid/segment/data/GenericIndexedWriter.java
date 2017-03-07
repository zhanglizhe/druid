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

package io.druid.segment.data;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import io.druid.io.Channels;
import io.druid.io.OutputBytes;
import io.druid.segment.serde.Serializer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/**
 * Streams arrays of objects out in the binary format described by GenericIndexed
 */
public class GenericIndexedWriter<T> implements Serializer
{
  private final ObjectStrategy<T> strategy;

  private boolean objectsSorted = true;
  private T prevObject = null;

  private OutputBytes headerOut = null;
  private OutputBytes valuesOut = null;
  private int numWritten = 0;

  public GenericIndexedWriter(
      ObjectStrategy<T> strategy
  )
  {
    this.strategy = strategy;
  }

  public void open() throws IOException
  {
    headerOut = new OutputBytes();
    valuesOut = new OutputBytes();
  }

  public void write(T objectToWrite) throws IOException
  {
    if (objectsSorted && prevObject != null && strategy.compare(prevObject, objectToWrite) >= 0) {
      objectsSorted = false;
    }

    byte[] bytesToWrite = strategy.toBytes(objectToWrite);

    ++numWritten;
    valuesOut.writeInt(bytesToWrite.length);
    valuesOut.write(bytesToWrite);

    headerOut.writeInt(Ints.checkedCast(valuesOut.size()));

    prevObject = objectToWrite;
  }

  @Override
  public long getSerializedSize() throws IOException
  {
    return metaSize() + headerOut.size() + valuesOut.size();
  }

  @Override
  public void writeTo(WritableByteChannel channel) throws IOException
  {
    final long numBytesWritten = headerOut.size() + valuesOut.size();

    Preconditions.checkState(
        headerOut.size() == (numWritten * 4),
        "numWritten[%s] number of rows should have [%s] bytes written to headerOut, had[%s]",
        numWritten,
        numWritten * 4,
        headerOut.size()
    );
    Preconditions.checkState(
        numBytesWritten < Integer.MAX_VALUE, "Wrote[%s] bytes, which is too many.", numBytesWritten
    );

    int metaSize = metaSize(); // numElements
    ByteBuffer meta = ByteBuffer.allocate(metaSize);
    meta.put((byte) 0x1);
    meta.put((byte) (objectsSorted ? 0x1 : 0x0));
    meta.putInt(Ints.checkedCast(numBytesWritten + 4));
    meta.putInt(numWritten);
    meta.flip();

    Channels.writeFully(channel, meta);
    headerOut.writeTo(channel);
    valuesOut.writeTo(channel);
  }

  private int metaSize()
  {
    return 2 + // version and sorted flag
           Ints.BYTES + // numBytesWritten
           Ints.BYTES; // numWritten
  }
}
