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

package io.druid.query.topn;

import io.druid.query.aggregation.SimpleDoubleBufferAggregator;
import io.druid.segment.BitmapOffset;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.historical.HistoricalCursor;
import io.druid.segment.historical.HistoricalDimensionSelector;
import io.druid.segment.historical.HistoricalFloatColumnSelector;
import io.druid.segment.historical.SingleValueHistoricalDimensionSelector;

import java.nio.ByteBuffer;

import static io.druid.query.topn.TopNAlgorithm.INIT_POSITION_VALUE;

public final class DoublePooledTopNScannerPrototype implements DoublePooledTopNScanner
{
  @Override
  public long scanAndAggregateHistorical(
      HistoricalDimensionSelector dimensionSelector,
      HistoricalFloatColumnSelector metricSelector,
      SimpleDoubleBufferAggregator aggregator,
      HistoricalCursor cursor,
      int[] positions,
      ByteBuffer resultsBuffer
  )
  {
    if (dimensionSelector instanceof SingleValueHistoricalDimensionSelector) {
      return scanAndAggregateSingleValueDimension(
          (SingleValueHistoricalDimensionSelector) dimensionSelector,
          metricSelector,
          aggregator,
          cursor,
          positions,
          resultsBuffer
      );
    } else {
      return scanAndAggregateMultiValueDimension(
          dimensionSelector,
          metricSelector,
          aggregator,
          cursor,
          positions,
          resultsBuffer
      );
    }
  }

  private static long scanAndAggregateSingleValueDimension(
      SingleValueHistoricalDimensionSelector dimensionSelector,
      HistoricalFloatColumnSelector metricSelector,
      SimpleDoubleBufferAggregator aggregator,
      HistoricalCursor cursor,
      int[] positions,
      ByteBuffer resultsBuffer
  )
  {
    BitmapOffset offset = (BitmapOffset) cursor.copyOffset();
    long scannedRows = 0;
    int positionToAllocate = 0;
    while (offset.withinBounds() && !Thread.currentThread().isInterrupted()) {
      int rowNum = offset.getOffset();
      int dimIndex = dimensionSelector.getRowValue(rowNum);
      int position = positions[dimIndex];
      if (position >= 0) {
        aggregator.aggregate(resultsBuffer, position, metricSelector.get(rowNum));
      } else if (position == INIT_POSITION_VALUE) {
        positions[dimIndex] = positionToAllocate;
        resultsBuffer.putDouble(positionToAllocate, metricSelector.get(rowNum));
        positionToAllocate += 8;
      }
      scannedRows++;
      offset.increment();
    }
    return scannedRows;
  }

  private static long scanAndAggregateMultiValueDimension(
      HistoricalDimensionSelector dimensionSelector,
      HistoricalFloatColumnSelector metricSelector,
      SimpleDoubleBufferAggregator aggregator,
      HistoricalCursor cursor,
      int[] positions,
      ByteBuffer resultsBuffer
  )
  {
    BitmapOffset offset = (BitmapOffset) cursor.copyOffset();
    long scannedRows = 0;
    int positionToAllocate = 0;
    while (offset.withinBounds() && !Thread.currentThread().isInterrupted()) {
      int rowNum = offset.getOffset();
      float metric = metricSelector.get(rowNum);
      final IndexedInts dimValues = dimensionSelector.getRow(rowNum);
      final int dimSize = dimValues.size();
      for (int i = 0; i < dimSize; i++) {
        int dimIndex = dimValues.get(i);
        int position = positions[dimIndex];
        if (position >= 0) {
          aggregator.aggregate(resultsBuffer, position, metric);
        } else if (position == INIT_POSITION_VALUE) {
          positions[dimIndex] = positionToAllocate;
          resultsBuffer.putDouble(positionToAllocate, (double) metric);
          positionToAllocate += 8;
        }
      }
      scannedRows++;
      offset.increment();
    }
    return scannedRows;
  }
}
