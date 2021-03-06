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

package io.druid.segment.historical;

import java.util.Map;

public final class SingleValueHistoricalForwardingDimensionSelector
    extends HistoricalForwardingDimensionSelector<SingleValueHistoricalDimensionSelector>
    implements SingleValueHistoricalDimensionSelector
{
  public SingleValueHistoricalForwardingDimensionSelector(
      SingleValueHistoricalDimensionSelector selector,
      Map<Integer, Integer> forwardMapping,
      int[] reverseMapping
  )
  {
    super(selector, forwardMapping, reverseMapping);
  }

  @Override
  public int getRowValue()
  {
    int originalRowValue = selector.getRowValue();
    Integer forwardedValue = forwardMapping.get(originalRowValue);
    return forwardedValue != null ? forwardedValue : 0;
  }

  @Override
  public int getRowValue(int rowNum)
  {
    int originalRowValue = selector.getRowValue(rowNum);
    Integer forwardedValue = forwardMapping.get(originalRowValue);
    return forwardedValue != null ? forwardedValue : 0;
  }
}
