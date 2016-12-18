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

package io.druid.query.dimension;

import io.druid.segment.DimensionSelector;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.data.ListBasedIndexedInts;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ForwardingDimensionSelector<DimensionSelectorType extends DimensionSelector> implements DimensionSelector
{
  protected final DimensionSelectorType selector;
  protected final Map<Integer, Integer> forwardMapping;
  private final int[] reverseMapping;

  public ForwardingDimensionSelector(
      DimensionSelectorType selector,
      Map<Integer, Integer> forwardMapping,
      int[] reverseMapping
  )
  {
    this.selector = selector;
    this.forwardMapping = forwardMapping;
    this.reverseMapping = reverseMapping;
  }

  @Override
  public IndexedInts getRow()
  {
    IndexedInts baseRow = selector.getRow();
    return forward(baseRow);
  }

  protected IndexedInts forward(IndexedInts baseRow)
  {
    List<Integer> result = new ArrayList<>(baseRow.size());

    for (int value : baseRow) {
      if (forwardMapping.containsKey(value)) {
        result.add(forwardMapping.get(value));
      }
    }

    return new ListBasedIndexedInts(result);
  }

  @Override
  public int constantRowSize()
  {
    return selector.constantRowSize();
  }

  @Override
  public int getValueCardinality()
  {
    return forwardMapping.size();
  }

  @Override
  public String lookupName(int id)
  {
    return selector.lookupName(reverseMapping[id]);
  }

  @Override
  public int lookupId(String name)
  {
    return forwardMapping.get(selector.lookupId(name));
  }

  @Override
  public String getDimensionSelectorType()
  {
    return getClass().getName() + "[selector=" + selector.getDimensionSelectorType() + "]";
  }

  @Override
  public String toString()
  {
    return "ForwardingDimensionSelector{" +
           "selector=" + selector +
           '}';
  }
}
