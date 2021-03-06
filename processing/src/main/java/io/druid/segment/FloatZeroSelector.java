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

import io.druid.segment.historical.HistoricalFloatColumnSelector;

/**
 * {@link FloatColumnSelector} which always returns 0.0f, e. g. if a column not found.
 */
public final class FloatZeroSelector implements HistoricalFloatColumnSelector
{
  private static final FloatZeroSelector SINGLETON = new FloatZeroSelector();

  public static FloatZeroSelector singleton()
  {
    return SINGLETON;
  }

  private FloatZeroSelector() {}

  @Override
  public float get()
  {
    return 0.0f;
  }

  @Override
  public float get(int rowNum)
  {
    return 0.0f;
  }

  @Override
  public String getFloatColumnSelectorType()
  {
    return getClass().getName();
  }
}
