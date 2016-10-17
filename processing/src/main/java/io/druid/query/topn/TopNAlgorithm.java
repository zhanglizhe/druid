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

import io.druid.query.aggregation.Aggregator;
import io.druid.segment.Cursor;
import io.druid.segment.DimensionSelector;

import javax.annotation.Nullable;

/**
 */
public interface TopNAlgorithm<DimValSelector, Parameters extends TopNParams>
{
  public static final Aggregator[] EMPTY_ARRAY = {};
  public static final int INIT_POSITION_VALUE = -1;
  public static final int SKIP_POSITION_VALUE = -2;

  public TopNParams makeInitParams(DimensionSelector dimSelector, Cursor cursor);

  /**
   * @param resultBuilder "output parameter"
   * @param topNQueryMetrics "ouput parameter", if topNQueryMetrics is not null, increment it's metrics with the values
   *                         accumulated during this algorithm run
   */
  public void run(
      Parameters params,
      DimValSelector dimValSelector,
      TopNResultBuilder resultBuilder,
      @Nullable TopNQueryMetrics topNQueryMetrics
  );

  public void cleanup(Parameters params);
}
