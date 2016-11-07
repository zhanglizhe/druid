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

package io.druid.query;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.math.IntMath;
import com.google.common.math.LongMath;
import com.metamx.common.IAE;

import java.math.RoundingMode;
import java.util.HashMap;
import java.util.Map;

/**
 * This object facilitates collecting dimensions and metrics during the query run. Those metrics are emitted in
 * {@link MetricsEmittingQueryRunner}. This class is union of relatively independent entities - {@link
 * #singleValueDimensions}, {@link #multiValueDimensions} and {@link #metrics}, and it's purpose is merely to be passed
 * as a single argument around methods during the query execution: {@code queryMetricsContext}, rather than three
 * separate arguments (and this number might grow in the future).
 */
public final class QueryMetricsContext
{

  /**
   * Return the closest number to the given {@code metric}, that has not more than {@code significantDigits} non-zero
   * digits. Examples: {@code roundMetric(5, 2)} = 5, {@code roundMetric(54, 2)} = 54, {@code roundMetric(543, 2)} =
   * 540, {@code roundMetric(567, 2)} = 570.
   * @param metric a number to round
   * @param significantDigits a number of significant digits to leave
   * @return rounded metric
   * @throws IllegalArgumentException if metric is negative; if significant digits <= 0
   */
  public static long roundMetric(long metric, int significantDigits)
  {
    if (metric < 0) {
      throw new IAE("metric should be non-negative: %d", metric);
    }
    if (significantDigits <= 0) {
      throw new IAE("significant digits must be positive: %d", significantDigits);
    }

    int log10 = metric == 0 ? 0 : LongMath.log10(metric, RoundingMode.UP);
    int granularity = IntMath.pow(10, Math.max(log10 - significantDigits, 0));
    long metricUp = metric + (granularity / 2);
    if (metricUp < 0) { // overflow
      metricUp = Long.MAX_VALUE;
    }
    return metricUp - (metricUp % granularity);
  }

  @JsonProperty("singleValueDimensions")
  public final Map<String, String> singleValueDimensions;
  @JsonProperty("multiValueDimensions")
  public final Map<String, String[]> multiValueDimensions;
  @JsonProperty("metrics")
  public final Map<String, Number> metrics;

  public QueryMetricsContext()
  {
    this(new HashMap<String, String>(), new HashMap<String, String[]>(), new HashMap<String, Number>());
  }

  @JsonCreator
  public QueryMetricsContext(
      @JsonProperty("singleValueDimensions") Map<String, String> singleValueDimensions,
      @JsonProperty("multiValueDimensions") Map<String, String[]> multiValueDimensions,
      @JsonProperty("metrics") Map<String, Number> metrics
  )
  {
    this.singleValueDimensions = singleValueDimensions;
    this.multiValueDimensions = multiValueDimensions;
    this.metrics = metrics;
  }

  /**
   * Equivalent to {@link #singleValueDimensions}{@code .put(dimension, value.toString())}.
   */
  public void setDimension(String dimension, Object value)
  {
    singleValueDimensions.put(dimension, value.toString());
  }
}
