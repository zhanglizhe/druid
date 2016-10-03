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


import com.google.common.math.IntMath;
import com.google.common.math.LongMath;
import com.metamx.common.IAE;
import com.metamx.emitter.service.ServiceMetricEvent;

import javax.annotation.Nullable;
import java.math.RoundingMode;

public final class QueryMetrics
{

  public static final String TOTAL_ROWS = "totalRows";
  public static final String BITMAP_FILTERED_ROWS = "bitmapFilteredRows";
  public static final String POST_FILTERS = "postFilters";

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

  /**
   * Checks if {@code metricBuilder} is non-null and converts the given {@code metric} (e. g. Integer or Long) to String
   * itself.
   *
   * @param metricBuilder if null, this method call is a no-op
   */
  public static void setDimension(@Nullable ServiceMetricEvent.Builder metricBuilder, String dimension, Object metric)
  {
    if (metricBuilder != null) {
      metricBuilder.setDimension(dimension, metric.toString());
    }
  }

  private QueryMetrics() {}
}
