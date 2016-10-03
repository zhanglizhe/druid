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

import org.junit.Test;

import static io.druid.query.QueryMetrics.roundMetric;
import static org.junit.Assert.assertEquals;

public class RoundMetricTest
{
  @Test
  public void testRoundMetric()
  {
    assertEquals(0, roundMetric(0, 1));
    assertEquals(5, roundMetric(5, 1));
    assertEquals(60, roundMetric(55, 1));
    assertEquals(50, roundMetric(54, 1));
    assertEquals(100, roundMetric(149, 1));
    assertEquals(200, roundMetric(150, 1));
    assertEquals(149, roundMetric(149, 3));
    assertEquals(150, roundMetric(149, 2));
    assertEquals(2147483600, roundMetric(2147483647, 8));
    assertEquals(2147483650L, roundMetric(2147483647, 9));
    assertEquals(2147483647, roundMetric(2147483647, 10));
    assertEquals(2147483647, roundMetric(2147483647, Integer.MAX_VALUE));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRoundNegativeMetric()
  {
    roundMetric(-1, 1);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRoundMetricNonZeroDigits()
  {
    roundMetric(1, 0);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRoundMetricNonNegativeDigits()
  {
    roundMetric(1, -1);
  }
}
