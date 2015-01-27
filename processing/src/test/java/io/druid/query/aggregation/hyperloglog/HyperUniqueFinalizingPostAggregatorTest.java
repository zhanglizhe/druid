/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.query.aggregation.hyperloglog;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import io.druid.query.aggregation.CountAggregator;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.aggregation.post.ArithmeticPostAggregator;
import io.druid.query.aggregation.post.ConstantPostAggregator;
import io.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 */
public class HyperUniqueFinalizingPostAggregatorTest
{
  private final HashFunction fn = Hashing.murmur3_128();

  @Test
  public void testCompute() throws Exception
  {
    Random random = new Random(0l);
    HyperUniqueFinalizingPostAggregator postAggregator = new HyperUniqueFinalizingPostAggregator(
        "uniques"
    );
    HyperLogLogCollector collector = HyperLogLogCollector.makeLatestCollector();

    for (int i = 0; i < 100; ++i) {
      byte[] hashedVal = fn.hashLong(random.nextLong()).asBytes();
      collector.add(hashedVal);
    }

    double cardinality = (Double) postAggregator.compute(ImmutableMap.<String, Object>of("uniques", collector));

    Assert.assertTrue(cardinality == 99.37233005831612);
  }

  @Test
  public void testArithmeticAndHLL() throws Exception
  {
    Random random = new Random(0l);
    HyperUniqueFinalizingPostAggregator hllPostAggregator = new HyperUniqueFinalizingPostAggregator(
        "uniques"
    );
    HyperLogLogCollector collector = HyperLogLogCollector.makeLatestCollector();

    for (int i = 0; i < 100; ++i) {
      byte[] hashedVal = fn.hashLong(random.nextLong()).asBytes();
      collector.add(hashedVal);
    }

    final CountAggregator agg = new CountAggregator("rows");
    agg.aggregate();
    agg.aggregate();
    agg.aggregate();
    final Map<String, Object> metricValues = ImmutableMap.<String, Object>of(
        agg.getName(), agg.get(),
        hllPostAggregator.getName(), collector
    );

    final List<PostAggregator> postAggregatorList =
        Lists.newArrayList(
            new ConstantPostAggregator(
                "roku", 6
            ),
            new FieldAccessPostAggregator(
                "rows", "rows"
            ),
            hllPostAggregator
        );

    Assert.assertEquals(
        (99.37233005831612 + 3 + 6),
        new ArithmeticPostAggregator("add", "+", postAggregatorList).compute(metricValues)
    );
    Assert.assertEquals(
        (double)(99.37233005831612 * 3 * 6),
        ((Double) new ArithmeticPostAggregator("multiply", "*", postAggregatorList).compute(metricValues)),
        1e-11
    );
  }
}
