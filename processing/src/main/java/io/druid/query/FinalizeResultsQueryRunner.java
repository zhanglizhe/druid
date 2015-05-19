/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.query;

import com.google.common.collect.ImmutableMap;
import com.metamx.common.guava.Sequence;
import io.druid.query.aggregation.MetricManipulationFn;
import io.druid.query.aggregation.MetricManipulatorFns;

import java.util.Map;

/**
 */
public class FinalizeResultsQueryRunner<T> implements QueryRunner<T>
{
  private final QueryRunner<T> baseRunner;
  private final QueryToolChest<T, Query<T>> toolChest;

  public FinalizeResultsQueryRunner(
      QueryRunner<T> baseRunner,
      QueryToolChest<T, Query<T>> toolChest
  )
  {
    this.baseRunner = baseRunner;
    this.toolChest = toolChest;
  }

  @Override
  public Sequence<T> run(final Query<T> query, Map<String, Object> responseContext)
  {
    final boolean shouldFinalize = query.getContextFinalize(true);
    final Query<T> queryToRun;
    if (shouldFinalize) {
      queryToRun = query.withOverriddenContext(ImmutableMap.<String, Object>of("finalize", false));
    } else {
      queryToRun = query;
    }
    Sequence<T> outSequence = baseRunner.run(queryToRun, responseContext);
    if(shouldFinalize) {
      outSequence = toolChest.finalizeSequence(query, outSequence);
    }
    return outSequence;
  }
}
