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

import com.metamx.common.logger.Logger;
import io.druid.query.QueryMetricsContext;
import io.druid.query.Result;
import io.druid.segment.Cursor;
import io.druid.segment.DimensionSelector;

import javax.annotation.Nullable;

public class TopNMapFn
{
  private static final Logger log = new Logger(TopNMapFn.class);

  static class TopNResult
  {
    final Result<TopNResultValue> queryResult;
    final long rowsScanned;

    TopNResult(Result<TopNResultValue> queryResult, long rowsScanned)
    {
      this.queryResult = queryResult;
      this.rowsScanned = rowsScanned;
    }
  }

  private final TopNQuery query;
  private final TopNAlgorithm topNAlgorithm;

  public TopNMapFn(
      TopNQuery query,
      TopNAlgorithm topNAlgorithm
  )
  {
    this.query = query;
    this.topNAlgorithm = topNAlgorithm;
  }

  /**
   * @param cursor cursor over rows to process
   * @param queryMetricsContext to emit metrics to
   * @param first if this is a first call of apply() in a series of similar calls over different ranges of rows, to
   *              emit metrics and log some diagnostic things only once
   * @return
   */
  @SuppressWarnings("unchecked")
  public TopNResult apply(Cursor cursor, @Nullable QueryMetricsContext queryMetricsContext, boolean first)
  {
    final DimensionSelector dimSelector = cursor.makeDimensionSelector(
        query.getDimensionSpec()
    );
    if (dimSelector == null) {
      return null;
    }

    TopNParams params = null;
    try {
      params = topNAlgorithm.makeInitParams(dimSelector, cursor);
      if (first && queryMetricsContext != null) {
        log.debug("TopN cursor: %s", cursor);
        log.debug("TopN dimension selector: %s", dimSelector);
        Long numValuesPerPass = QueryMetricsContext.roundMetric(params.getNumValuesPerPass(), 2);
        queryMetricsContext.setDimension("numValuesPerPass", numValuesPerPass);
        Long valueCardinality = QueryMetricsContext.roundMetric(params.getCardinality(), 2);
        queryMetricsContext.setDimension("valueCardinality", valueCardinality);
      }

      TopNResultBuilder resultBuilder = BaseTopNAlgorithm.makeResultBuilder(params, query);

      long rowsScanned = topNAlgorithm.run(params, resultBuilder, null, queryMetricsContext);

      return new TopNResult(resultBuilder.build(), rowsScanned);
    }
    finally {
      topNAlgorithm.cleanup(params);
    }
  }
}
