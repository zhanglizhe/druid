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

import com.google.common.base.Function;
import com.metamx.common.guava.Accumulator;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Yielder;
import com.metamx.common.guava.YieldingAccumulator;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceMetricEvent;
import io.druid.common.guava.MoreSequences;
import io.druid.common.guava.SequenceWrapper;
import io.druid.query.topn.TopNQuery;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 */
public class MetricsEmittingQueryRunner<T> implements QueryRunner<T>
{
  private final ServiceEmitter emitter;
  private final Function<Query<T>, ServiceMetricEvent.Builder> builderFn;
  private final QueryRunner<T> queryRunner;
  private final long creationTimeNs;
  private final String metricName;
  private final Map<String, String> userDimensions;

  private MetricsEmittingQueryRunner(
      ServiceEmitter emitter,
      Function<Query<T>, ServiceMetricEvent.Builder> builderFn,
      QueryRunner<T> queryRunner,
      long creationTimeNs,
      String metricName,
      Map<String, String> userDimensions
  )
  {
    this.emitter = emitter;
    this.builderFn = builderFn;
    this.queryRunner = queryRunner;
    this.creationTimeNs = creationTimeNs;
    this.metricName = metricName;
    this.userDimensions = userDimensions;
  }

  public MetricsEmittingQueryRunner(
      ServiceEmitter emitter,
      Function<Query<T>, ServiceMetricEvent.Builder> builderFn,
      QueryRunner<T> queryRunner,
      String metricName,
      Map<String, String> userDimensions
  )
  {
    this(emitter, builderFn, queryRunner, -1, metricName, userDimensions);
  }

  public MetricsEmittingQueryRunner<T> withWaitMeasuredFromNow()
  {
    return new MetricsEmittingQueryRunner<>(
        emitter,
        builderFn,
        queryRunner,
        System.nanoTime(),
        metricName,
        userDimensions
    );
  }

  @Override
  public Sequence<T> run(final Query<T> query, final Map<String, Object> responseContext)
  {
    final ServiceMetricEvent.Builder builder = builderFn.apply(query);

    for (Map.Entry<String, String> userDimension : userDimensions.entrySet()) {
      builder.setDimension(userDimension.getKey(), userDimension.getValue());
    }

    Sequence<T> statusRecordingSequence = new Sequence<T>()
    {
      @Override
      public <OutType> OutType accumulate(OutType outType, Accumulator<OutType, T> accumulator)
      {
        try {
          return queryRunner.run(query, responseContext).accumulate(outType, accumulator);
        }
        catch (Throwable t) {
          builder.setDimension(DruidMetrics.STATUS, "failed");
          throw t;
        }
      }

      @Override
      public <OutType> Yielder<OutType> toYielder(OutType initValue, YieldingAccumulator<OutType, T> accumulator)
      {
        try {
          Yielder<OutType> baseYielder = queryRunner.run(query, responseContext).toYielder(initValue, accumulator);
          return makeYielder(baseYielder, builder);
        }
        catch (Throwable t) {
          builder.setDimension(DruidMetrics.STATUS, "failed");
          throw t;
        }
      }

      private <OutType> Yielder<OutType> makeYielder(
          final Yielder<OutType> yielder,
          final ServiceMetricEvent.Builder builder
      )
      {
        return new Yielder<OutType>()
        {
          @Override
          public OutType get()
          {
            return yielder.get();
          }

          @Override
          public Yielder<OutType> next(OutType initValue)
          {
            try {
              return makeYielder(yielder.next(initValue), builder);
            }
            catch (Throwable t) {
              builder.setDimension(DruidMetrics.STATUS, "failed");
              throw t;
            }
          }

          @Override
          public boolean isDone()
          {
            return yielder.isDone();
          }

          @Override
          public void close() throws IOException
          {
            //noinspection unused
            try (Yielder<?> toClose = yielder) {
              if (!isDone() && builder.getDimension(DruidMetrics.STATUS) == null) {
                builder.setDimension(DruidMetrics.STATUS, "short");
              }
            }
          }
        };
      }
    };
    return MoreSequences.wrap(statusRecordingSequence, new SequenceWrapper()
    {
      private long startTimeNs;
      private QueryMetricsContext queryMetricsContext;

      @Override
      public void open()
      {
        startTimeNs = System.nanoTime();
        queryMetricsContext = new QueryMetricsContext();
        if (query instanceof TopNQuery) {
          // Response context seems to be the easiest way to transmit queryMetricsContext between queryRunner and
          // queryEngine, because it doesn't require to change interfaces throughout the codebase. If more types of
          // queries (not just topN) are interested in queryMetricsContext, it should probably be added to
          // queryRunner.run() parameters.
          //
          // This queryMetricsContext is extracted from the responseContext in TopNQueryRunnerFactory.createRunner()
          // method.
          responseContext.put("queryMetricsContext", queryMetricsContext);
        }
      }

      @Override
      public void close()
      {
        if (query instanceof TopNQuery) {
          responseContext.remove("queryMetricsContext");
        }
        long timeTakenNs = System.nanoTime() - startTimeNs;

        for (Map.Entry<String, String> dimension : queryMetricsContext.singleValueDimensions.entrySet()) {
          builder.setDimension(dimension.getKey(), dimension.getValue());
        }
        for (Map.Entry<String, String[]> dimension : queryMetricsContext.multiValueDimensions.entrySet()) {
          builder.setDimension(dimension.getKey(), dimension.getValue());
        }

        emitter.emit(builder.build(metricName + "Ns", timeTakenNs));
        emitter.emit(builder.build(metricName, TimeUnit.NANOSECONDS.toMillis(timeTakenNs)));


        if (creationTimeNs > 0) {
          long waitTimeNs = startTimeNs - creationTimeNs;
          emitter.emit(builder.build("query/wait/timeNs", waitTimeNs));
          emitter.emit(builder.build("query/wait/time", TimeUnit.NANOSECONDS.toMillis(waitTimeNs)));
        }

        for (Map.Entry<String, Number> queryMetric : queryMetricsContext.metrics.entrySet()) {
          emitter.emit(builder.build(queryMetric.getKey(), queryMetric.getValue()));
        }
      }
    });
  }
}
