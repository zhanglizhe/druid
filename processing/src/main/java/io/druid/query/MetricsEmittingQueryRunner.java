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

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 */
public class MetricsEmittingQueryRunner<T> implements QueryRunner<T>
{
  private static final boolean emitTimeNsMetrics = Boolean.getBoolean("emitTimeNsMetrics");

  private static final double extraDimensionsRatio = Double.parseDouble(
      System.getProperty("extraDimensionsRatio", "0.1")
  );

  private static final double extraMetricsRatio = Double.parseDouble(System.getProperty("extraMetricsRatio", "0.1"));

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

      @Override
      public void open()
      {
        startTimeNs = System.nanoTime();
      }

      @Override
      public void close()
      {
        long timeTakenNs = System.nanoTime() - startTimeNs;
        QueryMetricsContext queryMetricsContext =
            (QueryMetricsContext) responseContext.remove("queryMetricsContext");
        double random = queryMetricsContext != null ? ThreadLocalRandom.current().nextDouble() : 0;
        if (queryMetricsContext != null && random < extraDimensionsRatio) {
          for (Map.Entry<String, String> dimension : queryMetricsContext.singleValueDimensions.entrySet()) {
            builder.setDimension(dimension.getKey(), dimension.getValue());
          }
          for (Map.Entry<String, String[]> dimension : queryMetricsContext.multiValueDimensions.entrySet()) {
            builder.setDimension(dimension.getKey(), dimension.getValue());
          }
        }
        if (emitTimeNsMetrics) {
          emitter.emit(builder.build(metricName + "Ns", timeTakenNs));
        }
        emitter.emit(builder.build(metricName, TimeUnit.NANOSECONDS.toMillis(timeTakenNs)));


        if (creationTimeNs > 0) {
          long waitTimeNs = startTimeNs - creationTimeNs;
          if (emitTimeNsMetrics) {
            emitter.emit(builder.build("query/wait/timeNs", waitTimeNs));
          }
          emitter.emit(builder.build("query/wait/time", TimeUnit.NANOSECONDS.toMillis(waitTimeNs)));
        }

        if (queryMetricsContext != null && random < extraMetricsRatio) {
          for (Map.Entry<String, Number> queryMetric : queryMetricsContext.metrics.entrySet()) {
            emitter.emit(builder.build(queryMetric.getKey(), queryMetric.getValue()));
          }
        }
      }
    });
  }
}
