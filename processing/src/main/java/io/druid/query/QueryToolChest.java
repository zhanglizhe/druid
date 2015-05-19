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

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.metamx.common.IAE;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.emitter.service.ServiceMetricEvent;
import io.druid.query.aggregation.MetricManipulationFn;
import io.druid.query.aggregation.MetricManipulatorFns;
import io.druid.timeline.LogicalSegment;

import javax.annotation.Nullable;
import java.util.List;

/**
 * The broker-side (also used by server in some cases) API for a specific Query type.  This API is still undergoing
 * evolution and is only semi-stable, so proprietary Query implementations should be ready for the potential
 * maintenance burden when upgrading versions.
 */
public abstract class QueryToolChest<ResultType, QueryType extends Query<ResultType>>
{
  /**
   * This method wraps a QueryRunner.  The input QueryRunner, by contract, will provide a series of
   * ResultType objects in time order (ascending).  This method should return a new QueryRunner that
   * potentially merges the stream of ordered ResultType objects.
   *
   * @param runner A QueryRunner that provides a series of ResultType objects in time order (ascending)
   *
   * @return a QueryRunner that potentialy merges the stream of ordered ResultType objects
   */
  public abstract QueryRunner<ResultType> mergeResults(QueryRunner<ResultType> runner);

  /**
   * Manipulate the metrics in a sequence by applying the MetricManipulatorFn to each of the metrics.
   * <p/>
   * This exists because the QueryToolChest is the only thing that understands the internal serialization
   * format of ResultType, so it's primary responsibility is to "decompose" that structure and apply the
   * given function to all metrics.
   *
   * @param query       The query which was used to create the Sequence. This query defines which metrics will be manipulated
   * @param sequence    The original sequence. May actually be a `Sequence<Result<BySegmentResultValue>>` and any impl
   *                    must account for that
   * @param manipulator The manipulation to perform on the metrics in the sequence.
   *
   * @return A sequence with the appropriate manipulators applied to the input sequence's metrics
   */
  public Sequence<ResultType> manipulateMetrics(
      final QueryType query,
      final Sequence<ResultType> sequence,
      final MetricManipulationFn manipulator
  )
  {
    if (manipulator == null) {
      return sequence;
    }
    final Function<ResultType, ResultType> resultManipulator = new Function<ResultType, ResultType>()
    {
      @Nullable
      @Override
      public ResultType apply(@Nullable ResultType input)
      {
        return manipulateMetrics(query, input, manipulator);
      }
    };
    if (query.getContextBySegment(false)) {
      return bySegmentRepackage(
          sequence,
          resultManipulator
      );
    } else {
      return Sequences.map(
          sequence,
          resultManipulator
      );
    }
  }

  /**
   * Finalize the sequence for this query. May be overridden if the subclass has special things it needs to do.
   * @param query The query which generated the sequence
   * @param sequence The sequence that needs finalizing
   * @return A Sequence which has results finalized.
   */
  public Sequence<ResultType> finalizeSequence(QueryType query, Sequence<ResultType> sequence){
    return manipulateMetrics(query, sequence, MetricManipulatorFns.finalizing());
  }

  /**
   * Manipulate the metrics in a result by applying the MetricManipulatorFn to each of the metrics.
   * <p/>
   * This exists because the QueryToolChest is the only thing that understands the internal serialization
   * format of ResultType, so it's primary responsibility is to "decompose" that structure and apply the
   * given function to all metrics.
   *
   * @param query       The query which was used to create the Sequence. This query defines which metrics will be manipulated
   * @param result      The original result. Is expected to be actual result type and not a by segment result
   * @param manipulator The manipulation to perform on the metrics in the sequence. A null value should not manipulate the metrics.
   *
   * @return A sequence with the appropriate manipulators applied to the input sequence's metrics
   */
  protected abstract ResultType manipulateMetrics(QueryType query, ResultType result, @Nullable MetricManipulationFn manipulator);

  /**
   * This method doesn't belong here, but it's here for now just to make it work.  The method needs to
   * take a Sequence of Sequences and return a single Sequence of ResultType objects in time-order (ascending)
   * <p/>
   * This method assumes that its input sequences provide values already in sorted order.
   * Even more specifically, it assumes that the individual sequences are also ordered by their first element.
   * <p/>
   * In the vast majority of cases, this should just be implemented with:
   * <p/>
   * return new OrderedMergeSequence<>(getOrdering(), seqOfSequences);
   *
   * @param seqOfSequences sequence of sequences to be merged
   *
   * @return the sequence of merged results
   */
  public abstract Sequence<ResultType> mergeSequences(Sequence<Sequence<ResultType>> seqOfSequences);

  /**
   * This method doesn't belong here, but it's here for now just to make it work.  The method needs to
   * take a Sequence of Sequences and return a single Sequence of ResultType objects in time-order (ascending)
   * <p/>
   * This method assumes that its input sequences provide values already in sorted order, but, unlike
   * mergeSequences, it does *not* assume that the individual sequences are also ordered by their first element.
   * <p/>
   * In the vast majority if ocases, this hsould just be implemented with:
   * <p/>
   * return new MergeSequence<>(getOrdering(), seqOfSequences);
   *
   * @param seqOfSequences sequence of sequences to be merged
   *
   * @return the sequence of merged results
   */
  public abstract Sequence<ResultType> mergeSequencesUnordered(Sequence<Sequence<ResultType>> seqOfSequences);


  /**
   * Creates a builder that is used to generate a metric for this specific query type.  This exists
   * to allow for query-specific dimensions on metrics.  That is, the ToolChest is expected to set some
   * meaningful dimensions for metrics given this query type.  Examples might be the topN threshhold for
   * a TopN query or the number of dimensions included for a groupBy query.
   *
   * @param query The query that is being processed
   *
   * @return A MetricEvent.Builder that can be used to make metrics for the provided query
   */
  public abstract ServiceMetricEvent.Builder makeMetricBuilder(QueryType query);

  /**
   * Returns a TypeReference object that is just passed through to Jackson in order to deserialize
   * the results of this type of query.
   *
   * @return A TypeReference to indicate to Jackson what type of data will exist for this query
   */
  public abstract TypeReference<ResultType> getResultTypeReference();

  /**
   * Returns a CacheStrategy to be used to load data into the cache and remove it from the cache.
   * <p/>
   * This is optional.  If it returns null, caching is effectively disabled for the query.
   *
   * @param query The query whose results might be cached
   * @param <T>   The type of object that will be stored in the cache
   *
   * @return A CacheStrategy that can be used to populate and read from the Cache
   */
  public <T> CacheStrategy<ResultType, T, QueryType> getCacheStrategy(QueryType query)
  {
    return null;
  }

  /**
   * This method is called to allow the query to prune segments that it does not believe need to actually
   * be queried.  It can use whatever criteria it wants in order to do the pruning, it just needs to
   * return the list of Segments it actually wants to see queried.
   *
   * @param query    The query being processed
   * @param segments The list of candidate segments to be queried
   * @param <T>      A Generic parameter because Java is cool
   *
   * @return The list of segments to actually query
   */
  public <T extends LogicalSegment> List<T> filterSegments(QueryType query, List<T> segments)
  {
    return segments;
  }

  /**
   * Repackages a bySegment sequence running each result through the resultManipulator
   *
   * @param bySegmentSequence The sequence that includes results by segment
   * @param resultManipulator       The resultManipulator on the result
   *
   * @return A sequence of bySegment results `Sequence<Result<BySegmentValue>>`
   */
  protected final Sequence bySegmentRepackage(
      Sequence bySegmentSequence,
      final Function<ResultType, ResultType> resultManipulator
  )
  {
    return Sequences.map(
        bySegmentSequence,
        new Function()
        {
          @Nullable
          @Override
          public Object apply(@Nullable Object input)
          {
            if (input == null) {
              return null;
            }
            final Result result = (Result)input;
            input = result.getValue();
            if (input instanceof BySegmentResultValue) {
              final BySegmentResultValue bySegmentResultValue = (BySegmentResultValue) input;

              return new Result<>(
                  result.getTimestamp(),
                  new BySegmentResultValueClass(
                      Lists.transform(
                          bySegmentResultValue.getResults(), new Function()
                          {
                            @Nullable
                            @Override
                            public Object apply(@Nullable Object input)
                            {
                              if (input == null) {
                                return null;
                              }
                              return resultManipulator.apply((ResultType) input);
                            }
                          }
                      ),
                      bySegmentResultValue.getSegmentId(),
                      bySegmentResultValue.getInterval()
                  )
              );
            } else {
              throw new IAE(
                  "Unknown result object class. Expected [%s] found [%s]",
                  BySegmentResultValue.class.getCanonicalName(),
                  input.getClass().getCanonicalName()
              );
            }
          }
        }
    );
  }
}
