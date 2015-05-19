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

package io.druid.query.topn;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Ints;
import com.google.inject.Inject;
import com.metamx.common.ISE;
import com.metamx.common.guava.MergeSequence;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.common.guava.nary.BinaryFn;
import com.metamx.emitter.service.ServiceMetricEvent;
import io.druid.collections.OrderedMergeSequence;
import io.druid.granularity.QueryGranularity;
import io.druid.query.BySegmentResultValue;
import io.druid.query.CacheStrategy;
import io.druid.query.DruidMetrics;
import io.druid.query.IntervalChunkingQueryRunnerDecorator;
import io.druid.query.Query;
import io.druid.query.QueryCacheHelper;
import io.druid.query.QueryRunner;
import io.druid.query.QueryToolChest;
import io.druid.query.Result;
import io.druid.query.ResultGranularTimestampComparator;
import io.druid.query.ResultMergeQueryRunner;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.AggregatorUtil;
import io.druid.query.aggregation.MetricManipulationFn;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.filter.DimFilter;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 */
public class TopNQueryQueryToolChest extends QueryToolChest<Result<TopNResultValue>, TopNQuery>
{
  private static final byte TOPN_QUERY = 0x1;
  private static final TypeReference<Result<TopNResultValue>> TYPE_REFERENCE = new TypeReference<Result<TopNResultValue>>()
  {
  };
  private static final TypeReference<Object> OBJECT_TYPE_REFERENCE = new TypeReference<Object>()
  {
  };
  private final TopNQueryConfig config;

  private final IntervalChunkingQueryRunnerDecorator intervalChunkingQueryRunnerDecorator;

  @Inject
  public TopNQueryQueryToolChest(
      TopNQueryConfig config,
      IntervalChunkingQueryRunnerDecorator intervalChunkingQueryRunnerDecorator
  )
  {
    this.config = config;
    this.intervalChunkingQueryRunnerDecorator = intervalChunkingQueryRunnerDecorator;
  }

  protected static String[] extractFactoryName(final List<AggregatorFactory> aggregatorFactories)
  {
    return Lists.transform(
        aggregatorFactories, new Function<AggregatorFactory, String>()
        {
          @Nullable
          @Override
          public String apply(@Nullable AggregatorFactory input)
          {
            return input.getName();
          }
        }
    ).toArray(new String[0]);
  }

  private static List<PostAggregator> prunePostAggregators(TopNQuery query)
  {
    return AggregatorUtil.pruneDependentPostAgg(
        query.getPostAggregatorSpecs(),
        query.getTopNMetricSpec().getMetricName(query.getDimensionSpec())
    );
  }

  @Override
  public QueryRunner<Result<TopNResultValue>> mergeResults(
      final QueryRunner<Result<TopNResultValue>> baseRunner
  )
  {
    return new ThresholdAdjustingQueryRunner(
        new ResultMergeQueryRunner<Result<TopNResultValue>>(
            intervalChunkingQueryRunnerDecorator.decorate(
                new QueryRunner<Result<TopNResultValue>>()
                {
                  @Override
                  public Sequence<Result<TopNResultValue>> run(
                      Query<Result<TopNResultValue>> query, Map<String, Object> responseContext
                  )
                  {
                    TopNQuery topNQuery = (TopNQuery) query;
                    topNQuery = topNQuery.withPostAggregatorSpecs(prunePostAggregators(topNQuery));
                    final String dimKey = topNQuery.getDimensionSpec().getOutputName();
                    final ExtractionFn postExtractionFn;
                    if (TopNQueryEngine.canApplyExtractionInPost(topNQuery)) {
                      final DimensionSpec oldSpec = ((TopNQuery) query).getDimensionSpec();
                      postExtractionFn = topNQuery.getDimensionSpec().getExtractionFn();
                      topNQuery = topNQuery.withDimensionSpec(
                          // Preserve dimension name in subqueries
                          new DefaultDimensionSpec(oldSpec.getDimension(), oldSpec.getOutputName())
                      );

                    } else {
                      postExtractionFn = null;
                    }
                    Function<Result<TopNResultValue>, Result<TopNResultValue>> postAggComputator = makePostAggComputator(
                        topNQuery
                    );

                    if (postExtractionFn != null) {
                      final Function<Result<TopNResultValue>, Result<TopNResultValue>> baseComputator = postAggComputator;
                      postAggComputator = new Function<Result<TopNResultValue>, Result<TopNResultValue>>()
                      {
                        @Nullable
                        @Override
                        public Result<TopNResultValue> apply(@Nullable Result<TopNResultValue> input)
                        {
                          if (input == null) {
                            return null;
                          }
                          return manipulateDims(
                              baseComputator.apply(input),
                              new Function<DimensionAndMetricValueExtractor, DimensionAndMetricValueExtractor>()
                              {
                                @Nullable
                                @Override
                                public DimensionAndMetricValueExtractor apply(
                                    @Nullable DimensionAndMetricValueExtractor input
                                )
                                {
                                  if (input == null) {
                                    return null;
                                  }
                                  final String oldDimVal = input.getStringDimensionValue(dimKey);
                                  final Map<String, Object> map = new HashMap<>(input.getBaseObject());
                                  map.put(dimKey, postExtractionFn.apply(oldDimVal));
                                  return new DimensionAndMetricValueExtractor(map);
                                }
                              }
                          );
                        }
                      };
                    }
                    if (query.getContextBySegment(false)) {
                      return bySegmentRepackage(
                          baseRunner.run(topNQuery, responseContext),
                          postAggComputator
                      );
                    } else {
                      return Sequences.map(
                          baseRunner.run(topNQuery, responseContext),
                          postAggComputator
                      );
                    }
                  }
                }
                , this
            )
        )
        {
          @Override
          protected Ordering<Result<TopNResultValue>> makeOrdering(Query<Result<TopNResultValue>> query)
          {
            return Ordering.from(
                new ResultGranularTimestampComparator<TopNResultValue>(
                    ((TopNQuery) query).getGranularity()
                )
            );
          }

          @Override
          protected BinaryFn<Result<TopNResultValue>, Result<TopNResultValue>, Result<TopNResultValue>> createMergeFn(
              Query<Result<TopNResultValue>> input
          )
          {
            TopNQuery query = (TopNQuery) input;
            return new TopNBinaryFn(
                TopNResultMerger.identity,
                query.getGranularity(),
                query.getDimensionSpec(),
                query.getTopNMetricSpec(),
                query.getThreshold(),
                query.getAggregatorSpecs(),
                query.getPostAggregatorSpecs()
            );
          }
        },
        config.getMinTopNThreshold()
    );
  }

  @Override
  public Sequence<Result<TopNResultValue>> finalizeSequence(TopNQuery query, Sequence<Result<TopNResultValue>> sequence)
  {
    final Function<Result<TopNResultValue>, Result<TopNResultValue>> postAggComputator = makePostAggComputator(
        (TopNQuery) query
    );
    if (query.getContextBySegment(false)) {
      return super.finalizeSequence(
          query,
          bySegmentRepackage(sequence, postAggComputator)
      );
    } else {
      return super.finalizeSequence(
          query,
          Sequences.map(sequence, postAggComputator)
      );
    }
  }

  private static Result<TopNResultValue> manipulateDims(
      Result<TopNResultValue> input,
      Function<DimensionAndMetricValueExtractor, DimensionAndMetricValueExtractor> fn
  )
  {
    if (input == null) {
      return null;
    }
    return new Result<>(
        input.getTimestamp(),
        new TopNResultValue(
            Lists.transform(
                input.getValue().getValue(),
                fn
            )
        )
    );
  }

  @Override
  protected Result<TopNResultValue> manipulateMetrics(
      final TopNQuery query, Result<TopNResultValue> result, final @Nullable MetricManipulationFn manipulator
  )
  {
    if (result == null) {
      return null;
    }
    if (manipulator == null) {
      return result;
    }
    return new Result<>(
        result.getTimestamp(),
        new TopNResultValue(
            Lists.transform(
                result.getValue().getValue(),
                new Function<DimensionAndMetricValueExtractor, DimensionAndMetricValueExtractor>()
                {
                  @Override
                  public DimensionAndMetricValueExtractor apply(
                      DimensionAndMetricValueExtractor input
                  )
                  {
                    final Map<String, Object> map = new HashMap<>(input.getBaseObject());
                    for (AggregatorFactory agg : query.getAggregatorSpecs()) {
                      final String aggName = agg.getName();
                      final Object oldVal = map.get(aggName);
                      map.put(aggName, manipulator.manipulate(agg, oldVal));
                    }
                    return new DimensionAndMetricValueExtractor(map);
                  }
                }
            )
        )
    );
  }

  @Override
  public Sequence<Result<TopNResultValue>> mergeSequences(Sequence<Sequence<Result<TopNResultValue>>> seqOfSequences)
  {
    return new OrderedMergeSequence<>(getOrdering(), seqOfSequences);
  }

  @Override
  public Sequence<Result<TopNResultValue>> mergeSequencesUnordered(Sequence<Sequence<Result<TopNResultValue>>> seqOfSequences)
  {
    return new MergeSequence<>(getOrdering(), seqOfSequences);
  }

  @Override
  public ServiceMetricEvent.Builder makeMetricBuilder(TopNQuery query)
  {
    return DruidMetrics.makePartialQueryTimeMetric(query)
                       .setDimension(
                           "threshold",
                           String.valueOf(query.getThreshold())
                       )
                       .setDimension("dimension", query.getDimensionSpec().getDimension())
                       .setDimension(
                           "numMetrics",
                           String.valueOf(query.getAggregatorSpecs().size())
                       )
                       .setDimension(
                           "numComplexMetrics",
                           String.valueOf(DruidMetrics.findNumComplexAggs(query.getAggregatorSpecs()))
                       );
  }

  /**
   * Make a function which will calculate the post aggs properly
   */

  protected Function<Result<TopNResultValue>, Result<TopNResultValue>> makePostAggComputator(
      final TopNQuery query
  )
  {
    final List<PostAggregator> postAggregatorSpecs = query.getPostAggregatorSpecs();
    final PostAggregator[] postAggregators = postAggregatorSpecs.toArray(new PostAggregator[postAggregatorSpecs.size()]);
    final AggregatorFactory[] aggregatorFactories = query
        .getAggregatorSpecs()
        .toArray(new AggregatorFactory[query.getAggregatorSpecs().size()]);
    final String[] aggFactoryNames = extractFactoryName(query.getAggregatorSpecs());
    final DimensionSpec dimensionSpec = query.getDimensionSpec();
    return new Function<Result<TopNResultValue>, Result<TopNResultValue>>()
    {
      @Override
      public Result<TopNResultValue> apply(Result<TopNResultValue> result)
      {
        final Function<DimensionAndMetricValueExtractor, Map<String, Object>> resultMutator = new Function<DimensionAndMetricValueExtractor, Map<String, Object>>()
        {

          @Override
          public Map<String, Object> apply(DimensionAndMetricValueExtractor input)
          {
            final Map<String, Object> values = Maps.newHashMapWithExpectedSize(
                aggregatorFactories.length
                + postAggregators.length
                + 1
            );
            for (final String name : aggFactoryNames) {
              values.put(
                  name,
                  input.getMetric(name)
              );
            }

            for (PostAggregator postAgg : postAggregators) {
              final String postAggName = postAgg.getName();
              Object calculatedPostAgg = input.getMetric(postAggName);
              if (calculatedPostAgg != null) {
                values.put(postAggName, calculatedPostAgg);
              } else {
                values.put(postAggName, postAgg.compute(values));
              }
            }

            final Object dimValue = input.getDimensionValue(dimensionSpec.getOutputName());
            values.put(dimensionSpec.getOutputName(), dimValue);

            return values;
          }
        };
        return new Result<>(
            result.getTimestamp(),
            new TopNResultValue(
                Lists.newArrayList(
                    Iterables.transform(
                        result.getValue(),
                        resultMutator
                    )
                )
            )
        );
      }
    };
  }

  @Override
  public TypeReference<Result<TopNResultValue>> getResultTypeReference()
  {
    return TYPE_REFERENCE;
  }

  @Override
  public CacheStrategy<Result<TopNResultValue>, Object, TopNQuery> getCacheStrategy(final TopNQuery query)
  {
    return new CacheStrategy<Result<TopNResultValue>, Object, TopNQuery>()
    {
      private final List<AggregatorFactory> aggs = Lists.newArrayList(query.getAggregatorSpecs());
      private final List<PostAggregator> postAggs = AggregatorUtil.pruneDependentPostAgg(
          query.getPostAggregatorSpecs(),
          query.getTopNMetricSpec()
               .getMetricName(query.getDimensionSpec())
      );

      @Override
      public byte[] computeCacheKey(TopNQuery query)
      {
        final byte[] dimensionSpecBytes = query.getDimensionSpec().getCacheKey();
        final byte[] metricSpecBytes = query.getTopNMetricSpec().getCacheKey();

        final DimFilter dimFilter = query.getDimensionsFilter();
        final byte[] filterBytes = dimFilter == null ? new byte[]{} : dimFilter.getCacheKey();
        final byte[] aggregatorBytes = QueryCacheHelper.computeAggregatorBytes(query.getAggregatorSpecs());
        final byte[] granularityBytes = query.getGranularity().cacheKey();

        return ByteBuffer
            .allocate(
                1 + dimensionSpecBytes.length + metricSpecBytes.length + 4 +
                granularityBytes.length + filterBytes.length + aggregatorBytes.length
            )
            .put(TOPN_QUERY)
            .put(dimensionSpecBytes)
            .put(metricSpecBytes)
            .put(Ints.toByteArray(query.getThreshold()))
            .put(granularityBytes)
            .put(filterBytes)
            .put(aggregatorBytes)
            .array();
      }

      @Override
      public TypeReference<Object> getCacheObjectClazz()
      {
        return OBJECT_TYPE_REFERENCE;
      }

      @Override
      public Function<Result<TopNResultValue>, Object> prepareForCache()
      {
        return new Function<Result<TopNResultValue>, Object>()
        {
          private final String[] aggFactoryNames = extractFactoryName(query.getAggregatorSpecs());

          @Override
          public Object apply(final Result<TopNResultValue> input)
          {
            List<DimensionAndMetricValueExtractor> results = Lists.newArrayList(input.getValue());
            final List<Object> retVal = Lists.newArrayListWithCapacity(results.size() + 1);

            // make sure to preserve timezone information when caching results
            retVal.add(input.getTimestamp().getMillis());
            for (DimensionAndMetricValueExtractor result : results) {
              List<Object> vals = Lists.newArrayListWithCapacity(aggFactoryNames.length + 2);
              vals.add(result.getStringDimensionValue(query.getDimensionSpec().getOutputName()));
              for (String aggName : aggFactoryNames) {
                vals.add(result.getMetric(aggName));
              }
              retVal.add(vals);
            }
            return retVal;
          }
        };
      }

      @Override
      public Function<Object, Result<TopNResultValue>> pullFromCache()
      {
        return new Function<Object, Result<TopNResultValue>>()
        {
          private final QueryGranularity granularity = query.getGranularity();

          @Override
          public Result<TopNResultValue> apply(Object input)
          {
            List<Object> results = (List<Object>) input;
            List<Map<String, Object>> retVal = Lists.newArrayListWithCapacity(results.size());

            Iterator<Object> inputIter = results.iterator();
            DateTime timestamp = granularity.toDateTime(((Number) inputIter.next()).longValue());

            while (inputIter.hasNext()) {
              List<Object> result = (List<Object>) inputIter.next();
              Map<String, Object> vals = Maps.newLinkedHashMap();

              Iterator<AggregatorFactory> aggIter = aggs.iterator();
              Iterator<Object> resultIter = result.iterator();

              vals.put(query.getDimensionSpec().getOutputName(), resultIter.next());

              while (aggIter.hasNext() && resultIter.hasNext()) {
                final AggregatorFactory factory = aggIter.next();
                vals.put(factory.getName(), factory.deserialize(resultIter.next()));
              }

              for (PostAggregator postAgg : postAggs) {
                vals.put(postAgg.getName(), postAgg.compute(vals));
              }

              retVal.add(vals);
            }

            return new Result<>(timestamp, new TopNResultValue(retVal));
          }
        };
      }

      @Override
      public Sequence<Result<TopNResultValue>> mergeSequences(Sequence<Sequence<Result<TopNResultValue>>> seqOfSequences)
      {
        return new MergeSequence<>(getOrdering(), seqOfSequences);
      }
    };
  }

  public Ordering<Result<TopNResultValue>> getOrdering()
  {
    return Ordering.natural();
  }

  private static Function<Result<TopNResultValue>, Result<TopNResultValue>> bySegmentTransformer(
      final int threshold
  )
  {
    return new Function<Result<TopNResultValue>, Result<TopNResultValue>>()
    {
      @Nullable
      @Override
      public Result<TopNResultValue> apply(@Nullable Result<TopNResultValue> input)
      {

        BySegmentResultValue<Result<TopNResultValue>> value = (BySegmentResultValue<Result<TopNResultValue>>) input
            .getValue();

        return new Result<TopNResultValue>(
            input.getTimestamp(),
            new BySegmentTopNResultValue(
                Lists.transform(
                    value.getResults(),
                    new Function<Result<TopNResultValue>, Result<TopNResultValue>>()
                    {
                      @Override
                      public Result<TopNResultValue> apply(Result<TopNResultValue> input)
                      {
                        return new Result<>(
                            input.getTimestamp(),
                            new TopNResultValue(
                                Lists.<Object>newArrayList(
                                    Iterables.limit(
                                        input.getValue(),
                                        threshold
                                    )
                                )
                            )
                        );
                      }
                    }
                ),
                value.getSegmentId(),
                value.getInterval()
            )
        );
      }
    };
  }

  private static Function<Result<TopNResultValue>, Result<TopNResultValue>> limitResultsTransformer(
      final int threshold
  )
  {
    return new Function<Result<TopNResultValue>, Result<TopNResultValue>>()
    {
      @Override
      public Result<TopNResultValue> apply(Result<TopNResultValue> input)
      {
        return new Result<>(
            input.getTimestamp(),
            new TopNResultValue(
                Lists.<Object>newArrayList(
                    Iterables.limit(
                        input.getValue(),
                        threshold
                    )
                )
            )
        );
      }
    };
  }

  private static class ThresholdAdjustingQueryRunner implements QueryRunner<Result<TopNResultValue>>
  {
    private final QueryRunner<Result<TopNResultValue>> runner;
    private final int minTopNThreshold;

    public ThresholdAdjustingQueryRunner(
        QueryRunner<Result<TopNResultValue>> runner,
        int minTopNThreshold
    )
    {
      this.runner = runner;
      this.minTopNThreshold = minTopNThreshold;
    }

    @Override
    public Sequence<Result<TopNResultValue>> run(
        Query<Result<TopNResultValue>> input,
        Map<String, Object> responseContext
    )
    {
      if (!(input instanceof TopNQuery)) {
        throw new ISE("Can only handle [%s], got [%s]", TopNQuery.class, input.getClass());
      }

      final TopNQuery query = (TopNQuery) input;
      if (query.getThreshold() > minTopNThreshold) {
        return runner.run(query, responseContext);
      }
      return Sequences.map(
          runner.run(query.withThreshold(minTopNThreshold), responseContext),
          query.getContextBySegment(false)
          ? bySegmentTransformer(query.getThreshold())
          : limitResultsTransformer(query.getThreshold())
      );
    }
  }
}
