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

package io.druid.query.groupby;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.inject.Inject;
import com.metamx.common.IAE;
import com.metamx.common.ISE;
import com.metamx.common.Pair;
import com.metamx.common.guava.Accumulator;
import com.metamx.common.guava.MergeSequence;
import com.metamx.common.guava.ResourceClosingSequence;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.emitter.service.ServiceMetricEvent;
import io.druid.collections.OrderedMergeSequence;
import io.druid.collections.StupidPool;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.granularity.QueryGranularity;
import io.druid.guice.annotations.Global;
import io.druid.query.CacheStrategy;
import io.druid.query.DataSource;
import io.druid.query.DruidMetrics;
import io.druid.query.IntervalChunkingQueryRunnerDecorator;
import io.druid.query.Query;
import io.druid.query.QueryCacheHelper;
import io.druid.query.QueryDataSource;
import io.druid.query.QueryRunner;
import io.druid.query.QueryToolChest;
import io.druid.query.SubqueryQueryRunner;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.MetricManipulationFn;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.filter.DimFilter;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexStorageAdapter;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 */
public class GroupByQueryQueryToolChest extends QueryToolChest<Row, GroupByQuery>
{
  private static final byte GROUPBY_QUERY = 0x14;
  private static final TypeReference<Object> OBJECT_TYPE_REFERENCE =
      new TypeReference<Object>()
      {
      };
  private static final TypeReference<Row> TYPE_REFERENCE = new TypeReference<Row>()
  {
  };
  private static final String GROUP_BY_MERGE_KEY = "groupByMerge";

  private final Supplier<GroupByQueryConfig> configSupplier;

  private final StupidPool<ByteBuffer> bufferPool;
  private final ObjectMapper jsonMapper;
  private GroupByQueryEngine engine; // For running the outer query around a subquery

  private final IntervalChunkingQueryRunnerDecorator intervalChunkingQueryRunnerDecorator;

  @Inject
  public GroupByQueryQueryToolChest(
      Supplier<GroupByQueryConfig> configSupplier,
      ObjectMapper jsonMapper,
      GroupByQueryEngine engine,
      @Global StupidPool<ByteBuffer> bufferPool,
      IntervalChunkingQueryRunnerDecorator intervalChunkingQueryRunnerDecorator
  )
  {
    this.configSupplier = configSupplier;
    this.jsonMapper = jsonMapper;
    this.engine = engine;
    this.bufferPool = bufferPool;
    this.intervalChunkingQueryRunnerDecorator = intervalChunkingQueryRunnerDecorator;
  }

  @Override
  public QueryRunner<Row> mergeResults(
      final QueryRunner<Row> runner
  )
  {
    final SubqueryQueryRunner<Row> subQuery = new SubqueryQueryRunner<Row>(
        intervalChunkingQueryRunnerDecorator.decorate(
            new QueryRunner<Row>()
            {
              @Override
              public Sequence<Row> run(Query<Row> query, Map<String, Object> responseContext)
              {
                if (!(query instanceof GroupByQuery)) {
                  return runner.run(query, responseContext);
                }
                GroupByQuery groupByQuery = (GroupByQuery) query;
                ArrayList<DimensionSpec> dimensionSpecs = new ArrayList<>();
                Set<String> optimizedDimensions = extractionsToRewrite(groupByQuery, true);
                for (DimensionSpec dimensionSpec : groupByQuery.getDimensions()) {
                  if (optimizedDimensions.contains(dimensionSpec.getDimension())) {
                    dimensionSpecs.add(
                        new DefaultDimensionSpec(dimensionSpec.getDimension(), dimensionSpec.getOutputName())
                    );
                  } else {
                    dimensionSpecs.add(dimensionSpec);
                  }
                }
                return runner.run(
                    groupByQuery.withDimensionSpecs(dimensionSpecs),
                    responseContext
                );
              }
            }, this
        )
    );

    return new QueryRunner<Row>()
    {
      @Override
      public Sequence<Row> run(Query<Row> query, Map<String, Object> responseContext)
      {
        GroupByQuery groupByQuery = (GroupByQuery) query;
        final Set<String> rewritableDims = extractionsToRewrite(groupByQuery, false);
        final Map<String, ExtractionFn> extractionFnMap = new HashMap<>();

        if (!rewritableDims.isEmpty()) {
          final List<DimensionSpec> newSpecs = new ArrayList<>(groupByQuery.getDimensions().size());
          // If we have optimizations that can be done at this level, we apply them here
          for (DimensionSpec dimensionSpec : groupByQuery.getDimensions()) {
            final String dimensionOut = dimensionSpec.getOutputName();
            if (rewritableDims.contains(dimensionOut)) {
              extractionFnMap.put(dimensionOut, dimensionSpec.getExtractionFn());
              newSpecs.add(new DefaultDimensionSpec(dimensionSpec.getDimension(), dimensionOut));
            } else {
              newSpecs.add(dimensionSpec);
            }
          }
          groupByQuery = groupByQuery.withDimensionSpecs(newSpecs);
        }
        final Function<Row, Row> rowTransformer = new Function<Row, Row>()
        {
          @Nullable
          @Override
          public Row apply(@Nullable Row input)
          {
            if (input instanceof MapBasedRow) {
              final MapBasedRow preMapRow = (MapBasedRow) input;
              final Map<String, Object> event = Maps.newHashMap(preMapRow.getEvent());
              for (String dim : rewritableDims) {
                final Object eventVal = event.get(dim);
                event.put(dim, extractionFnMap.get(dim).apply(eventVal == null ? "" : eventVal));
              }
              return new MapBasedRow(preMapRow.getTimestamp(), event);
            } else {
              return input;
            }
          }
        };
        final Sequence<Row> resultSeq;
        if (groupByQuery.getContextBySegment(false)) {
          return bySegmentRepackage(subQuery.run(groupByQuery, responseContext), rowTransformer);
        } else if (Boolean.valueOf(groupByQuery.getContextValue(GROUP_BY_MERGE_KEY, "true"))) {
          resultSeq = mergeGroupByResults(
              groupByQuery,
              subQuery,
              responseContext
          );
        } else {
          resultSeq = subQuery.run(groupByQuery, responseContext);
        }
        return Sequences.map(
            resultSeq,
            rowTransformer
        );
      }
    };
  }

  @Override
  protected Row manipulateMetrics(
      GroupByQuery query, Row result, @Nullable MetricManipulationFn manipulator
  )
  {
    final MapBasedRow inputRow = (MapBasedRow) result;
    final Map<String, Object> values = Maps.newHashMap(inputRow.getEvent());
    if (manipulator != null) {
      for (AggregatorFactory agg : query.getAggregatorSpecs()) {
        values.put(agg.getName(), manipulator.manipulate(agg, inputRow.getEvent().get(agg.getName())));
      }
    }
    return new MapBasedRow(inputRow.getTimestamp(), values);
  }

  private Sequence<Row> mergeGroupByResults(
      final GroupByQuery query,
      QueryRunner<Row> runner,
      Map<String, Object> context
  )
  {
    // If there's a subquery, merge subquery results and then apply the aggregator

    final DataSource dataSource = query.getDataSource();

    if (dataSource instanceof QueryDataSource) {
      GroupByQuery subquery;
      try {
        subquery = (GroupByQuery) ((QueryDataSource) dataSource).getQuery().withOverriddenContext(query.getContext());
      }
      catch (ClassCastException e) {
        throw new UnsupportedOperationException("Subqueries must be of type 'group by'");
      }

      final Sequence<Row> subqueryResult = mergeGroupByResults(subquery, runner, context);
      final List<AggregatorFactory> aggs = Lists.newArrayList();
      for (AggregatorFactory aggregatorFactory : query.getAggregatorSpecs()) {
        aggs.addAll(aggregatorFactory.getRequiredColumns());
      }

      // We need the inner incremental index to have all the columns required by the outer query
      final GroupByQuery innerQuery = new GroupByQuery.Builder(query)
          .setAggregatorSpecs(aggs)
          .setInterval(subquery.getIntervals())
          .setPostAggregatorSpecs(Lists.<PostAggregator>newArrayList())
          .build();

      final GroupByQuery outerQuery = new GroupByQuery.Builder(query)
          .setLimitSpec(query.getLimitSpec().merge(subquery.getLimitSpec()))
          .build();
      IncrementalIndex index = makeIncrementalIndex(innerQuery, subqueryResult);

      return new ResourceClosingSequence<>(
          outerQuery.applyLimit(
              engine.process(
                  outerQuery,
                  new IncrementalIndexStorageAdapter(
                      index
                  )
              )
          ),
          index
      );
    } else {
      final IncrementalIndex index = makeIncrementalIndex(
          query, runner.run(

              new GroupByQuery(
                  query.getDataSource(),
                  query.getQuerySegmentSpec(),
                  query.getDimFilter(),
                  query.getGranularity(),
                  query.getDimensions(),
                  query.getAggregatorSpecs(),
                  // Don't do post aggs until the end of this method.
                  ImmutableList.<PostAggregator>of(),
                  // Don't do "having" clause until the end of this method.
                  null,
                  query.getLimitSpec(),
                  query.getContext()
              ).withOverriddenContext(
                  ImmutableMap.<String, Object>of(
                      "finalize", false
                  )
              )
              , context
          )
      );
      Sequence<Row> sequence = Sequences.simple(index);
      final List<PostAggregator> postAggregators = query.getPostAggregatorSpecs();
      if (postAggregators != null && !postAggregators.isEmpty()) {
        sequence = manipulateRows(
            query,
            sequence,
            makeRowFromMapRowFunction(query, IncrementalIndex.postAggregateFunction(postAggregators))
        );
      }
      return new ResourceClosingSequence<Row>(query.applyLimit(sequence), index);
    }
  }

  private IncrementalIndex makeIncrementalIndex(GroupByQuery query, Sequence<Row> rows)
  {
    final GroupByQueryConfig config = configSupplier.get();
    Pair<IncrementalIndex, Accumulator<IncrementalIndex, Row>> indexAccumulatorPair = GroupByQueryHelper.createIndexAccumulatorPair(
        query,
        config,
        bufferPool
    );

    return rows.accumulate(indexAccumulatorPair.lhs, indexAccumulatorPair.rhs);
  }

  @Override
  public Sequence<Row> mergeSequences(Sequence<Sequence<Row>> seqOfSequences)
  {
    return new OrderedMergeSequence<>(getOrdering(), seqOfSequences);
  }

  @Override
  public Sequence<Row> mergeSequencesUnordered(Sequence<Sequence<Row>> seqOfSequences)
  {
    return new MergeSequence<>(getOrdering(), seqOfSequences);
  }

  private Ordering<Row> getOrdering()
  {
    return Ordering.<Row>natural().nullsFirst();
  }

  @Override
  public ServiceMetricEvent.Builder makeMetricBuilder(GroupByQuery query)
  {
    return DruidMetrics.makePartialQueryTimeMetric(query)
                       .setDimension("numDimensions", String.valueOf(query.getDimensions().size()))
                       .setDimension("numMetrics", String.valueOf(query.getAggregatorSpecs().size()))
                       .setDimension(
                           "numComplexMetrics",
                           String.valueOf(DruidMetrics.findNumComplexAggs(query.getAggregatorSpecs()))
                       );
  }

  @Override
  public TypeReference<Row> getResultTypeReference()
  {
    return TYPE_REFERENCE;
  }

  @Override
  public CacheStrategy<Row, Object, GroupByQuery> getCacheStrategy(final GroupByQuery query)
  {
    return new CacheStrategy<Row, Object, GroupByQuery>()
    {
      private final List<AggregatorFactory> aggs = query.getAggregatorSpecs();

      @Override
      public byte[] computeCacheKey(GroupByQuery query)
      {
        final DimFilter dimFilter = query.getDimFilter();
        final byte[] filterBytes = dimFilter == null ? new byte[]{} : dimFilter.getCacheKey();
        final byte[] aggregatorBytes = QueryCacheHelper.computeAggregatorBytes(query.getAggregatorSpecs());
        final byte[] granularityBytes = query.getGranularity().cacheKey();
        final byte[][] dimensionsBytes = new byte[query.getDimensions().size()][];
        int dimensionsBytesSize = 0;
        int index = 0;
        for (DimensionSpec dimension : query.getDimensions()) {
          dimensionsBytes[index] = dimension.getCacheKey();
          dimensionsBytesSize += dimensionsBytes[index].length;
          ++index;
        }
        final byte[] havingBytes = query.getHavingSpec() == null ? new byte[]{} : query.getHavingSpec().getCacheKey();
        final byte[] limitBytes = query.getLimitSpec().getCacheKey();

        ByteBuffer buffer = ByteBuffer
            .allocate(
                1
                + granularityBytes.length
                + filterBytes.length
                + aggregatorBytes.length
                + dimensionsBytesSize
                + havingBytes.length
                + limitBytes.length
            )
            .put(GROUPBY_QUERY)
            .put(granularityBytes)
            .put(filterBytes)
            .put(aggregatorBytes);

        for (byte[] dimensionsByte : dimensionsBytes) {
          buffer.put(dimensionsByte);
        }

        return buffer
            .put(havingBytes)
            .put(limitBytes)
            .array();
      }

      @Override
      public TypeReference<Object> getCacheObjectClazz()
      {
        return OBJECT_TYPE_REFERENCE;
      }

      @Override
      public Function<Row, Object> prepareForCache()
      {
        return new Function<Row, Object>()
        {
          @Override
          public Object apply(Row input)
          {
            if (input instanceof MapBasedRow) {
              final MapBasedRow row = (MapBasedRow) input;
              final List<Object> retVal = Lists.newArrayListWithCapacity(2);
              retVal.add(row.getTimestamp().getMillis());
              retVal.add(row.getEvent());

              return retVal;
            }

            throw new ISE("Don't know how to cache input rows of type[%s]", input.getClass());
          }
        };
      }

      @Override
      public Function<Object, Row> pullFromCache()
      {
        return new Function<Object, Row>()
        {
          private final QueryGranularity granularity = query.getGranularity();

          @Override
          public Row apply(Object input)
          {
            Iterator<Object> results = ((List<Object>) input).iterator();

            DateTime timestamp = granularity.toDateTime(((Number) results.next()).longValue());

            Iterator<AggregatorFactory> aggsIter = aggs.iterator();

            Map<String, Object> event = jsonMapper.convertValue(
                results.next(),
                new TypeReference<Map<String, Object>>()
                {
                }
            );

            while (aggsIter.hasNext()) {
              final AggregatorFactory factory = aggsIter.next();
              Object agg = event.get(factory.getName());
              if (agg != null) {
                event.put(factory.getName(), factory.deserialize(agg));
              }
            }

            return new MapBasedRow(
                timestamp,
                event
            );
          }
        };
      }

      @Override
      public Sequence<Row> mergeSequences(Sequence<Sequence<Row>> seqOfSequences)
      {
        return new MergeSequence<>(getOrdering(), seqOfSequences);
      }
    };
  }


  public static Set<String> extractionsToRewrite(GroupByQuery query, final boolean getPre)
  {
    final Set<String> rewriteSet = new HashSet<>();
    for (final DimensionSpec spec : query.getDimensions()) {
      if (
          spec != null
          && spec.getExtractionFn() != null
          && ExtractionFn.ExtractionType.ONE_TO_ONE.equals(
              spec.getExtractionFn().getExtractionType()
          )
          ) {
        rewriteSet.add(getPre ? spec.getDimension() : spec.getOutputName());
      }
    }
    return rewriteSet;
  }


  private Sequence<Row> manipulateRows(GroupByQuery query, Sequence<Row> sequence, Function<Row, Row> manipulator)
  {
    if (manipulator == null) {
      return sequence;
    }
    if (query.getContextBySegment(false)) {
      return bySegmentRepackage(
          sequence,
          manipulator
      );
    } else {
      return Sequences.map(
          sequence,
          manipulator
      );
    }
  }

  private static Function<Row, Row> makeRowFromMapRowFunction(
      final GroupByQuery query,
      final Function<MapBasedRow, MapBasedRow> fn
  )
  {
    return new Function<Row, Row>()
    {
      @Nullable
      @Override
      public Row apply(@Nullable Row input)
      {
        if (input == null) {
          return null;
        }
        if (input instanceof MapBasedRow) {
          return fn.apply((MapBasedRow) input);
        } else {
          throw new IAE(
              "Expected argument of class [%s] found [%s]",
              MapBasedRow.class.getCanonicalName(),
              input.getClass().getCanonicalName()
          );
        }
      }
    };
  }

  @Override
  public Sequence<Row> finalizeSequence(final GroupByQuery query, Sequence<Row> sequence)
  {
    final List<PostAggregator> postAggregators = query.getPostAggregatorSpecs();
    final Function<MapBasedRow, MapBasedRow> postAggFn;
    if (postAggregators == null || postAggregators.isEmpty()) {
      postAggFn = null;
    } else {
      postAggFn = IncrementalIndex.postAggregateFunction(postAggregators);
    }
    return super.finalizeSequence(
        query,
        manipulateRows(
            query,
            sequence,
            makeRowFromMapRowFunction(
                query,
                new Function<MapBasedRow, MapBasedRow>()
                {
                  @Nullable
                  @Override
                  public MapBasedRow apply(@Nullable MapBasedRow input)
                  {
                    final MapBasedRow row = postAggFn == null ? input : postAggFn.apply(input);
                    if(row == null){
                      return null;
                    }
                    return new MapBasedRow(
                        query.getGranularity().toDateTime(row.getTimestampFromEpoch()),
                        row.getEvent()
                    );
                  }
                }
            )
        )
    );
  }
}
