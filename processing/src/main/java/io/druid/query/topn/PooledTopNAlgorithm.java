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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.metamx.common.Pair;
import com.metamx.common.guava.CloseQuietly;
import com.metamx.common.logger.Logger;
import io.druid.collections.ResourceHolder;
import io.druid.collections.StupidPool;
import io.druid.query.BaseQuery;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.query.aggregation.SimpleDoubleBufferAggregator;
import io.druid.segment.Capabilities;
import io.druid.segment.Cursor;
import io.druid.segment.DimensionSelector;
import io.druid.segment.FloatColumnSelector;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.data.Offset;
import io.druid.segment.historical.HistoricalCursor;
import io.druid.segment.historical.HistoricalDimensionSelector;
import io.druid.segment.historical.HistoricalFloatColumnSelector;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.commons.ClassRemapper;
import org.objectweb.asm.commons.SimpleRemapper;
import sun.misc.Unsafe;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 */
public class PooledTopNAlgorithm
    extends BaseTopNAlgorithm<int[], BufferAggregator[], PooledTopNAlgorithm.PooledTopNParams>
{
  private static final Unsafe UNSAFE;

  static {
    try {
      Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
      theUnsafe.setAccessible(true);
      UNSAFE = (Unsafe) theUnsafe.get(null);
    } catch (Exception e) {
      throw new RuntimeException("Cannot access Unsafe methods", e);
    }
  }
  private static final Logger LOG = new Logger(PooledTopNAlgorithm.class);

  private static final boolean optimizeDoublePooledTopN = !Boolean.getBoolean("dontOptimizeDoublePooledTopN");
  private static final boolean optimizeGenericPooledTopN = !Boolean.getBoolean("dontOptimizeGenericPooledTopN");
  private static final boolean optimizeGeneric2PooledTopN = !Boolean.getBoolean("dontOptimizeGeneric2PooledTopN");
  private static final boolean dontCopyClass = Boolean.getBoolean("dontCopyClass");
  private static final ConcurrentHashMap<String, AtomicReference> shapeStates =
      new ConcurrentHashMap<>();
  private static final long C2_INLINE_THRESHOLD = 10_000;
  private static final ExecutorService classSpecializationService = Executors.newFixedThreadPool(
      1,
      new ThreadFactoryBuilder().setDaemon(true).setNameFormat("class-specialization-%d").build()
  );
  static class ClassSpecializationTask<T> implements Runnable
  {
    private final AtomicReference<ShapeState<T>> shapeStateReference;
    private final Class<? extends T> prototypeClass;
    private final Class<? extends Offset> offsetClassToSpecialize;

    ClassSpecializationTask(
        AtomicReference<ShapeState<T>> shapeStateReference,
        Class<? extends T> prototypeClass,
        Class<? extends Offset> offsetClassToSpecialize
    ) {
      this.shapeStateReference = shapeStateReference;
      this.prototypeClass = prototypeClass;
      this.offsetClassToSpecialize = offsetClassToSpecialize;
    }

    @Override
    public void run()
    {
      try {
        T specializedScanner = makeNewPooledTopNScanner(prototypeClass, offsetClassToSpecialize);
        shapeStateReference.set(new ShapeState<T>(shapeStateReference.get().shape, specializedScanner));
      }
      catch (Exception e) {
        LOG.error(e, "Error specializing class for shape: %s", shapeStateReference.get().shape);
      }
    }
  }
  private static final AtomicLong specializedClassCounter = new AtomicLong();

  static class ShapeState<T>
  {
    final String shape;
    final T scanner;

    protected ShapeState(String shape, T scanner)
    {
      this.shape = shape;
      this.scanner = scanner;
    }
  }

  static class StatsShapeState<T> extends ShapeState<T>
  {
    final ConcurrentMap<Long, AtomicLong> stats = new ConcurrentHashMap<>();

    protected StatsShapeState(String shape, T scanner)
    {
      super(shape, scanner);
    }

    long totalScannerRows(long scannedRows)
    {
      long currentMillis = System.currentTimeMillis();
      long currentMinute = currentMillis / TimeUnit.MINUTES.toMillis(1);
      long minuteOneHourAgo = currentMinute - TimeUnit.HOURS.toMinutes(1);
      long totalScannedRows = 0;
      boolean currentMinutePresent = false;
      for (Iterator<Map.Entry<Long, AtomicLong>> it = stats.entrySet().iterator(); it.hasNext(); ) {
        Map.Entry<Long, AtomicLong> minuteStats = it.next();
        long minute = minuteStats.getKey();
        if (minute < minuteOneHourAgo) {
          it.remove();
        } else if (minute == currentMinute) {
          totalScannedRows += minuteStats.getValue().addAndGet(scannedRows);
          currentMinutePresent = true;
        } else {
          totalScannedRows += minuteStats.getValue().get();
        }
      }
      if (!currentMinutePresent) {
        AtomicLong existingValue = stats.putIfAbsent(currentMinute, new AtomicLong(scannedRows));
        if (existingValue != null) {
          existingValue.addAndGet(scannedRows);
        }
        totalScannedRows += scannedRows;
      }
      return totalScannedRows;
    }
  }

  private static <T> AtomicReference<ShapeState<T>> getPooledTopShapeState(
      String runtimeShape,
      Class<? extends T> prototypeClass
  )
  {
    AtomicReference<ShapeState<T>> shapeState = (AtomicReference<ShapeState<T>>) shapeStates.get(runtimeShape);
    if (shapeState != null) {
      return shapeState;
    }
    try {
      shapeState = new AtomicReference<ShapeState<T>>(new StatsShapeState<>(runtimeShape, prototypeClass.newInstance()));
      AtomicReference existingShapeState = shapeStates.putIfAbsent(runtimeShape, shapeState);
      return existingShapeState != null ? existingShapeState : shapeState;
    }
    catch (InstantiationException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  private static <T> void updateShapeState(
      AtomicReference<ShapeState<T>> shapeStateReference,
      Class<? extends T> prototypeClass,
      HistoricalCursor cursor,
      long scannedRows
  )
  {
    ShapeState<T> shapeState = shapeStateReference.get();
    if (!(shapeState instanceof StatsShapeState)) {
      return;
    }
    if (scannedRows > C2_INLINE_THRESHOLD ||
        ((StatsShapeState) shapeState).totalScannerRows(scannedRows) > C2_INLINE_THRESHOLD) {
      if (shapeStateReference.compareAndSet(shapeState, new ShapeState<>(shapeState.shape, shapeState.scanner))) {
        Class<? extends Offset> offsetClassToSpecialize;
        if (cursor != null) {
          offsetClassToSpecialize = ((Offset) cursor.copyOffset()).getClass();
        } else {
          offsetClassToSpecialize = null;
        }
        ClassSpecializationTask<T> classSpecializationTask = new ClassSpecializationTask<>(
            shapeStateReference,
            prototypeClass,
            offsetClassToSpecialize
        );
        classSpecializationService.submit(classSpecializationTask);
      }
    }
  }

  private static <T> T makeNewPooledTopNScanner(
      Class<? extends T> prototypeClass,
      Class<? extends Offset> specializingOffsetClass
  )
  {
    if (dontCopyClass) {
      try {
        return prototypeClass.newInstance();
      }
      catch (InstantiationException | IllegalAccessException e) {
        throw new RuntimeException(e);
      }
    }
    ClassLoader cl = prototypeClass.getClassLoader();
    String prototypeClassName = prototypeClass.getName();
    String prototypeClassBytecodeName = prototypeClassName.replace('.', '/');
    String specializedClassName = prototypeClassName + "$Copy" + specializedClassCounter.getAndIncrement();
    ClassWriter specializedClassWriter = new ClassWriter(0);
    HashMap<String, String> remapping = new HashMap<>();
    remapping.put(prototypeClassBytecodeName, specializedClassName.replace('.', '/'));
    if (specializingOffsetClass != null) {
      remapping.put(Offset.class.getName().replace('.', '/'), specializingOffsetClass.getName().replace('.', '/'));
    }
    ClassVisitor classTransformer = new ClassRemapper(specializedClassWriter, new SimpleRemapper(remapping));
    String prototypeClassResource = prototypeClassBytecodeName+ ".class";
    try (InputStream prototypeClassBytecodeStream = cl.getResourceAsStream(prototypeClassResource)) {
      ClassReader prototypeClassReader = new ClassReader(prototypeClassBytecodeStream);
      prototypeClassReader.accept(classTransformer, 0);
      byte[] specializedClassBytecode = specializedClassWriter.toByteArray();
      Class specializedClass = UNSAFE.defineClass(
          specializedClassName,
          specializedClassBytecode,
          0,
          specializedClassBytecode.length,
          cl,
          prototypeClass.getProtectionDomain()
      );
      return (T) specializedClass.newInstance();
    }
    catch (InstantiationException | IllegalAccessException | IOException e) {
      throw new RuntimeException(e);
    }
  }

  private final Capabilities capabilities;
  private final TopNQuery query;
  private final StupidPool<ByteBuffer> bufferPool;
  private static final int AGG_UNROLL_COUNT = 8; // Must be able to fit loop below

  public PooledTopNAlgorithm(
      Capabilities capabilities,
      TopNQuery query,
      StupidPool<ByteBuffer> bufferPool
  )
  {
    super(capabilities);

    this.capabilities = capabilities;
    this.query = query;
    this.bufferPool = bufferPool;
  }

  @Override
  public PooledTopNParams makeInitParams(
      DimensionSelector dimSelector, Cursor cursor
  )
  {
    ResourceHolder<ByteBuffer> resultsBufHolder = bufferPool.take();
    ByteBuffer resultsBuf = resultsBufHolder.get();
    resultsBuf.clear();

    final int cardinality = dimSelector.getValueCardinality();

    if (cardinality < 0) {
      throw new UnsupportedOperationException("Cannot operate on a dimension with no dictionary");
    }

    final TopNMetricSpecBuilder<int[]> arrayProvider = new BaseArrayProvider<int[]>(
        dimSelector,
        query,
        capabilities
    )
    {
      private final int[] positions = new int[cardinality];

      @Override
      public int[] build()
      {
        Pair<Integer, Integer> startEnd = computeStartEnd(cardinality);

        Arrays.fill(positions, 0, startEnd.lhs, SKIP_POSITION_VALUE);
        Arrays.fill(positions, startEnd.lhs, startEnd.rhs, INIT_POSITION_VALUE);
        Arrays.fill(positions, startEnd.rhs, positions.length, SKIP_POSITION_VALUE);

        return positions;
      }
    };

    final int numBytesToWorkWith = resultsBuf.remaining();
    final int[] aggregatorSizes = new int[query.getAggregatorSpecs().size()];
    int numBytesPerRecord = 0;

    for (int i = 0; i < query.getAggregatorSpecs().size(); ++i) {
      aggregatorSizes[i] = query.getAggregatorSpecs().get(i).getMaxIntermediateSize();
      numBytesPerRecord += aggregatorSizes[i];
    }

    final int numValuesPerPass = numBytesPerRecord > 0 ? numBytesToWorkWith / numBytesPerRecord : cardinality;

    return PooledTopNParams.builder()
                           .withDimSelector(dimSelector)
                           .withCursor(cursor)
                           .withResultsBufHolder(resultsBufHolder)
                           .withResultsBuf(resultsBuf)
                           .withArrayProvider(arrayProvider)
                           .withNumBytesPerRecord(numBytesPerRecord)
                           .withNumValuesPerPass(numValuesPerPass)
                           .withAggregatorSizes(aggregatorSizes)
                           .build();
  }


  @Override
  protected int[] makeDimValSelector(PooledTopNParams params, int numProcessed, int numToProcess)
  {
    final TopNMetricSpecBuilder<int[]> arrayProvider = params.getArrayProvider();

    if (!query.getDimensionSpec().preservesOrdering()) {
      return arrayProvider.build();
    }

    arrayProvider.ignoreFirstN(numProcessed);
    arrayProvider.keepOnlyN(numToProcess);
    return query.getTopNMetricSpec().configureOptimizer(arrayProvider).build();
  }

  @Override
  protected int computeNewLength(int[] dimValSelector, int numProcessed, int numToProcess)
  {
    int valid = 0;
    int length = 0;
    for (int i = numProcessed; i < dimValSelector.length && valid < numToProcess; i++) {
      length++;
      if (SKIP_POSITION_VALUE != dimValSelector[i]) {
        valid++;
      }
    }
    return length;
  }

  @Override
  protected int[] updateDimValSelector(int[] dimValSelector, int numProcessed, int numToProcess)
  {
    final int[] retVal = Arrays.copyOf(dimValSelector, dimValSelector.length);

    final int validEnd = Math.min(retVal.length, numProcessed + numToProcess);
    final int end = Math.max(retVal.length, validEnd);

    Arrays.fill(retVal, 0, numProcessed, SKIP_POSITION_VALUE);
    Arrays.fill(retVal, validEnd, end, SKIP_POSITION_VALUE);

    return retVal;
  }

  @Override
  protected BufferAggregator[] makeDimValAggregateStore(PooledTopNParams params)
  {
    return makeBufferAggregators(params.getCursor(), query.getAggregatorSpecs());
  }
  /**
   * Use aggressive loop unrolling to aggregate the data
   *
   * How this works: The aggregates are evaluated AGG_UNROLL_COUNT at a time. This was chosen to be 8 rather arbitrarily.
   * The offsets into the output buffer are precalculated and stored in aggregatorOffsets
   *
   * For queries whose aggregate count is less than AGG_UNROLL_COUNT, the aggregates evaluted in a switch statement.
   * See http://en.wikipedia.org/wiki/Duff's_device for more information on this kind of approach
   *
   * This allows out of order execution of the code. In local tests, the JVM inlines all the way to this function.
   *
   * If there are more than AGG_UNROLL_COUNT aggregates, then the remainder is calculated with the switch, and the
   * blocks of AGG_UNROLL_COUNT are calculated in a partially unrolled for-loop.
   *
   * Putting the switch first allows for optimization for the common case (less than AGG_UNROLL_COUNT aggs) but
   * still optimizes the high quantity of aggregate queries which benefit greatly from any speed improvements
   * (they simply take longer to start with).
   */
  @Override
  protected long scanAndAggregate(
      final PooledTopNParams params,
      final int[] positions,
      final BufferAggregator[] theAggregators,
      final int numProcessed
  )
  {
    final Cursor cursor = params.getCursor();
    if (optimizeDoublePooledTopN && cursor instanceof HistoricalCursor && theAggregators.length == 1) {
      BufferAggregator aggregator = theAggregators[0];
      if (aggregator instanceof SimpleDoubleBufferAggregator) {
        FloatColumnSelector metricSelector = ((SimpleDoubleBufferAggregator) aggregator).getSelector();
        String runtimeShape = aggregator.getBufferAggregatorType();
        Class<? extends DoublePooledTopNScanner> prototypeClass = DoublePooledTopNScannerPrototype.class;
        AtomicReference<ShapeState<DoublePooledTopNScanner>> shapeStateReference = getPooledTopShapeState(
            runtimeShape,
            prototypeClass
        );
        HistoricalCursor historicalCursor = (HistoricalCursor) cursor;
        long scannedRows = shapeStateReference.get().scanner.scanAndAggregateHistorical(
            (HistoricalDimensionSelector) params.getDimSelector(),
            (HistoricalFloatColumnSelector) metricSelector,
            (SimpleDoubleBufferAggregator) aggregator,
            historicalCursor,
            positions,
            params.getResultsBuf()
        );
        updateShapeState(shapeStateReference, prototypeClass, historicalCursor, scannedRows);
        BaseQuery.checkInterrupted();
        return scannedRows;
      }
    }
    if (optimizeGenericPooledTopN && theAggregators.length == 1) {
      BufferAggregator aggregator = theAggregators[0];
      String runtimeShape = aggregator.getBufferAggregatorType();
      Class<? extends GenericPooledTopNScanner> prototypeClass = GenericPooledTopNScannerPrototype.class;
      AtomicReference<ShapeState<GenericPooledTopNScanner>> shapeStateReference = getPooledTopShapeState(
          runtimeShape,
          prototypeClass
      );
      long scannedRows = shapeStateReference.get().scanner.scanAndAggregate(
          params.getDimSelector(),
          aggregator,
          params.getAggregatorSizes()[0],
          cursor,
          positions,
          params.getResultsBuf()
      );
      updateShapeState(shapeStateReference, prototypeClass, null, scannedRows);
      BaseQuery.checkInterrupted();
      return scannedRows;
    }
    if (optimizeGeneric2PooledTopN && theAggregators.length == 2) {
      BufferAggregator aggregator1 = theAggregators[0];
      BufferAggregator aggregator2 = theAggregators[1];
      String runtimeShape = aggregator1.getBufferAggregatorType() + "," + aggregator2.getBufferAggregatorType();
      Class<? extends Generic2PooledTopNScanner> prototypeClass = Generic2PooledTopNScannerPrototype.class;
      AtomicReference<ShapeState<Generic2PooledTopNScanner>> shapeStateReference = getPooledTopShapeState(
          runtimeShape,
          prototypeClass
      );
      int[] aggregatorSizes = params.getAggregatorSizes();
      long scannedRows = shapeStateReference.get().scanner.scanAndAggregate(
          params.getDimSelector(),
          aggregator1,
          aggregatorSizes[0],
          aggregator2,
          aggregatorSizes[1],
          cursor,
          positions,
          params.getResultsBuf()
      );
      updateShapeState(shapeStateReference, prototypeClass, null, scannedRows);
      BaseQuery.checkInterrupted();
      return scannedRows;
    }
    return genericScanAndAggregate(params, positions, theAggregators, numProcessed);
  }

  private static long genericScanAndAggregate(
      PooledTopNParams params,
      int[] positions,
      BufferAggregator[] theAggregators,
      int numProcessed
  )
  {
    final ByteBuffer resultsBuf = params.getResultsBuf();
    final int numBytesPerRecord = params.getNumBytesPerRecord();
    final int[] aggregatorSizes = params.getAggregatorSizes();
    final Cursor cursor = params.getCursor();
    final DimensionSelector dimSelector = params.getDimSelector();

    final int[] aggregatorOffsets = new int[aggregatorSizes.length];
    for (int j = 0, offset = 0; j < aggregatorSizes.length; ++j) {
      aggregatorOffsets[j] = offset;
      offset += aggregatorSizes[j];
    }

    final int aggSize = theAggregators.length;
    final int aggExtra = aggSize % AGG_UNROLL_COUNT;
    final AtomicInteger currentPosition = new AtomicInteger(0);

    long scannedRows = 0;
    while (!cursor.isDone()) {
      final IndexedInts dimValues = dimSelector.getRow();

      final int dimSize = dimValues.size();
      final int dimExtra = dimSize % AGG_UNROLL_COUNT;
      switch(dimExtra){
        case 7:
          aggregateDimValue(
              positions,
              theAggregators,
              numProcessed,
              resultsBuf,
              numBytesPerRecord,
              aggregatorOffsets,
              aggSize,
              aggExtra,
              dimValues.get(6),
              currentPosition
          );
        case 6:
          aggregateDimValue(
              positions,
              theAggregators,
              numProcessed,
              resultsBuf,
              numBytesPerRecord,
              aggregatorOffsets,
              aggSize,
              aggExtra,
              dimValues.get(5),
              currentPosition
          );
        case 5:
          aggregateDimValue(
              positions,
              theAggregators,
              numProcessed,
              resultsBuf,
              numBytesPerRecord,
              aggregatorOffsets,
              aggSize,
              aggExtra,
              dimValues.get(4),
              currentPosition
          );
        case 4:
          aggregateDimValue(
              positions,
              theAggregators,
              numProcessed,
              resultsBuf,
              numBytesPerRecord,
              aggregatorOffsets,
              aggSize,
              aggExtra,
              dimValues.get(3),
              currentPosition
          );
        case 3:
          aggregateDimValue(
              positions,
              theAggregators,
              numProcessed,
              resultsBuf,
              numBytesPerRecord,
              aggregatorOffsets,
              aggSize,
              aggExtra,
              dimValues.get(2),
              currentPosition
          );
        case 2:
          aggregateDimValue(
              positions,
              theAggregators,
              numProcessed,
              resultsBuf,
              numBytesPerRecord,
              aggregatorOffsets,
              aggSize,
              aggExtra,
              dimValues.get(1),
              currentPosition
          );
        case 1:
          aggregateDimValue(
              positions,
              theAggregators,
              numProcessed,
              resultsBuf,
              numBytesPerRecord,
              aggregatorOffsets,
              aggSize,
              aggExtra,
              dimValues.get(0),
              currentPosition
          );
      }
      for (int i = dimExtra; i < dimSize; i += AGG_UNROLL_COUNT) {
        aggregateDimValue(
            positions,
            theAggregators,
            numProcessed,
            resultsBuf,
            numBytesPerRecord,
            aggregatorOffsets,
            aggSize,
            aggExtra,
            dimValues.get(i),
            currentPosition
        );
        aggregateDimValue(
            positions,
            theAggregators,
            numProcessed,
            resultsBuf,
            numBytesPerRecord,
            aggregatorOffsets,
            aggSize,
            aggExtra,
            dimValues.get(i + 1),
            currentPosition
        );
        aggregateDimValue(
            positions,
            theAggregators,
            numProcessed,
            resultsBuf,
            numBytesPerRecord,
            aggregatorOffsets,
            aggSize,
            aggExtra,
            dimValues.get(i + 2),
            currentPosition
        );
        aggregateDimValue(
            positions,
            theAggregators,
            numProcessed,
            resultsBuf,
            numBytesPerRecord,
            aggregatorOffsets,
            aggSize,
            aggExtra,
            dimValues.get(i + 3),
            currentPosition
        );
        aggregateDimValue(
            positions,
            theAggregators,
            numProcessed,
            resultsBuf,
            numBytesPerRecord,
            aggregatorOffsets,
            aggSize,
            aggExtra,
            dimValues.get(i + 4),
            currentPosition
        );
        aggregateDimValue(
            positions,
            theAggregators,
            numProcessed,
            resultsBuf,
            numBytesPerRecord,
            aggregatorOffsets,
            aggSize,
            aggExtra,
            dimValues.get(i + 5),
            currentPosition
        );
        aggregateDimValue(
            positions,
            theAggregators,
            numProcessed,
            resultsBuf,
            numBytesPerRecord,
            aggregatorOffsets,
            aggSize,
            aggExtra,
            dimValues.get(i + 6),
            currentPosition
        );
        aggregateDimValue(
            positions,
            theAggregators,
            numProcessed,
            resultsBuf,
            numBytesPerRecord,
            aggregatorOffsets,
            aggSize,
            aggExtra,
            dimValues.get(i + 7),
            currentPosition
        );
      }
      scannedRows++;
      cursor.advance();
    }
    return scannedRows;
  }

  private static void aggregateDimValue(
      final int[] positions,
      final BufferAggregator[] theAggregators,
      final int numProcessed,
      final ByteBuffer resultsBuf,
      final int numBytesPerRecord,
      final int[] aggregatorOffsets,
      final int aggSize,
      final int aggExtra,
      final int dimIndex,
      final AtomicInteger currentPosition
  )
  {
    if (SKIP_POSITION_VALUE == positions[dimIndex]) {
      return;
    }
    if (INIT_POSITION_VALUE == positions[dimIndex]) {
      positions[dimIndex] = currentPosition.getAndIncrement() * numBytesPerRecord;
      final int pos = positions[dimIndex];
      for (int j = 0; j < aggSize; ++j) {
        theAggregators[j].init(resultsBuf, pos + aggregatorOffsets[j]);
      }
    }
    final int position = positions[dimIndex];

    switch(aggExtra) {
      case 7:
        theAggregators[6].aggregate(resultsBuf, position + aggregatorOffsets[6]);
      case 6:
        theAggregators[5].aggregate(resultsBuf, position + aggregatorOffsets[5]);
      case 5:
        theAggregators[4].aggregate(resultsBuf, position + aggregatorOffsets[4]);
      case 4:
        theAggregators[3].aggregate(resultsBuf, position + aggregatorOffsets[3]);
      case 3:
        theAggregators[2].aggregate(resultsBuf, position + aggregatorOffsets[2]);
      case 2:
        theAggregators[1].aggregate(resultsBuf, position + aggregatorOffsets[1]);
      case 1:
        theAggregators[0].aggregate(resultsBuf, position + aggregatorOffsets[0]);
    }
    for (int j = aggExtra; j < aggSize; j += AGG_UNROLL_COUNT) {
      theAggregators[j].aggregate(resultsBuf, position + aggregatorOffsets[j]);
      theAggregators[j+1].aggregate(resultsBuf, position + aggregatorOffsets[j+1]);
      theAggregators[j+2].aggregate(resultsBuf, position + aggregatorOffsets[j+2]);
      theAggregators[j+3].aggregate(resultsBuf, position + aggregatorOffsets[j+3]);
      theAggregators[j+4].aggregate(resultsBuf, position + aggregatorOffsets[j+4]);
      theAggregators[j+5].aggregate(resultsBuf, position + aggregatorOffsets[j+5]);
      theAggregators[j+6].aggregate(resultsBuf, position + aggregatorOffsets[j+6]);
      theAggregators[j+7].aggregate(resultsBuf, position + aggregatorOffsets[j+7]);
    }
  }

  @Override
  protected void updateResults(
      PooledTopNParams params,
      int[] positions,
      BufferAggregator[] theAggregators,
      TopNResultBuilder resultBuilder
  )
  {
    final ByteBuffer resultsBuf = params.getResultsBuf();
    final int[] aggregatorSizes = params.getAggregatorSizes();
    final DimensionSelector dimSelector = params.getDimSelector();

    for (int i = 0; i < positions.length; i++) {
      int position = positions[i];
      if (position >= 0) {
        Object[] vals = new Object[theAggregators.length];
        for (int j = 0; j < theAggregators.length; j++) {
          vals[j] = theAggregators[j].get(resultsBuf, position);
          position += aggregatorSizes[j];
        }

        resultBuilder.addEntry(
            dimSelector.lookupName(i),
            i,
            vals
        );
      }
    }
  }

  @Override
  protected void closeAggregators(BufferAggregator[] bufferAggregators)
  {
    for (BufferAggregator agg : bufferAggregators) {
      agg.close();
    }
  }

  @Override
  public void cleanup(PooledTopNParams params)
  {
    if (params != null) {
      ResourceHolder<ByteBuffer> resultsBufHolder = params.getResultsBufHolder();

      if (resultsBufHolder != null) {
        resultsBufHolder.get().clear();
      }
      CloseQuietly.close(resultsBufHolder);
    }
  }

  public static class PooledTopNParams extends TopNParams
  {
    private final ResourceHolder<ByteBuffer> resultsBufHolder;
    private final ByteBuffer resultsBuf;
    private final int[] aggregatorSizes;
    private final int numBytesPerRecord;
    private final TopNMetricSpecBuilder<int[]> arrayProvider;

    public PooledTopNParams(
        DimensionSelector dimSelector,
        Cursor cursor,
        ResourceHolder<ByteBuffer> resultsBufHolder,
        ByteBuffer resultsBuf,
        int[] aggregatorSizes,
        int numBytesPerRecord,
        int numValuesPerPass,
        TopNMetricSpecBuilder<int[]> arrayProvider
    )
    {
      super(dimSelector, cursor, numValuesPerPass);

      this.resultsBufHolder = resultsBufHolder;
      this.resultsBuf = resultsBuf;
      this.aggregatorSizes = aggregatorSizes;
      this.numBytesPerRecord = numBytesPerRecord;
      this.arrayProvider = arrayProvider;
    }

    public static Builder builder()
    {
      return new Builder();
    }

    public ResourceHolder<ByteBuffer> getResultsBufHolder()
    {
      return resultsBufHolder;
    }

    public ByteBuffer getResultsBuf()
    {
      return resultsBuf;
    }

    public int[] getAggregatorSizes()
    {
      return aggregatorSizes;
    }

    public int getNumBytesPerRecord()
    {
      return numBytesPerRecord;
    }

    public TopNMetricSpecBuilder<int[]> getArrayProvider()
    {
      return arrayProvider;
    }

    public static class Builder
    {
      private DimensionSelector dimSelector;
      private Cursor cursor;
      private ResourceHolder<ByteBuffer> resultsBufHolder;
      private ByteBuffer resultsBuf;
      private int[] aggregatorSizes;
      private int numBytesPerRecord;
      private int numValuesPerPass;
      private TopNMetricSpecBuilder<int[]> arrayProvider;

      public Builder()
      {
        dimSelector = null;
        cursor = null;
        resultsBufHolder = null;
        resultsBuf = null;
        aggregatorSizes = null;
        numBytesPerRecord = 0;
        numValuesPerPass = 0;
        arrayProvider = null;
      }

      public Builder withDimSelector(DimensionSelector dimSelector)
      {
        this.dimSelector = dimSelector;
        return this;
      }

      public Builder withCursor(Cursor cursor)
      {
        this.cursor = cursor;
        return this;
      }

      public Builder withResultsBufHolder(ResourceHolder<ByteBuffer> resultsBufHolder)
      {
        this.resultsBufHolder = resultsBufHolder;
        return this;
      }

      public Builder withResultsBuf(ByteBuffer resultsBuf)
      {
        this.resultsBuf = resultsBuf;
        return this;
      }

      public Builder withAggregatorSizes(int[] aggregatorSizes)
      {
        this.aggregatorSizes = aggregatorSizes;
        return this;
      }

      public Builder withNumBytesPerRecord(int numBytesPerRecord)
      {
        this.numBytesPerRecord = numBytesPerRecord;
        return this;
      }

      public Builder withNumValuesPerPass(int numValuesPerPass)
      {
        this.numValuesPerPass = numValuesPerPass;
        return this;
      }

      public Builder withArrayProvider(TopNMetricSpecBuilder<int[]> arrayProvider)
      {
        this.arrayProvider = arrayProvider;
        return this;
      }

      public PooledTopNParams build()
      {
        return new PooledTopNParams(
            dimSelector,
            cursor,
            resultsBufHolder,
            resultsBuf,
            aggregatorSizes,
            numBytesPerRecord,
            numValuesPerPass,
            arrayProvider
        );
      }
    }
  }
}
