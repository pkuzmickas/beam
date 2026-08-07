/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.runners.flink.translation.wrappers.streaming.io.source;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.translation.utils.SerdeUtils;
import org.apache.beam.runners.flink.translation.wrappers.streaming.io.source.bounded.FlinkBoundedSource;
import org.apache.beam.runners.flink.translation.wrappers.streaming.io.source.impulse.BeamImpulseSource;
import org.apache.beam.runners.flink.translation.wrappers.streaming.io.source.unbounded.FlinkUnboundedSource;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The base class for {@link FlinkBoundedSource} and {@link FlinkUnboundedSource}.
 *
 * @param <T> The data type of the records emitted by the raw Beam sources.
 * @param <OutputT> The data type of the records emitted by the Flink Source.
 */
public abstract class FlinkSource<T, OutputT>
    implements Source<OutputT, FlinkSourceSplit<T>, Map<Integer, List<FlinkSourceSplit<T>>>> {

  private static final Logger LOG = LoggerFactory.getLogger(FlinkSource.class);

  protected final String stepName;
  protected final org.apache.beam.sdk.io.Source<T> beamSource;
  protected final Boundedness boundedness;
  protected final SerializablePipelineOptions serializablePipelineOptions;

  private final int numSplits;
  private final FlinkSourceSplitAssignmentPreference splitAssignmentPreference;

  // ----------------- public static methods to construct sources --------------------

  public static <T> FlinkBoundedSource<T> bounded(
      String stepName,
      BoundedSource<T> boundedSource,
      SerializablePipelineOptions serializablePipelineOptions,
      int numSplits) {
    return bounded(
        stepName,
        boundedSource,
        serializablePipelineOptions,
        numSplits,
        FlinkSourceSplitAssignmentPreference.LAZY);
  }

  public static <T> FlinkBoundedSource<T> bounded(
      String stepName,
      BoundedSource<T> boundedSource,
      SerializablePipelineOptions serializablePipelineOptions,
      int numSplits,
      FlinkSourceSplitAssignmentPreference splitAssignmentPreference) {
    return new FlinkBoundedSource<>(
        stepName,
        boundedSource,
        serializablePipelineOptions,
        Boundedness.BOUNDED,
        numSplits,
        splitAssignmentPreference);
  }

  public static <T> FlinkUnboundedSource<T> unbounded(
      String stepName,
      UnboundedSource<T, ?> source,
      SerializablePipelineOptions serializablePipelineOptions,
      int numSplits) {
    return new FlinkUnboundedSource<>(stepName, source, serializablePipelineOptions, numSplits);
  }

  public static FlinkBoundedSource<byte[]> boundedImpulse() {
    return new FlinkBoundedSource<>(
        "Impulse",
        new BeamImpulseSource(),
        new SerializablePipelineOptions(FlinkPipelineOptions.defaults()),
        Boundedness.BOUNDED,
        1,
        record -> Watermark.MAX_WATERMARK.getTimestamp());
  }

  // ------ Common implementations for both bounded and unbounded source ---------

  protected FlinkSource(
      String stepName,
      org.apache.beam.sdk.io.Source<T> beamSource,
      SerializablePipelineOptions serializablePipelineOptions,
      Boundedness boundedness,
      int numSplits,
      FlinkSourceSplitAssignmentPreference splitAssignmentPreference) {
    this.stepName = stepName;
    this.beamSource = beamSource;
    this.serializablePipelineOptions = serializablePipelineOptions;
    this.boundedness = boundedness;
    this.numSplits = numSplits;
    this.splitAssignmentPreference =
        Objects.requireNonNull(
            splitAssignmentPreference,
            "A Flink source split-assignment preference must not be null");
  }

  @Override
  public Boundedness getBoundedness() {
    return boundedness;
  }

  @Override
  public SplitEnumerator<FlinkSourceSplit<T>, Map<Integer, List<FlinkSourceSplit<T>>>>
      createEnumerator(SplitEnumeratorContext<FlinkSourceSplit<T>> enumContext) throws Exception {
    return createEnumerator(enumContext, false);
  }

  public SplitEnumerator<FlinkSourceSplit<T>, Map<Integer, List<FlinkSourceSplit<T>>>>
      createEnumerator(
          SplitEnumeratorContext<FlinkSourceSplit<T>> enumContext, boolean splitInitialized)
          throws Exception {

    if (boundedness == Boundedness.BOUNDED) {
      // The normal bounded-source enumerator is deliberately pull-based: a reader receives its
      // next split only after it finishes the current one. That is useful work stealing when the
      // source split itself represents the expensive operation.
      //
      // Static assignment instead gives every reader a round-robin share before work begins. That
      // can help sources whose split ordering or downstream topology makes pull-based allocation
      // imbalanced.
      //
      // The runner defaults bounded sources to lazy assignment. A Flink configuration property can
      // opt specific source classes into static assignment when their splits must be distributed
      // before readers start emitting records.
      PipelineOptions pipelineOptions = serializablePipelineOptions.get();
      boolean useStaticAssignment =
          splitAssignmentPreference == FlinkSourceSplitAssignmentPreference.STATIC;
      LOG.info(
          "Using {} split assignment for bounded source {}",
          useStaticAssignment ? "static" : "lazy",
          beamSource);
      if (useStaticAssignment) {
        checkPositiveParallelism(enumContext.currentParallelism());
        return new FlinkSourceSplitEnumerator<>(
            enumContext, beamSource, pipelineOptions, numSplits, splitInitialized);
      }

      return new LazyFlinkSourceSplitEnumerator<>(
          enumContext, beamSource, pipelineOptions, numSplits, splitInitialized);
    } else {
      return new FlinkSourceSplitEnumerator<>(
          enumContext, beamSource, serializablePipelineOptions.get(), numSplits, splitInitialized);
    }
  }

  @Override
  public SplitEnumerator<FlinkSourceSplit<T>, Map<Integer, List<FlinkSourceSplit<T>>>>
      restoreEnumerator(
          SplitEnumeratorContext<FlinkSourceSplit<T>> enumContext,
          Map<Integer, List<FlinkSourceSplit<T>>> checkpoint)
          throws Exception {
    SplitEnumerator<FlinkSourceSplit<T>, Map<Integer, List<FlinkSourceSplit<T>>>> enumerator =
        createEnumerator(enumContext, true);
    if (boundedness == Boundedness.BOUNDED) {
      restoreBoundedSplits(enumerator, enumContext.currentParallelism(), checkpoint);
    } else {
      checkpoint.forEach(
          (subtaskId, splitsForSubtask) -> enumerator.addSplitsBack(splitsForSubtask, subtaskId));
    }
    return enumerator;
  }

  @Override
  public SimpleVersionedSerializer<FlinkSourceSplit<T>> getSplitSerializer() {
    return FlinkSourceSplit.serializer();
  }

  @Override
  public SimpleVersionedSerializer<Map<Integer, List<FlinkSourceSplit<T>>>>
      getEnumeratorCheckpointSerializer() {
    return SerdeUtils.getNaiveObjectSerializer();
  }

  public int getNumSplits() {
    return numSplits;
  }

  private void checkPositiveParallelism(int sourceParallelism) {
    if (sourceParallelism <= 0) {
      throw new IllegalArgumentException(
          "Bounded source parallelism must be positive, but was " + sourceParallelism);
    }
  }

  private void restoreBoundedSplits(
      SplitEnumerator<FlinkSourceSplit<T>, Map<Integer, List<FlinkSourceSplit<T>>>> enumerator,
      int sourceParallelism,
      Map<Integer, List<FlinkSourceSplit<T>>> checkpoint) {
    List<FlinkSourceSplit<T>> restoredSplits = new ArrayList<>();
    checkpoint.values().forEach(restoredSplits::addAll);

    if (enumerator instanceof FlinkSourceSplitEnumerator) {
      checkPositiveParallelism(sourceParallelism);
      Map<Integer, List<FlinkSourceSplit<T>>> splitsBySubtask = new HashMap<>();
      for (FlinkSourceSplit<T> split : restoredSplits) {
        int targetSubtask = Math.floorMod(split.splitIndex(), sourceParallelism);
        splitsBySubtask.computeIfAbsent(targetSubtask, ignored -> new ArrayList<>()).add(split);
      }
      splitsBySubtask.forEach(
          (subtaskId, splitsForSubtask) -> enumerator.addSplitsBack(splitsForSubtask, subtaskId));
    } else {
      // Lazy assignment uses one shared pending queue, so checkpoint subtask keys are irrelevant.
      enumerator.addSplitsBack(restoredSplits, 0);
    }
  }

  @FunctionalInterface
  public interface TimestampExtractor<T> extends Function<T, Long>, Serializable {}
}
