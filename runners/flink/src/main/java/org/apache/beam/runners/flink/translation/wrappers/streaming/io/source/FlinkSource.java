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
import java.util.List;
import java.util.Map;
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

  static final long STATIC_ASSIGNMENT_MIN_BYTES_PER_READER = 128L * 1024L * 1024L;
  private static final Logger LOG = LoggerFactory.getLogger(FlinkSource.class);

  protected final String stepName;
  protected final org.apache.beam.sdk.io.Source<T> beamSource;
  protected final Boundedness boundedness;
  protected final SerializablePipelineOptions serializablePipelineOptions;

  private final int numSplits;

  // ----------------- public static methods to construct sources --------------------

  public static <T> FlinkBoundedSource<T> bounded(
      String stepName,
      BoundedSource<T> boundedSource,
      SerializablePipelineOptions serializablePipelineOptions,
      int numSplits) {
    return new FlinkBoundedSource<>(
        stepName, boundedSource, serializablePipelineOptions, Boundedness.BOUNDED, numSplits);
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
      int numSplits) {
    this.stepName = stepName;
    this.beamSource = beamSource;
    this.serializablePipelineOptions = serializablePipelineOptions;
    this.boundedness = boundedness;
    this.numSplits = numSplits;
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
      // Some Beam transforms instead use a bounded source to emit lightweight work descriptors.
      // The expensive operation then runs in the next Flink operator. A fast source reader can emit
      // many descriptors before its peers start, and a RESCALE/FORWARD-style edge preserves that
      // sparse allocation. Static assignment bounds every reader to its round-robin share before
      // any descriptors are emitted, so the downstream pointwise edge receives balanced work.
      //
      // Estimated bytes per requested reader provide a source-local signal, unlike a pipeline-wide
      // option. Small sources retain work stealing. Large sources use static assignment so that a
      // few fast readers cannot drain all descriptors before downstream backpressure arrives.
      PipelineOptions pipelineOptions = serializablePipelineOptions.get();
      if (shouldUseStaticSplitAssignment(pipelineOptions, enumContext.currentParallelism())) {
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
    checkpoint.forEach(
        (subtaskId, splitsForSubtask) -> enumerator.addSplitsBack(splitsForSubtask, subtaskId));
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

  private boolean shouldUseStaticSplitAssignment(
      PipelineOptions pipelineOptions, int sourceParallelism) throws Exception {
    if (sourceParallelism <= 0) {
      LOG.warn(
          "Using lazy split assignment for bounded source {} because the source parallelism is "
              + "not positive: {}",
          beamSource,
          sourceParallelism);
      return false;
    }

    BoundedSource<?> boundedSource = (BoundedSource<?>) beamSource;
    long estimatedSizeBytes = boundedSource.getEstimatedSizeBytes(pipelineOptions);
    long estimatedBytesPerReader = estimatedSizeBytes / sourceParallelism;
    boolean useStaticAssignment = estimatedBytesPerReader >= STATIC_ASSIGNMENT_MIN_BYTES_PER_READER;

    LOG.info(
        "Using {} split assignment for bounded source {}: estimated size {} bytes, source "
            + "parallelism {}, estimated bytes per reader {}, static assignment threshold {} "
            + "bytes",
        useStaticAssignment ? "static" : "lazy",
        beamSource,
        estimatedSizeBytes,
        sourceParallelism,
        estimatedBytesPerReader,
        STATIC_ASSIGNMENT_MIN_BYTES_PER_READER);
    return useStaticAssignment;
  }

  @FunctionalInterface
  public interface TimestampExtractor<T> extends Function<T, Long>, Serializable {}
}
