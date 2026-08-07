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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.translation.wrappers.streaming.io.TestBoundedCountingSource;
import org.apache.beam.runners.flink.translation.wrappers.streaming.io.TestCountingSource;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileBasedSource;
import org.apache.beam.sdk.io.FileBasedSource.FileBasedReader;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.Source;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.values.KV;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.connector.testutils.source.reader.TestingSplitEnumeratorContext;
import org.junit.Test;

/** Unit tests for {@link FlinkSourceSplitEnumerator}. */
public class FlinkSourceSplitEnumeratorTest {
  private static final long MEBIBYTE = 1024L * 1024L;
  private static final int SOURCE_PARALLELISM = 2;

  @Test
  public void testSmallBoundedSourceUsesLazyAssignment() throws Exception {
    long estimatedSizeBytes =
        SOURCE_PARALLELISM * FlinkSource.STATIC_ASSIGNMENT_MIN_BYTES_PER_READER - 1L;
    try (SplitEnumerator<
            FlinkSourceSplit<KV<Integer, Integer>>,
            Map<Integer, List<FlinkSourceSplit<KV<Integer, Integer>>>>>
        enumerator = createBoundedEnumerator(estimatedSizeBytes)) {
      // Even one byte below the per-reader threshold retains work stealing because a fixed split
      // allocation would make the job wait for the slowest reader without enough balancing value.
      assertTrue(enumerator instanceof LazyFlinkSourceSplitEnumerator);
      assertFalse(enumerator instanceof FlinkSourceSplitEnumerator);
    }
  }

  @Test
  public void testLargeBoundedSourceUsesStaticRoundRobinAssignment() throws Exception {
    long estimatedSizeBytes =
        SOURCE_PARALLELISM * FlinkSource.STATIC_ASSIGNMENT_MIN_BYTES_PER_READER;
    try (SplitEnumerator<
            FlinkSourceSplit<KV<Integer, Integer>>,
            Map<Integer, List<FlinkSourceSplit<KV<Integer, Integer>>>>>
        enumerator = createBoundedEnumerator(estimatedSizeBytes)) {
      // Reaching the per-reader threshold selects static assignment. The bounded-source assignment
      // test below verifies that the enumerator gives every reader an equal round-robin share.
      assertTrue(enumerator instanceof FlinkSourceSplitEnumerator);
      assertFalse(enumerator instanceof LazyFlinkSourceSplitEnumerator);
    }
  }

  @Test
  public void testAssignSplitsWithBoundedSource() throws IOException {
    final int numSubtasks = 2;
    final int numSplits = 10;
    final int totalNumRecords = 10;
    TestingSplitEnumeratorContext<FlinkSourceSplit<KV<Integer, Integer>>> testContext =
        new TestingSplitEnumeratorContext<>(numSubtasks);
    TestBoundedCountingSource testSource =
        new TestBoundedCountingSource(numSplits, totalNumRecords);

    assignSplits(testContext, testSource, numSplits);
    assertEquals(numSubtasks, testContext.getSplitAssignments().size());

    testContext
        .getSplitAssignments()
        .forEach(
            (subtaskId, state) -> {
              int expectedNumSplitsPerSubtask = numSplits / numSubtasks;
              assertEquals(
                  "Each subtask should have " + expectedNumSplitsPerSubtask + " assigned splits",
                  expectedNumSplitsPerSubtask,
                  state.getAssignedSplits().size());
              assertTrue(
                  "Each subtask should have received NoMoreSplits",
                  state.hasReceivedNoMoreSplitsSignal());
              state
                  .getAssignedSplits()
                  .forEach(
                      split -> {
                        TestBoundedCountingSource source =
                            (TestBoundedCountingSource) split.getBeamSplitSource();
                        try {
                          int expectedSplitSize = totalNumRecords / numSplits;
                          assertEquals(
                              expectedSplitSize,
                              source.getEstimatedSizeBytes(FlinkPipelineOptions.defaults()));
                        } catch (Exception e) {
                          fail("Received exception" + e);
                        }
                      });
            });
  }

  @Test
  public void testSignalsNoMoreSplitsToEarlyReaderWithoutAssignment() throws IOException {
    final int numSubtasks = 2;
    final int numSplits = 1;
    TestingSplitEnumeratorContext<FlinkSourceSplit<KV<Integer, Integer>>> testContext =
        new TestingSplitEnumeratorContext<>(numSubtasks);
    TestBoundedCountingSource testSource = new TestBoundedCountingSource(numSplits, numSplits);

    try (FlinkSourceSplitEnumerator<KV<Integer, Integer>> splitEnumerator =
        new FlinkSourceSplitEnumerator<>(
            testContext, testSource, FlinkPipelineOptions.defaults(), numSplits)) {
      splitEnumerator.start();

      // Register both readers before the asynchronous split operation completes. Reader 1 will
      // not receive a split, but it must still receive NoMoreSplits after initialization. Without
      // that signal a bounded source remains active forever and prevents the batch job finishing.
      for (int subtaskId = 0; subtaskId < numSubtasks; subtaskId++) {
        testContext.registerReader(subtaskId, String.valueOf(subtaskId));
        splitEnumerator.addReader(subtaskId);
      }
      testContext.getExecutorService().triggerAll();

      assertEquals(numSubtasks, testContext.getSplitAssignments().size());
      assertEquals(1, testContext.getSplitAssignments().get(0).getAssignedSplits().size());
      assertEquals(0, testContext.getSplitAssignments().get(1).getAssignedSplits().size());
      assertTrue(testContext.getSplitAssignments().get(0).hasReceivedNoMoreSplitsSignal());
      assertTrue(testContext.getSplitAssignments().get(1).hasReceivedNoMoreSplitsSignal());
    }
  }

  @Test
  public void testStaticAssignmentRespectsFileInputSplitMaxSize() throws IOException {
    final int numSubtasks = 2;
    final int requestedSplits = 2;
    final long fileSize = 100L * MEBIBYTE;
    final long maxSplitSizeMb = 10L;
    final int expectedSplits = 10;
    FlinkPipelineOptions options = FlinkPipelineOptions.defaults();
    options.setFileInputSplitMaxSizeMB(maxSplitSizeMb);
    TestingSplitEnumeratorContext<FlinkSourceSplit<String>> testContext =
        new TestingSplitEnumeratorContext<>(numSubtasks);
    TestFileBasedSource testSource = TestFileBasedSource.create(fileSize);

    try (FlinkSourceSplitEnumerator<String> splitEnumerator =
        new FlinkSourceSplitEnumerator<>(testContext, testSource, options, requestedSplits)) {
      splitEnumerator.start();
      for (int subtaskId = 0; subtaskId < numSubtasks; subtaskId++) {
        testContext.registerReader(subtaskId, String.valueOf(subtaskId));
        splitEnumerator.addReader(subtaskId);
      }
      testContext.getExecutorService().triggerAll();

      // Without the cap, the 100 MiB source and two requested splits would produce two 50 MiB
      // splits. The 10 MiB cap must instead produce ten splits while retaining round-robin balance.
      int actualSplits =
          testContext.getSplitAssignments().values().stream()
              .mapToInt(state -> state.getAssignedSplits().size())
              .sum();
      assertEquals(expectedSplits, actualSplits);
      testContext
          .getSplitAssignments()
          .values()
          .forEach(
              state -> {
                assertEquals(expectedSplits / numSubtasks, state.getAssignedSplits().size());
                assertTrue(state.hasReceivedNoMoreSplitsSignal());
              });
    }
  }

  @Test
  public void testAssignSplitsWithUnboundedSource() throws IOException {
    final int numSplits = 10;
    final int numSubtasks = 5;
    final int numRecordsPerSplit = 10;
    TestingSplitEnumeratorContext<FlinkSourceSplit<KV<Integer, Integer>>> testContext =
        new TestingSplitEnumeratorContext<>(numSubtasks);
    TestCountingSource testSource = new TestCountingSource(numRecordsPerSplit);

    assignSplits(testContext, testSource, numSplits);

    testContext
        .getSplitAssignments()
        .forEach(
            (subtaskId, state) -> {
              int expectedNumSplitsPerSubtask = numSplits / numSubtasks;
              assertEquals(
                  "Each subtask should have " + expectedNumSplitsPerSubtask + " assigned splits",
                  expectedNumSplitsPerSubtask,
                  state.getAssignedSplits().size());
              assertTrue(
                  "Each subtask should have received NoMoreSplits",
                  state.hasReceivedNoMoreSplitsSignal());
            });
  }

  @Test
  public void testAddSplitsBack() throws IOException {
    final int numSubtasks = 2;
    final int numSplits = 10;
    final int totalNumRecords = 10;
    TestingSplitEnumeratorContext<FlinkSourceSplit<KV<Integer, Integer>>> testContext =
        new TestingSplitEnumeratorContext<>(numSubtasks);
    TestBoundedCountingSource testSource =
        new TestBoundedCountingSource(numSplits, totalNumRecords);
    try (FlinkSourceSplitEnumerator<KV<Integer, Integer>> splitEnumerator =
        new FlinkSourceSplitEnumerator<>(
            testContext, testSource, FlinkPipelineOptions.defaults(), numSplits)) {
      splitEnumerator.start();
      testContext.registerReader(0, "0");
      splitEnumerator.addReader(0);
      testContext.getExecutorService().triggerAll();

      List<FlinkSourceSplit<KV<Integer, Integer>>> splitsForReader =
          testContext.getSplitAssignments().get(0).getAssignedSplits();
      assertEquals(numSplits / numSubtasks, splitsForReader.size());

      splitEnumerator.addSplitsBack(splitsForReader, 0);
      splitEnumerator.addReader(0);
      assertEquals(2 * numSplits / numSubtasks, splitsForReader.size());
    }
  }

  @Test
  public void testAddSplitsBackAfterRescale() throws Exception {
    final int numSubtasks = 2;
    final int numSplits = 10;
    final int totalNumRecords = 10;
    TestingSplitEnumeratorContext<FlinkSourceSplit<KV<Integer, Integer>>> testContext =
        new TestingSplitEnumeratorContext<>(numSubtasks);
    TestBoundedCountingSource testSource =
        new TestBoundedCountingSource(numSplits, totalNumRecords);
    final Map<Integer, List<FlinkSourceSplit<KV<Integer, Integer>>>> assignment;
    try (FlinkSourceSplitEnumerator<KV<Integer, Integer>> splitEnumerator =
        new FlinkSourceSplitEnumerator<>(
            testContext, testSource, FlinkPipelineOptions.defaults(), numSplits)) {
      splitEnumerator.start();
      for (int i = 0; i < numSubtasks; i++) {
        testContext.registerReader(i, String.valueOf(i));
        splitEnumerator.addReader(i);
      }
      testContext.getExecutorService().triggerAll();
      assignment =
          testContext.getSplitAssignments().entrySet().stream()
              .map(e -> KV.of(e.getKey(), e.getValue().getAssignedSplits()))
              .collect(Collectors.toMap(KV::getKey, KV::getValue));
    }

    // add tasks back
    testContext = new TestingSplitEnumeratorContext<>(numSubtasks);
    try (FlinkSourceSplitEnumerator<KV<Integer, Integer>> splitEnumerator =
        new FlinkSourceSplitEnumerator<>(
            testContext, testSource, FlinkPipelineOptions.defaults(), numSplits, true)) {
      splitEnumerator.start();
      assignment.forEach(
          (splitId, assignedSplits) -> splitEnumerator.addSplitsBack(assignedSplits, splitId));
      testContext.registerReader(0, "0");
      splitEnumerator.addReader(0);
      testContext.getExecutorService().triggerAll();

      List<FlinkSourceSplit<KV<Integer, Integer>>> splitsForReader =
          testContext.getSplitAssignments().get(0).getAssignedSplits();
      assertEquals(numSplits / numSubtasks, splitsForReader.size());
    }
  }

  private void assignSplits(
      TestingSplitEnumeratorContext<FlinkSourceSplit<KV<Integer, Integer>>> context,
      Source<KV<Integer, Integer>> source,
      int numSplits)
      throws IOException {
    try (FlinkSourceSplitEnumerator<KV<Integer, Integer>> splitEnumerator =
        new FlinkSourceSplitEnumerator<>(
            context, source, FlinkPipelineOptions.defaults(), numSplits)) {
      splitEnumerator.start();
      // Add a reader before splitting the beam source.
      context.registerReader(0, "0");
      splitEnumerator.addReader(0);
      context.getExecutorService().triggerAll();
      context.registerReader(1, "1");
      // Add another reader after splitting the beam source.
      splitEnumerator.addReader(1);
    }
  }

  private SplitEnumerator<
          FlinkSourceSplit<KV<Integer, Integer>>,
          Map<Integer, List<FlinkSourceSplit<KV<Integer, Integer>>>>>
      createBoundedEnumerator(long estimatedSizeBytes) throws Exception {
    final int numSplits = 10;
    TestBoundedCountingSource testSource =
        new TestBoundedCountingSource(numSplits, Math.toIntExact(estimatedSizeBytes));
    FlinkSource<KV<Integer, Integer>, ?> source =
        FlinkSource.bounded(
            "test-bounded-source",
            testSource,
            new SerializablePipelineOptions(FlinkPipelineOptions.defaults()),
            numSplits);
    TestingSplitEnumeratorContext<FlinkSourceSplit<KV<Integer, Integer>>> context =
        new TestingSplitEnumeratorContext<>(SOURCE_PARALLELISM);
    return source.createEnumerator(context);
  }

  private static final class TestFileBasedSource extends FileBasedSource<String> {
    private TestFileBasedSource(Metadata metadata, long startOffset, long endOffset) {
      super(metadata, 1L, startOffset, endOffset);
    }

    private static TestFileBasedSource create(long sizeBytes) {
      Metadata metadata =
          Metadata.builder()
              .setResourceId(FileSystems.matchNewResource("static-split-size-test", false))
              .setSizeBytes(sizeBytes)
              .setIsReadSeekEfficient(true)
              .build();
      return new TestFileBasedSource(metadata, 0L, sizeBytes);
    }

    @Override
    public Coder<String> getOutputCoder() {
      return StringUtf8Coder.of();
    }

    @Override
    protected FileBasedSource<String> createForSubrangeOfFile(
        Metadata metadata, long start, long end) {
      return new TestFileBasedSource(metadata, start, end);
    }

    @Override
    protected FileBasedReader<String> createSingleFileReader(PipelineOptions options) {
      throw new UnsupportedOperationException("This source is only used to test split sizing");
    }
  }
}
