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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.translation.wrappers.streaming.io.TestBoundedCountingSource;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.values.KV;
import org.apache.flink.connector.testutils.source.reader.TestingSplitEnumeratorContext;
import org.junit.Test;

/** Unit tests for {@link LazyFlinkSourceSplitEnumerator}. */
public class LazyFlinkSourceSplitEnumeratorTest {

  @Test
  public void testSizeEstimationFailurePreservesCause() throws IOException {
    IOException failure = new IOException("Failed to estimate source size");
    TestBoundedCountingSource source =
        new TestBoundedCountingSource(0, 10) {
          @Override
          public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
            throw failure;
          }
        };

    assertInitializationFailure(source, failure);
  }

  @Test
  public void testSplitFailurePreservesCause() throws IOException {
    IOException failure = new IOException("Failed to split source");
    TestBoundedCountingSource source =
        new TestBoundedCountingSource(0, 10) {
          @Override
          public List<? extends BoundedSource<KV<Integer, Integer>>> split(
              long desiredBundleSizeBytes, PipelineOptions options) throws Exception {
            throw failure;
          }
        };

    assertInitializationFailure(source, failure);
  }

  @Test
  public void testSuccessfulInitializationAssignsEachSplitOnce() throws Exception {
    int numSplits = 3;
    TestingSplitEnumeratorContext<FlinkSourceSplit<KV<Integer, Integer>>> context =
        new TestingSplitEnumeratorContext<>(1);
    try (LazyFlinkSourceSplitEnumerator<KV<Integer, Integer>> enumerator =
        new LazyFlinkSourceSplitEnumerator<>(
            context,
            new TestBoundedCountingSource(0, 12),
            FlinkPipelineOptions.defaults(),
            numSplits,
            false)) {
      enumerator.start();
      context.getExecutorService().triggerAll();
      context.registerReader(0, "localhost");
      for (int i = 0; i <= numSplits; i++) {
        enumerator.handleSplitRequest(0, "localhost");
      }

      List<FlinkSourceSplit<KV<Integer, Integer>>> assignedSplits =
          context.getSplitAssignments().get(0).getAssignedSplits();
      assertEquals(numSplits, assignedSplits.size());
      assertEquals(
          numSplits,
          assignedSplits.stream()
              .map(FlinkSourceSplit::splitId)
              .collect(Collectors.toSet())
              .size());
      assertTrue(context.getSplitAssignments().get(0).hasReceivedNoMoreSplitsSignal());
    }
  }

  @Test
  public void testEmptySourceSignalsNoMoreSplits() throws Exception {
    TestingSplitEnumeratorContext<FlinkSourceSplit<KV<Integer, Integer>>> context =
        new TestingSplitEnumeratorContext<>(1);
    try (LazyFlinkSourceSplitEnumerator<KV<Integer, Integer>> enumerator =
        new LazyFlinkSourceSplitEnumerator<>(
            context,
            new TestBoundedCountingSource(0, 0),
            FlinkPipelineOptions.defaults(),
            1,
            false)) {
      enumerator.start();
      context.getExecutorService().triggerAll();
      context.registerReader(0, "localhost");
      enumerator.handleSplitRequest(0, "localhost");

      assertTrue(context.getSplitAssignments().get(0).getAssignedSplits().isEmpty());
      assertTrue(context.getSplitAssignments().get(0).hasReceivedNoMoreSplitsSignal());
    }
  }

  private void assertInitializationFailure(
      TestBoundedCountingSource source, IOException expectedCause) throws IOException {
    TestingSplitEnumeratorContext<FlinkSourceSplit<KV<Integer, Integer>>> context =
        new TestingSplitEnumeratorContext<>(1);
    try (LazyFlinkSourceSplitEnumerator<KV<Integer, Integer>> enumerator =
        new LazyFlinkSourceSplitEnumerator<>(
            context, source, FlinkPipelineOptions.defaults(), 1, false)) {
      enumerator.start();
      RuntimeException failure =
          assertThrows(RuntimeException.class, () -> context.getExecutorService().triggerAll());

      assertEquals("Failed to start source enumerator.", failure.getMessage());
      assertSame(expectedCause, failure.getCause().getCause());
      assertTrue(context.getSplitAssignments().isEmpty());
    }
  }
}
