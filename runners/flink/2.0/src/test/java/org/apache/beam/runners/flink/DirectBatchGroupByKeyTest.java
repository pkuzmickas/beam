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
package org.apache.beam.runners.flink;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Test;

/** Correctness coverage for direct bounded {@link GroupByKey} translation. */
public class DirectBatchGroupByKeyTest implements Serializable {

  private static final int LARGE_VALUE_COUNT = 64;
  private static final int LARGE_VALUE_SIZE = 256 * 1024;

  @Test
  public void testDirectAndPreAggregatedBatchGroupByKeyProduceTheSameResults() {
    runCorrectnessMatrix(false);
    runCorrectnessMatrix(true);
  }

  @Test
  public void testDirectBatchGroupByKeyHandlesLargeGroupOfSmallValues() {
    Pipeline pipeline = batchPipeline(true);
    byte[] value = new byte[LARGE_VALUE_SIZE];
    List<KV<String, byte[]>> input = new ArrayList<>();
    for (int i = 0; i < LARGE_VALUE_COUNT; i++) {
      input.add(KV.of("hot-key", value));
    }

    PCollection<KV<Long, Long>> result =
        pipeline
            .apply(
                "LargeInput",
                Create.of(input).withCoder(KvCoder.of(StringUtf8Coder.of(), ByteArrayCoder.of())))
            .apply("LargeGBK", GroupByKey.create())
            .apply(
                "MeasureLargeGroup",
                ParDo.of(
                    new DoFn<KV<String, Iterable<byte[]>>, KV<Long, Long>>() {
                      @ProcessElement
                      public void processElement(ProcessContext context) {
                        long count = 0;
                        long bytes = 0;
                        for (byte[] groupedValue : context.element().getValue()) {
                          count++;
                          bytes += groupedValue.length;
                        }
                        context.output(KV.of(count, bytes));
                      }
                    }));

    PAssert.that(result)
        .containsInAnyOrder(
            KV.of((long) LARGE_VALUE_COUNT, (long) LARGE_VALUE_COUNT * LARGE_VALUE_SIZE));
    Assert.assertEquals(PipelineResult.State.DONE, pipeline.run().waitUntilFinish());
  }

  private static void runCorrectnessMatrix(boolean disablePreAggregation) {
    Pipeline pipeline = batchPipeline(disablePreAggregation);

    PCollection<String> globalResult =
        pipeline
            .apply(
                "GlobalInput",
                Create.of(
                    KV.of("many", 3), KV.of("many", 1), KV.of("many", 2), KV.of("singleton", 7)))
            .apply("GlobalGBK", GroupByKey.create())
            .apply("NormalizeGlobal", ParDo.of(new NormalizeIntegerGroup()));
    PAssert.that(globalResult).containsInAnyOrder("many=[1, 2, 3]", "singleton=[7]");

    PCollection<Integer> fixedWindowResult =
        pipeline
            .apply(
                "FixedInput",
                Create.timestamped(
                        TimestampedValue.of(KV.of("nulls", (Void) null), new Instant(0)),
                        TimestampedValue.of(KV.of("nulls", (Void) null), new Instant(5)),
                        TimestampedValue.of(KV.of("nulls", (Void) null), new Instant(20)))
                    .withCoder(KvCoder.of(StringUtf8Coder.of(), VoidCoder.of())))
            .apply("FixedWindows", Window.into(FixedWindows.of(Duration.millis(10))))
            .apply("FixedGBK", GroupByKey.create())
            .apply("CountNulls", ParDo.of(new CountValues<>()));
    PAssert.that(fixedWindowResult).containsInAnyOrder(2, 1);

    PCollection<Integer> sessionResult =
        pipeline
            .apply(
                "SessionInput",
                Create.timestamped(
                    TimestampedValue.of(KV.of("sessions", 1), new Instant(0)),
                    TimestampedValue.of(KV.of("sessions", 2), new Instant(5)),
                    TimestampedValue.of(KV.of("sessions", 3), new Instant(30))))
            .apply("SessionWindows", Window.into(Sessions.withGapDuration(Duration.millis(10))))
            .apply("SessionGBK", GroupByKey.create())
            .apply("CountSessions", ParDo.of(new CountValues<>()));
    PAssert.that(sessionResult).containsInAnyOrder(2, 1);

    PCollection<KV<String, Iterable<Integer>>> emptyResult =
        pipeline
            .apply("EmptyInput", Create.empty(KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of())))
            .apply("EmptyGBK", GroupByKey.create());
    PAssert.that(emptyResult).empty();

    Assert.assertEquals(PipelineResult.State.DONE, pipeline.run().waitUntilFinish());
  }

  private static Pipeline batchPipeline(boolean disablePreAggregation) {
    Pipeline pipeline = FlinkTestPipeline.createForBatch();
    pipeline
        .getOptions()
        .as(FlinkPipelineOptions.class)
        .setDisableBatchGroupByKeyPreAggregation(disablePreAggregation);
    return pipeline;
  }

  private static class NormalizeIntegerGroup extends DoFn<KV<String, Iterable<Integer>>, String> {

    @ProcessElement
    public void processElement(ProcessContext context) {
      List<Integer> values = new ArrayList<>();
      context.element().getValue().forEach(values::add);
      Collections.sort(values);
      context.output(context.element().getKey() + "=" + values);
    }
  }

  private static class CountValues<T> extends DoFn<KV<String, Iterable<T>>, Integer> {

    @ProcessElement
    public void processElement(ProcessContext context) {
      int count = 0;
      for (T ignored : context.element().getValue()) {
        count++;
      }
      context.output(count);
    }
  }
}
