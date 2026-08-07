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

import static org.junit.Assert.assertEquals;

import org.apache.beam.runners.flink.translation.wrappers.streaming.io.TestBoundedCountingSource;
import org.apache.beam.runners.flink.translation.wrappers.streaming.io.source.FlinkSourceSplitAssignmentPreference;
import org.apache.flink.configuration.Configuration;
import org.junit.Test;

/** Tests for {@link FlinkSourceSplitAssignmentOptions}. */
public class FlinkSourceSplitAssignmentOptionsTest {

  @Test
  public void defaultsToLazyAssignment() {
    assertEquals(
        FlinkSourceSplitAssignmentPreference.LAZY,
        FlinkSourceSplitAssignmentOptions.preferenceFor(
            new Configuration(), new TestBoundedCountingSource(1, 1)));
  }

  @Test
  public void configuredSourceClassUsesStaticAssignment() {
    Configuration configuration = new Configuration();
    configuration.set(
        FlinkSourceSplitAssignmentOptions.STATIC_SOURCE_CLASSES,
        "example.OtherSource, " + TestBoundedCountingSource.class.getName());

    assertEquals(
        FlinkSourceSplitAssignmentPreference.STATIC,
        FlinkSourceSplitAssignmentOptions.preferenceFor(
            configuration, new TestBoundedCountingSource(1, 1)));
  }

  @Test
  public void unconfiguredSourceClassUsesLazyAssignment() {
    Configuration configuration = new Configuration();
    configuration.set(
        FlinkSourceSplitAssignmentOptions.STATIC_SOURCE_CLASSES, "example.OtherSource");

    assertEquals(
        FlinkSourceSplitAssignmentPreference.LAZY,
        FlinkSourceSplitAssignmentOptions.preferenceFor(
            configuration, new TestBoundedCountingSource(1, 1)));
  }
}
