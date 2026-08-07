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

import java.util.Arrays;
import org.apache.beam.runners.flink.translation.wrappers.streaming.io.source.FlinkSourceSplitAssignmentPreference;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;

/** Flink configuration for bounded Beam source split assignment. */
public final class FlinkSourceSplitAssignmentOptions {
  /** Bounded Beam source classes that use static split assignment. */
  public static final ConfigOption<String> STATIC_SOURCE_CLASSES =
      ConfigOptions.key("beam.flink.bounded-source.static-assignment.source-classes")
          .stringType()
          .defaultValue("")
          .withDescription(
              "Comma-separated bounded Beam source class names that use static split assignment. "
                  + "All other bounded sources use lazy split assignment.");

  private FlinkSourceSplitAssignmentOptions() {}

  static FlinkSourceSplitAssignmentPreference preferenceFor(
      ReadableConfig configuration, BoundedSource<?> source) {
    String sourceClassName = source.getClass().getName();
    boolean useStaticAssignment =
        Arrays.stream(configuration.get(STATIC_SOURCE_CLASSES).split(","))
            .map(String::trim)
            .anyMatch(sourceClassName::equals);

    return useStaticAssignment
        ? FlinkSourceSplitAssignmentPreference.STATIC
        : FlinkSourceSplitAssignmentPreference.LAZY;
  }
}
