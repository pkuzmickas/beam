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
package org.apache.beam.runners.flink.translation.wrappers.streaming;

import static org.apache.beam.sdk.transforms.Materializations.MULTIMAP_MATERIALIZATION_URN;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.sdk.transforms.Materialization;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Throwables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.Cache;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.CacheBuilder;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.UncheckedExecutionException;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * {@link SideInputReader} that caches costly {@link Materialization materializations} within one
 * operator subtask.
 */
public final class CachedSideInputReader implements SideInputReader {

  private static final int MAX_CACHED_WINDOWS = 1000;
  private static final long CACHE_EXPIRATION_MINUTES = 5;

  public static SideInputReader of(
      SideInputReader delegate, Collection<PCollectionView<?>> sideInputs) {
    Map<PCollectionView<?>, Cache<BoundedWindow, Value<?>>> caches = initCaches(sideInputs);
    return caches.isEmpty() ? delegate : new CachedSideInputReader(delegate, caches);
  }

  private final SideInputReader delegate;
  private final Map<PCollectionView<?>, Cache<BoundedWindow, Value<?>>> caches;

  private CachedSideInputReader(
      SideInputReader delegate, Map<PCollectionView<?>, Cache<BoundedWindow, Value<?>>> caches) {
    this.delegate = delegate;
    this.caches = caches;
  }

  private static Map<PCollectionView<?>, Cache<BoundedWindow, Value<?>>> initCaches(
      Collection<PCollectionView<?>> sideInputs) {
    ImmutableMap.Builder<PCollectionView<?>, Cache<BoundedWindow, Value<?>>> caches =
        ImmutableMap.builder();
    for (PCollectionView<?> view : sideInputs) {
      if (MULTIMAP_MATERIALIZATION_URN.equals(view.getViewFn().getMaterialization().getUrn())) {
        caches.put(view, newCache());
      }
    }
    return caches.build();
  }

  private static Cache<BoundedWindow, Value<?>> newCache() {
    return CacheBuilder.newBuilder()
        .concurrencyLevel(1)
        .maximumSize(MAX_CACHED_WINDOWS)
        .expireAfterAccess(CACHE_EXPIRATION_MINUTES, TimeUnit.MINUTES)
        .softValues()
        .build();
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> @Nullable T get(PCollectionView<T> view, BoundedWindow window) {
    Cache<BoundedWindow, Value<?>> cache = caches.get(view);
    if (cache == null) {
      return delegate.get(view, window);
    }
    try {
      return (T) cache.get(window, () -> new Value<>(delegate.get(view, window))).getValue();
    } catch (ExecutionException | UncheckedExecutionException e) {
      Throwable cause = e.getCause() != null ? e.getCause() : e;
      Throwables.throwIfUnchecked(cause);
      throw new RuntimeException(cause);
    }
  }

  void invalidate(PCollectionView<?> view, BoundedWindow window) {
    Cache<BoundedWindow, Value<?>> cache = caches.get(view);
    if (cache != null) {
      cache.invalidate(window);
    }
  }

  void invalidateAll() {
    caches.values().forEach(Cache::invalidateAll);
  }

  @Override
  public <T> boolean contains(PCollectionView<T> view) {
    return delegate.contains(view);
  }

  @Override
  public boolean isEmpty() {
    return delegate.isEmpty();
  }

  /** Guava caches reject null values, but null is valid for a side-input reader. */
  private static final class Value<T> {
    private final @Nullable T value;

    private Value(@Nullable T value) {
      this.value = value;
    }

    private @Nullable T getValue() {
      return value;
    }
  }
}
