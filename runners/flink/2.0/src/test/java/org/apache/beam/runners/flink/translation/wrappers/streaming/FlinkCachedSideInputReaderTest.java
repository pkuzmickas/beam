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

import static org.apache.beam.sdk.transforms.Materializations.ITERABLE_MATERIALIZATION_URN;
import static org.apache.beam.sdk.transforms.Materializations.MULTIMAP_MATERIALIZATION_URN;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.sdk.transforms.Materialization;
import org.apache.beam.sdk.transforms.ViewFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.values.PCollectionView;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;
import org.junit.Test;

/** Tests for cached materialization of Flink side-input views. */
public class FlinkCachedSideInputReaderTest {

  @Test
  public void repeatedGetMaterializesOnce() {
    PCollectionView<String> view = view();
    CountingSideInputReader delegate = new CountingSideInputReader("value");
    SideInputReader reader = CachedSideInputReader.of(delegate, Collections.singleton(view));

    assertThat(reader.get(view, GlobalWindow.INSTANCE), is("value"));
    assertThat(reader.get(view, GlobalWindow.INSTANCE), is("value"));
    assertThat(delegate.getCount(), is(1));
  }

  @Test
  public void readersDoNotShareCachedValues() {
    PCollectionView<String> view = view();
    CountingSideInputReader firstDelegate = new CountingSideInputReader("first");
    CountingSideInputReader secondDelegate = new CountingSideInputReader("second");
    SideInputReader firstReader =
        CachedSideInputReader.of(firstDelegate, Collections.singleton(view));
    SideInputReader secondReader =
        CachedSideInputReader.of(secondDelegate, Collections.singleton(view));

    assertThat(firstReader.get(view, GlobalWindow.INSTANCE), is("first"));
    assertThat(secondReader.get(view, GlobalWindow.INSTANCE), is("second"));
    assertThat(firstDelegate.getCount(), is(1));
    assertThat(secondDelegate.getCount(), is(1));
  }

  @Test
  public void keyIncludesViewAndWindow() {
    PCollectionView<String> firstView = view();
    PCollectionView<String> secondView = view();
    IntervalWindow firstWindow = new IntervalWindow(Instant.EPOCH, Instant.ofEpochMilli(10));
    IntervalWindow secondWindow =
        new IntervalWindow(Instant.ofEpochMilli(10), Instant.ofEpochMilli(20));
    CountingSideInputReader delegate = new CountingSideInputReader("value");
    SideInputReader reader =
        CachedSideInputReader.of(delegate, Arrays.asList(firstView, secondView));

    reader.get(firstView, firstWindow);
    reader.get(secondView, firstWindow);
    reader.get(firstView, secondWindow);
    reader.get(firstView, firstWindow);

    assertThat(delegate.getCount(), is(3));
  }

  @Test
  public void invalidateRematerializesLocalValue() {
    PCollectionView<String> view = view();
    CountingSideInputReader delegate = new CountingSideInputReader("old");
    CachedSideInputReader reader =
        (CachedSideInputReader) CachedSideInputReader.of(delegate, Collections.singleton(view));

    assertThat(reader.get(view, GlobalWindow.INSTANCE), is("old"));
    delegate.setValue("new");
    reader.invalidate(view, GlobalWindow.INSTANCE);

    assertThat(reader.get(view, GlobalWindow.INSTANCE), is("new"));
    assertThat(delegate.getCount(), is(2));
  }

  @Test
  public void invalidateAllRematerializesValues() {
    PCollectionView<String> view = view();
    CountingSideInputReader delegate = new CountingSideInputReader("value");
    CachedSideInputReader reader =
        (CachedSideInputReader) CachedSideInputReader.of(delegate, Collections.singleton(view));

    reader.get(view, GlobalWindow.INSTANCE);
    reader.invalidateAll();
    reader.get(view, GlobalWindow.INSTANCE);

    assertThat(delegate.getCount(), is(2));
  }

  @Test
  public void cachesNull() {
    PCollectionView<String> view = view();
    CountingSideInputReader delegate = new CountingSideInputReader(null);
    SideInputReader reader = CachedSideInputReader.of(delegate, Collections.singleton(view));

    assertThat(reader.get(view, GlobalWindow.INSTANCE), nullValue());
    assertThat(reader.get(view, GlobalWindow.INSTANCE), nullValue());
    assertThat(delegate.getCount(), is(1));
  }

  @Test
  public void automaticallyWrapsOnlyMultimapViews() {
    SideInputReader delegate = new CountingSideInputReader("value");
    PCollectionView<String> multimapView = view(MULTIMAP_MATERIALIZATION_URN);
    PCollectionView<String> iterableView = view(ITERABLE_MATERIALIZATION_URN);

    assertThat(
        DoFnOperator.createSideInputReader(Collections.singleton(multimapView), delegate),
        instanceOf(CachedSideInputReader.class));
    assertThat(
        DoFnOperator.createSideInputReader(Collections.singleton(iterableView), delegate),
        is(delegate));
  }

  @Test
  public void nonMultimapViewAlwaysUsesDelegate() {
    PCollectionView<String> view = view(ITERABLE_MATERIALIZATION_URN);
    CountingSideInputReader delegate = new CountingSideInputReader("value");
    SideInputReader reader = CachedSideInputReader.of(delegate, Collections.singleton(view));

    reader.get(view, GlobalWindow.INSTANCE);
    reader.get(view, GlobalWindow.INSTANCE);

    assertThat(reader, is(delegate));
    assertThat(delegate.getCount(), is(2));
  }

  @Test
  public void materializationExceptionPropagatesUnwrapped() {
    PCollectionView<String> view = view();
    SideInputReader reader =
        CachedSideInputReader.of(
            new SideInputReader() {
              @Override
              public <T> @Nullable T get(PCollectionView<T> view, BoundedWindow window) {
                throw new IllegalStateException("materialization failed");
              }

              @Override
              public <T> boolean contains(PCollectionView<T> view) {
                return true;
              }

              @Override
              public boolean isEmpty() {
                return false;
              }
            },
            Collections.singleton(view));

    IllegalStateException exception =
        assertThrows(IllegalStateException.class, () -> reader.get(view, GlobalWindow.INSTANCE));
    assertThat(exception.getMessage(), is("materialization failed"));
  }

  private static <T> PCollectionView<T> view() {
    return view(MULTIMAP_MATERIALIZATION_URN);
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static <T> PCollectionView<T> view(String materializationUrn) {
    PCollectionView<T> view = mock(PCollectionView.class);
    ViewFn viewFn = mock(ViewFn.class);
    Materialization materialization = mock(Materialization.class);
    doReturn(viewFn).when(view).getViewFn();
    doReturn(materialization).when(viewFn).getMaterialization();
    when(materialization.getUrn()).thenReturn(materializationUrn);
    return view;
  }

  private static final class CountingSideInputReader implements SideInputReader {
    private final AtomicInteger getCount = new AtomicInteger();
    private @Nullable Object value;

    private CountingSideInputReader(@Nullable Object value) {
      this.value = value;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> @Nullable T get(PCollectionView<T> view, BoundedWindow window) {
      getCount.incrementAndGet();
      return (T) value;
    }

    @Override
    public <T> boolean contains(PCollectionView<T> view) {
      return true;
    }

    @Override
    public boolean isEmpty() {
      return false;
    }

    private int getCount() {
      return getCount.get();
    }

    private void setValue(@Nullable Object value) {
      this.value = value;
    }
  }
}
