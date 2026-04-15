// Copyright 2026 The Buildfarm Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package build.buildfarm.worker.persistent;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import build.bazel.remote.execution.v2.DigestFunction;
import build.buildfarm.cas.cfc.CASFileCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.nio.file.Path;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class FetchResultTest {
  @Test
  public void close_decrementsRefsOnce() throws Exception {
    CASFileCache mockCache = mock(CASFileCache.class);
    build.bazel.remote.execution.v2.Digest testDigest =
        build.bazel.remote.execution.v2.Digest.newBuilder()
            .setHash("abc123")
            .setSizeBytes(100)
            .build();

    FetchResult fetchResult =
        new FetchResult(
            ImmutableList.of(
                new FetchResult.Entry(
                    "src/Foo.java",
                    FetchResult.EntryType.FILE,
                    Path.of("/cas/abc123"),
                    "sha256_abc123",
                    null,
                    null),
                new FetchResult.Entry(
                    "lib",
                    FetchResult.EntryType.DIRECTORY,
                    Path.of("/cas/dir_def456"),
                    null,
                    testDigest,
                    null)),
            ImmutableSet.of(),
            ImmutableList.of("sha256_abc123"),
            ImmutableList.of(testDigest),
            DigestFunction.Value.SHA256,
            mockCache,
            ImmutableMap.of(),
            ImmutableSet.of());

    assertThat(fetchResult.isClosed()).isFalse();
    fetchResult.close();
    assertThat(fetchResult.isClosed()).isTrue();

    verify(mockCache, times(1))
        .decrementReferences(
            eq(ImmutableList.of("sha256_abc123")),
            eq(ImmutableList.of(testDigest)),
            eq(DigestFunction.Value.SHA256));
  }

  @Test
  public void doubleClose_isSafe() throws Exception {
    CASFileCache mockCache = mock(CASFileCache.class);

    FetchResult fetchResult =
        new FetchResult(
            ImmutableList.of(
                new FetchResult.Entry(
                    "src/Foo.java",
                    FetchResult.EntryType.FILE,
                    Path.of("/cas/abc123"),
                    "sha256_abc123",
                    null,
                    null)),
            ImmutableSet.of(),
            ImmutableList.of("sha256_abc123"),
            ImmutableList.of(),
            DigestFunction.Value.SHA256,
            mockCache,
            ImmutableMap.of(),
            ImmutableSet.of());

    fetchResult.close();
    fetchResult.close(); // second call should be a no-op

    // decrementReferences called exactly once, not twice
    verify(mockCache, times(1)).decrementReferences(any(), any(), any(DigestFunction.Value.class));
  }

  @Test
  public void close_failedDecrementLeavesOpenForRetry() throws Exception {
    CASFileCache mockCache = mock(CASFileCache.class);
    ImmutableList<String> refKeys = ImmutableList.of("sha256_abc123");
    ImmutableList<build.bazel.remote.execution.v2.Digest> refDigests = ImmutableList.of();

    doThrow(new IOException("temporary failure"))
        .doNothing()
        .when(mockCache)
        .decrementReferences(eq(refKeys), eq(refDigests), eq(DigestFunction.Value.SHA256));

    FetchResult fetchResult =
        new FetchResult(
            ImmutableList.of(),
            ImmutableSet.of(),
            refKeys,
            refDigests,
            DigestFunction.Value.SHA256,
            mockCache,
            ImmutableMap.of(),
            ImmutableSet.of());

    fetchResult.close();
    assertThat(fetchResult.isClosed()).isFalse();

    fetchResult.close();
    assertThat(fetchResult.isClosed()).isTrue();

    verify(mockCache, times(2))
        .decrementReferences(eq(refKeys), eq(refDigests), eq(DigestFunction.Value.SHA256));
  }

  @Test
  public void close_noRefsToDecrement() throws Exception {
    CASFileCache mockCache = mock(CASFileCache.class);

    // FetchResult with only zero-size files and symlink nodes — no CAS refs
    FetchResult fetchResult =
        new FetchResult(
            ImmutableList.of(
                new FetchResult.Entry(
                    "empty.txt", FetchResult.EntryType.ZERO_SIZE_FILE, null, null, null, null),
                new FetchResult.Entry(
                    "link", FetchResult.EntryType.SYMLINK_NODE, null, null, null, "../target")),
            ImmutableSet.of(),
            ImmutableList.of(),
            ImmutableList.of(),
            DigestFunction.Value.SHA256,
            mockCache,
            ImmutableMap.of(),
            ImmutableSet.of());

    fetchResult.close();

    // No refs to decrement — decrementReferences should NOT be called
    verify(mockCache, never()).decrementReferences(any(), any(), any(DigestFunction.Value.class));
  }

  @Test
  public void entries_returnsAllEntries() {
    CASFileCache mockCache = mock(CASFileCache.class);

    ImmutableList<FetchResult.Entry> entries =
        ImmutableList.of(
            new FetchResult.Entry(
                "a/b/file.txt", FetchResult.EntryType.FILE, Path.of("/cas/x"), "key1", null, null),
            new FetchResult.Entry(
                "c/dir", FetchResult.EntryType.DIRECTORY, Path.of("/cas/y"), null, null, null));

    FetchResult fetchResult =
        new FetchResult(
            entries,
            ImmutableSet.of("a", "a/b"),
            ImmutableList.of("key1"),
            ImmutableList.of(),
            DigestFunction.Value.SHA256,
            mockCache,
            ImmutableMap.of(),
            ImmutableSet.of());

    assertThat(fetchResult.entries()).hasSize(2);
    assertThat(fetchResult.descendedDirectories()).containsExactly("a", "a/b");
  }
}
