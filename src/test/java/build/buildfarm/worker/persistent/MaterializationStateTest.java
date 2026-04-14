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
import static org.junit.Assert.assertThrows;
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
public class MaterializationStateTest {
  private static FetchResult.Entry dirEntry(String path, String digestHash) {
    return new FetchResult.Entry(
        path,
        FetchResult.EntryType.DIRECTORY,
        Path.of("/cas/" + digestHash),
        null,
        build.bazel.remote.execution.v2.Digest.newBuilder()
            .setHash(digestHash)
            .setSizeBytes(100)
            .build(),
        null);
  }

  private static FetchResult.Entry fileEntry(String path, String key) {
    return new FetchResult.Entry(
        path, FetchResult.EntryType.FILE, Path.of("/cas/" + key), key, null, null);
  }

  private static FetchResult.Entry zeroEntry(String path) {
    return new FetchResult.Entry(
        path, FetchResult.EntryType.ZERO_SIZE_FILE, null, null, null, null);
  }

  private static FetchResult.Entry symlinkEntry(String path, String target) {
    return new FetchResult.Entry(
        path, FetchResult.EntryType.SYMLINK_NODE, null, null, null, target);
  }

  private static FetchResult makeFetchResult(
      ImmutableList<FetchResult.Entry> entries, ImmutableSet<String> descendedDirectories) {
    CASFileCache mockCache = mock(CASFileCache.class);
    return new FetchResult(
        entries,
        descendedDirectories,
        ImmutableList.of(),
        ImmutableList.of(),
        DigestFunction.Value.SHA256,
        mockCache,
        ImmutableMap.of(),
        ImmutableSet.of());
  }

  @Test
  public void diff_identicalInputs_emptyDiff() {
    FetchResult prev =
        makeFetchResult(
            ImmutableList.of(dirEntry("src", "aaa"), fileEntry("build/Foo.java", "key1")),
            ImmutableSet.of("build"));
    MaterializationState state = MaterializationState.fromFetchResult(prev);

    FetchResult next =
        makeFetchResult(
            ImmutableList.of(dirEntry("src", "aaa"), fileEntry("build/Foo.java", "key1")),
            ImmutableSet.of("build"));

    MaterializationState.DiffResult diff = state.diff(next);
    assertThat(diff.added()).isEmpty();
    assertThat(diff.removed()).isEmpty();
    assertThat(diff.changed()).isEmpty();
    assertThat(diff.descendedDirectoriesChanged()).isFalse();
  }

  @Test
  public void diff_newFileAdded() {
    FetchResult prev = makeFetchResult(ImmutableList.of(dirEntry("src", "aaa")), ImmutableSet.of());
    MaterializationState state = MaterializationState.fromFetchResult(prev);

    FetchResult next =
        makeFetchResult(
            ImmutableList.of(dirEntry("src", "aaa"), fileEntry("build/Bar.java", "key2")),
            ImmutableSet.of("build"));

    MaterializationState.DiffResult diff = state.diff(next);
    assertThat(diff.added()).hasSize(1);
    assertThat(diff.added().get(0).relativePath()).isEqualTo("build/Bar.java");
    assertThat(diff.removed()).isEmpty();
    assertThat(diff.changed()).isEmpty();
  }

  @Test
  public void diff_fileRemoved() {
    FetchResult prev =
        makeFetchResult(
            ImmutableList.of(dirEntry("src", "aaa"), fileEntry("build/Foo.java", "key1")),
            ImmutableSet.of("build"));
    MaterializationState state = MaterializationState.fromFetchResult(prev);

    FetchResult next = makeFetchResult(ImmutableList.of(dirEntry("src", "aaa")), ImmutableSet.of());

    MaterializationState.DiffResult diff = state.diff(next);
    assertThat(diff.added()).isEmpty();
    assertThat(diff.removed()).hasSize(1);
    assertThat(diff.removed().get(0).relativePath()).isEqualTo("build/Foo.java");
    assertThat(diff.changed()).isEmpty();
  }

  @Test
  public void diff_directoryDigestChanged() {
    FetchResult prev = makeFetchResult(ImmutableList.of(dirEntry("src", "aaa")), ImmutableSet.of());
    MaterializationState state = MaterializationState.fromFetchResult(prev);

    FetchResult next = makeFetchResult(ImmutableList.of(dirEntry("src", "bbb")), ImmutableSet.of());

    MaterializationState.DiffResult diff = state.diff(next);
    assertThat(diff.added()).isEmpty();
    assertThat(diff.removed()).isEmpty();
    assertThat(diff.changed()).hasSize(1);
    assertThat(diff.changed().get(0).relativePath()).isEqualTo("src");
  }

  @Test
  public void diff_entryTypeChanged_classifiedAsChanged() {
    // A path whose entry type flips (FILE -> DIRECTORY) is a content change, classified as
    // "changed" (not an add+remove pair). applyDelta's changed branch (deleteExistingPath + relink)
    // depends on this to replace a prior on-disk file with a directory symlink.
    FetchResult prev = makeFetchResult(ImmutableList.of(fileEntry("x", "key1")), ImmutableSet.of());
    MaterializationState state = MaterializationState.fromFetchResult(prev);

    FetchResult next = makeFetchResult(ImmutableList.of(dirEntry("x", "aaa")), ImmutableSet.of());

    MaterializationState.DiffResult diff = state.diff(next);
    assertThat(diff.added()).isEmpty();
    assertThat(diff.removed()).isEmpty();
    assertThat(diff.changed()).hasSize(1);
    assertThat(diff.changed().get(0).relativePath()).isEqualTo("x");
    assertThat(diff.changed().get(0).type()).isEqualTo(FetchResult.EntryType.DIRECTORY);
  }

  @Test
  public void diff_zeroSizeFileUnchanged_emptyDiff() {
    // Two ZERO_SIZE_FILE entries at the same path are interchangeable (no content) -> unchanged.
    FetchResult prev = makeFetchResult(ImmutableList.of(zeroEntry("empty.txt")), ImmutableSet.of());
    MaterializationState state = MaterializationState.fromFetchResult(prev);

    FetchResult next = makeFetchResult(ImmutableList.of(zeroEntry("empty.txt")), ImmutableSet.of());

    MaterializationState.DiffResult diff = state.diff(next);
    assertThat(diff.added()).isEmpty();
    assertThat(diff.removed()).isEmpty();
    assertThat(diff.changed()).isEmpty();
  }

  @Test
  public void diff_symlinkTargetChanged_classifiedAsChanged() {
    // Two SYMLINK_NODE entries at the same path with different targets are a content change ->
    // "changed". digestsMatch compares symlink targets, so a retargeted symlink must be re-linked.
    FetchResult prev =
        makeFetchResult(ImmutableList.of(symlinkEntry("link", "../old/target")), ImmutableSet.of());
    MaterializationState state = MaterializationState.fromFetchResult(prev);

    FetchResult next =
        makeFetchResult(ImmutableList.of(symlinkEntry("link", "../new/target")), ImmutableSet.of());

    MaterializationState.DiffResult diff = state.diff(next);
    assertThat(diff.added()).isEmpty();
    assertThat(diff.removed()).isEmpty();
    assertThat(diff.changed()).hasSize(1);
    assertThat(diff.changed().get(0).relativePath()).isEqualTo("link");
    assertThat(diff.changed().get(0).symlinkTarget()).isEqualTo("../new/target");
  }

  @Test
  public void diff_symlinkTargetUnchanged_emptyDiff() {
    // Same path, same target -> digestsMatch returns true, so the symlink is left in place.
    FetchResult prev =
        makeFetchResult(ImmutableList.of(symlinkEntry("link", "../target")), ImmutableSet.of());
    MaterializationState state = MaterializationState.fromFetchResult(prev);

    FetchResult next =
        makeFetchResult(ImmutableList.of(symlinkEntry("link", "../target")), ImmutableSet.of());

    MaterializationState.DiffResult diff = state.diff(next);
    assertThat(diff.added()).isEmpty();
    assertThat(diff.removed()).isEmpty();
    assertThat(diff.changed()).isEmpty();
  }

  @Test
  public void diff_descendedDirectoriesChanged() {
    FetchResult prev =
        makeFetchResult(ImmutableList.of(dirEntry("src", "aaa")), ImmutableSet.of("build"));
    MaterializationState state = MaterializationState.fromFetchResult(prev);

    FetchResult next =
        makeFetchResult(ImmutableList.of(dirEntry("src", "aaa")), ImmutableSet.of("out"));

    MaterializationState.DiffResult diff = state.diff(next);
    assertThat(diff.descendedDirectoriesChanged()).isTrue();
  }

  @Test
  public void diff_descendedDirectoriesUnchanged() {
    FetchResult prev =
        makeFetchResult(ImmutableList.of(dirEntry("src", "aaa")), ImmutableSet.of("build"));
    MaterializationState state = MaterializationState.fromFetchResult(prev);

    FetchResult next =
        makeFetchResult(ImmutableList.of(dirEntry("src", "bbb")), ImmutableSet.of("build"));

    MaterializationState.DiffResult diff = state.diff(next);
    assertThat(diff.descendedDirectoriesChanged()).isFalse();
    assertThat(diff.changed()).hasSize(1); // digest changed, but descendedDirectories didn't
  }

  @Test
  public void diff_multipleChanges() {
    FetchResult prev =
        makeFetchResult(
            ImmutableList.of(
                dirEntry("src", "aaa"),
                dirEntry("lib", "bbb"),
                fileEntry("build/Foo.java", "key1")),
            ImmutableSet.of("build"));
    MaterializationState state = MaterializationState.fromFetchResult(prev);

    FetchResult next =
        makeFetchResult(
            ImmutableList.of(
                dirEntry("src", "ccc"), // changed digest
                // lib removed
                fileEntry("build/Foo.java", "key1"), // unchanged
                fileEntry("build/Bar.java", "key2")), // added
            ImmutableSet.of("build"));

    MaterializationState.DiffResult diff = state.diff(next);
    assertThat(diff.added()).hasSize(1);
    assertThat(diff.added().get(0).relativePath()).isEqualTo("build/Bar.java");
    assertThat(diff.removed()).hasSize(1);
    assertThat(diff.removed().get(0).relativePath()).isEqualTo("lib");
    assertThat(diff.changed()).hasSize(1);
    assertThat(diff.changed().get(0).relativePath()).isEqualTo("src");
    assertThat(diff.descendedDirectoriesChanged()).isFalse();
  }

  @Test
  public void fromFetchResult_createsCorrectState() {
    FetchResult fetchResult =
        makeFetchResult(
            ImmutableList.of(dirEntry("src", "aaa"), fileEntry("build/Foo.java", "key1")),
            ImmutableSet.of("build"));
    MaterializationState state = MaterializationState.fromFetchResult(fetchResult);

    assertThat(state.getEntry("src")).isNotNull();
    assertThat(state.getEntry("build/Foo.java")).isNotNull();
    assertThat(state.getEntry("nonexistent")).isNull();
    assertThat(state.descendedDirectories()).containsExactly("build");
  }

  // ---- carried-ref close() tests ----

  @Test
  public void close_decrementsCarriedRefs() throws Exception {
    CASFileCache mockCache = mock(CASFileCache.class);
    build.bazel.remote.execution.v2.Digest testDigest =
        build.bazel.remote.execution.v2.Digest.newBuilder()
            .setHash("aaa")
            .setSizeBytes(100)
            .build();

    FetchResult fetchResult =
        makeFetchResult(ImmutableList.of(fileEntry("src/Foo.java", "key1")), ImmutableSet.of());

    MaterializationState state =
        MaterializationState.fromFetchResult(
            fetchResult,
            ImmutableList.of("key1"),
            ImmutableList.of(testDigest),
            DigestFunction.Value.SHA256,
            mockCache);

    state.close();

    verify(mockCache, times(1))
        .decrementReferences(
            eq(ImmutableList.of("key1")),
            eq(ImmutableList.of(testDigest)),
            eq(DigestFunction.Value.SHA256));
  }

  @Test
  public void close_isIdempotent() throws Exception {
    CASFileCache mockCache = mock(CASFileCache.class);

    FetchResult fetchResult =
        makeFetchResult(ImmutableList.of(fileEntry("src/Foo.java", "key1")), ImmutableSet.of());

    MaterializationState state =
        MaterializationState.fromFetchResult(
            fetchResult,
            ImmutableList.of("key1"),
            ImmutableList.of(),
            DigestFunction.Value.SHA256,
            mockCache);

    state.close();
    state.close(); // second call should be a no-op

    verify(mockCache, times(1)).decrementReferences(any(), any(), any(DigestFunction.Value.class));
  }

  @Test
  public void close_failedDecrementCanRetry() throws Exception {
    CASFileCache mockCache = mock(CASFileCache.class);
    ImmutableList<String> refKeys = ImmutableList.of("key1");
    ImmutableList<build.bazel.remote.execution.v2.Digest> refDigests = ImmutableList.of();

    doThrow(new IOException("temporary failure"))
        .doNothing()
        .when(mockCache)
        .decrementReferences(eq(refKeys), eq(refDigests), eq(DigestFunction.Value.SHA256));

    FetchResult fetchResult =
        makeFetchResult(ImmutableList.of(fileEntry("src/Foo.java", "key1")), ImmutableSet.of());

    MaterializationState state =
        MaterializationState.fromFetchResult(
            fetchResult, refKeys, refDigests, DigestFunction.Value.SHA256, mockCache);

    state.close();
    state.close();

    verify(mockCache, times(2))
        .decrementReferences(eq(refKeys), eq(refDigests), eq(DigestFunction.Value.SHA256));
  }

  @Test
  public void close_noOpWhenNoRefs() throws Exception {
    CASFileCache mockCache = mock(CASFileCache.class);

    // no refs carried
    FetchResult fetchResult =
        makeFetchResult(ImmutableList.of(fileEntry("src/Foo.java", "key1")), ImmutableSet.of());
    MaterializationState state = MaterializationState.fromFetchResult(fetchResult);

    state.close();

    verify(mockCache, never()).decrementReferences(any(), any(), any(DigestFunction.Value.class));
  }

  // ---- digestsMatch null-field crash tests ----

  @Test
  public void diff_fileEntryWithNullKey_throwsNPE() {
    FetchResult prev =
        makeFetchResult(ImmutableList.of(fileEntry("src/Foo.java", "key1")), ImmutableSet.of());
    MaterializationState state = MaterializationState.fromFetchResult(prev);

    // Malformed FILE entry with null key
    FetchResult next =
        makeFetchResult(
            ImmutableList.of(
                new FetchResult.Entry(
                    "src/Foo.java",
                    FetchResult.EntryType.FILE,
                    Path.of("/cas/x"),
                    null,
                    null,
                    null)),
            ImmutableSet.of());

    assertThrows(NullPointerException.class, () -> state.diff(next));
  }

  @Test
  public void diff_directoryEntryWithNullDigest_throwsNPE() {
    FetchResult prev = makeFetchResult(ImmutableList.of(dirEntry("src", "aaa")), ImmutableSet.of());
    MaterializationState state = MaterializationState.fromFetchResult(prev);

    // Malformed DIRECTORY entry with null digest
    FetchResult next =
        makeFetchResult(
            ImmutableList.of(
                new FetchResult.Entry(
                    "src", FetchResult.EntryType.DIRECTORY, Path.of("/cas/x"), null, null, null)),
            ImmutableSet.of());

    assertThrows(NullPointerException.class, () -> state.diff(next));
  }

  @Test
  public void diff_symlinkEntryWithNullTarget_throwsNPE() {
    FetchResult prev =
        makeFetchResult(
            ImmutableList.of(
                new FetchResult.Entry(
                    "link", FetchResult.EntryType.SYMLINK_NODE, null, null, null, "../target")),
            ImmutableSet.of());
    MaterializationState state = MaterializationState.fromFetchResult(prev);

    // Malformed SYMLINK_NODE entry with null target
    FetchResult next =
        makeFetchResult(
            ImmutableList.of(
                new FetchResult.Entry(
                    "link", FetchResult.EntryType.SYMLINK_NODE, null, null, null, null)),
            ImmutableSet.of());

    assertThrows(NullPointerException.class, () -> state.diff(next));
  }
}
