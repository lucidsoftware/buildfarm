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

import static com.google.common.base.Preconditions.checkNotNull;

import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.DigestFunction;
import build.buildfarm.cas.cfc.CASFileCache;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import javax.annotation.Nullable;
import lombok.extern.java.Log;

/**
 * Snapshot of a persistent worker's exec root state for incremental input updates. Stores entry
 * digests keyed by relative path for diffing against the next request's FetchResult.
 *
 * <p>Holds CAS references for the entries that persist across requests. When this state is replaced
 * or its worker is destroyed, close() decrements the carried refs. This ensures CAS entries backing
 * persistent symlinks/hardlinks are not evicted while in use.
 */
@Log
public class MaterializationState implements AutoCloseable {
  /** Result of diffing the previous state against a new FetchResult. */
  public record DiffResult(
      /** Entries in the new FetchResult but not in the previous state. */
      ImmutableList<FetchResult.Entry> added,
      /** Entries in the previous state but not in the new FetchResult. */
      ImmutableList<FetchResult.Entry> removed,
      /** New entries where the digest/type changed from the previous state. */
      ImmutableList<FetchResult.Entry> changed,
      /** Whether the descendedDirectories set changed (requires full re-materialization). */
      boolean descendedDirectoriesChanged) {}

  private final ImmutableMap<String, FetchResult.Entry> entriesByPath;
  private final ImmutableSet<String> descendedDirectories;
  private final ImmutableList<String> carriedRefKeys;
  private final ImmutableList<Digest> carriedRefDigests;
  private final DigestFunction.Value digestFunction;
  @Nullable private final CASFileCache fileCache;

  // Guarded by `this`. Decrement happens at most once; a failed decrement leaves this false so a
  // later close() can retry (matches FetchResult's OPEN -> CLOSED lifecycle).
  private boolean closed = false;

  private MaterializationState(
      ImmutableMap<String, FetchResult.Entry> entriesByPath,
      ImmutableSet<String> descendedDirectories,
      ImmutableList<String> carriedRefKeys,
      ImmutableList<Digest> carriedRefDigests,
      DigestFunction.Value digestFunction,
      @Nullable CASFileCache fileCache) {
    this.entriesByPath = entriesByPath;
    this.descendedDirectories = descendedDirectories;
    this.carriedRefKeys = carriedRefKeys;
    this.carriedRefDigests = carriedRefDigests;
    this.digestFunction = digestFunction;
    this.fileCache = fileCache;
  }

  /** Creates a ref-less snapshot from a FetchResult. Test-only; production carries CAS refs. */
  @VisibleForTesting
  static MaterializationState fromFetchResult(FetchResult fetchResult) {
    return new MaterializationState(
        indexEntries(fetchResult.entries(), ImmutableSet.of()),
        fetchResult.descendedDirectories(),
        ImmutableList.of(),
        ImmutableList.of(),
        DigestFunction.Value.UNKNOWN,
        null);
  }

  /**
   * Creates a snapshot from a FetchResult with explicitly supplied CAS refs. Test-only seam for
   * exercising close()/decrement behavior with injected ref lists; production uses {@link
   * #fromFetchResultWithRefs}.
   */
  @VisibleForTesting
  static MaterializationState fromFetchResult(
      FetchResult fetchResult,
      ImmutableList<String> carriedRefKeys,
      ImmutableList<Digest> carriedRefDigests,
      DigestFunction.Value digestFunction,
      CASFileCache fileCache) {
    if (!carriedRefKeys.isEmpty() || !carriedRefDigests.isEmpty()) {
      checkNotNull(fileCache, "fileCache required when carrying CAS refs");
    }
    return new MaterializationState(
        indexEntries(fetchResult.entries(), ImmutableSet.of()),
        fetchResult.descendedDirectories(),
        carriedRefKeys,
        carriedRefDigests,
        digestFunction,
        fileCache);
  }

  /**
   * Creates a snapshot carrying all CAS refs from the FetchResult. The caller must call
   * markTransferred() on the FetchResult to prevent double-decrement.
   */
  public static MaterializationState fromFetchResultWithRefs(FetchResult fetchResult) {
    return fromFetchResultWithRefs(fetchResult, ImmutableSet.of());
  }

  /**
   * Like {@link #fromFetchResultWithRefs(FetchResult)}, but omits from {@code entriesByPath} any
   * entry whose relative path is in {@code excludedPaths} — the paths {@link
   * ProtoCoordinator#moveOutputsToOperationRoot} moved out of the exec root as declared outputs.
   * Those links no longer exist on disk, so they must not appear in the diff baseline; omitting
   * them makes the next request's {@link #diff} classify them as {@code added} and re-link them.
   *
   * <p>CAS refs are kept FULL-SET (the excluded paths' refs are still carried and released on
   * replacement): refs are accounted independently of {@code entriesByPath}, and the omitted paths
   * are re-incremented and re-linked next request, so dropping their refs here would leak them.
   */
  public static MaterializationState fromFetchResultWithRefs(
      FetchResult fetchResult, ImmutableSet<String> excludedPaths) {
    if (!fetchResult.refKeys().isEmpty() || !fetchResult.refDigests().isEmpty()) {
      checkNotNull(fetchResult.fileCache(), "fileCache required when carrying CAS refs");
    }
    return new MaterializationState(
        indexEntries(fetchResult.entries(), excludedPaths),
        fetchResult.descendedDirectories(),
        fetchResult.refKeys(),
        fetchResult.refDigests(),
        fetchResult.digestFunction(),
        fetchResult.fileCache());
  }

  /**
   * Indexes entries by relative path, omitting any whose path is in {@code excludedPaths}. Uses
   * {@link Maps#uniqueIndex} so a duplicate relative path fails loudly rather than silently
   * dropping an entry.
   */
  private static ImmutableMap<String, FetchResult.Entry> indexEntries(
      ImmutableList<FetchResult.Entry> entries, ImmutableSet<String> excludedPaths) {
    ImmutableList<FetchResult.Entry> retained =
        excludedPaths.isEmpty()
            ? entries
            : entries.stream()
                .filter(entry -> !excludedPaths.contains(entry.relativePath()))
                .collect(ImmutableList.toImmutableList());
    return Maps.uniqueIndex(retained, FetchResult.Entry::relativePath);
  }

  /**
   * Decrements all carried CAS references. Idempotent — only the first successful call performs the
   * decrement. No-op when no refs are carried. If the decrement fails, stays open so a later call
   * can retry.
   */
  @Override
  public synchronized void close() {
    if (closed) {
      return;
    }

    if (fileCache != null && (!carriedRefKeys.isEmpty() || !carriedRefDigests.isEmpty())) {
      try {
        fileCache.decrementReferences(carriedRefKeys, carriedRefDigests, digestFunction);
      } catch (RuntimeException | IOException | InterruptedException e) {
        // decrementReferences can throw unchecked (e.g. IllegalStateException) when the backing CAS
        // entry is already gone or its refcount underflows. Swallow and log like the checked cases:
        // close() is best-effort and runs from teardown finally-blocks/loops (shutdown drain,
        // destroyObject, onTimeout) where throwing would strand sibling states. Stay open so a
        // later call can retry.
        log.log(Level.SEVERE, cleanupFailureMessage(), e);
        if (e instanceof InterruptedException) {
          Thread.currentThread().interrupt();
        }
        return;
      }
    }
    closed = true;
  }

  private String cleanupFailureMessage() {
    return "Failed to decrement carried CAS references during MaterializationState cleanup ("
        + carriedRefKeys.size()
        + " file refs, "
        + carriedRefDigests.size()
        + " directory refs)";
  }

  /**
   * Computes the diff between this (previous) state and a new FetchResult. If the
   * descendedDirectories set changed, the caller should fall back to full re-materialization.
   */
  public DiffResult diff(FetchResult newResult) {
    List<FetchResult.Entry> added = new ArrayList<>();
    List<FetchResult.Entry> removed = new ArrayList<>();
    List<FetchResult.Entry> changed = new ArrayList<>();

    boolean descendedDirectoriesChanged =
        !descendedDirectories.equals(newResult.descendedDirectories());

    // Single pass: find added, changed entries and collect new path set for removal detection
    Set<String> newPathSet = new HashSet<>();
    for (FetchResult.Entry newEntry : newResult.entries()) {
      newPathSet.add(newEntry.relativePath());
      FetchResult.Entry oldEntry = entriesByPath.get(newEntry.relativePath());
      if (oldEntry == null) {
        added.add(newEntry);
      } else if (!digestsMatch(oldEntry, newEntry)) {
        changed.add(newEntry);
      }
      // If digests match: unchanged, skip
    }

    // Find removed entries
    for (Map.Entry<String, FetchResult.Entry> oldEntry : entriesByPath.entrySet()) {
      if (!newPathSet.contains(oldEntry.getKey())) {
        removed.add(oldEntry.getValue());
      }
    }

    return new DiffResult(
        ImmutableList.copyOf(added),
        ImmutableList.copyOf(removed),
        ImmutableList.copyOf(changed),
        descendedDirectoriesChanged);
  }

  /** Checks if two entries have the same content (by type and digest/key/target). */
  private static boolean digestsMatch(FetchResult.Entry oldEntry, FetchResult.Entry newEntry) {
    if (oldEntry.type() != newEntry.type()) {
      return false;
    }
    return switch (oldEntry.type()) {
      case DIRECTORY -> {
        checkNotNull(
            oldEntry.digest(), "DIRECTORY entry missing digest: %s", oldEntry.relativePath());
        checkNotNull(
            newEntry.digest(), "DIRECTORY entry missing digest: %s", newEntry.relativePath());
        yield oldEntry.digest().equals(newEntry.digest());
      }
      case FILE -> {
        checkNotNull(oldEntry.key(), "FILE entry missing key: %s", oldEntry.relativePath());
        checkNotNull(newEntry.key(), "FILE entry missing key: %s", newEntry.relativePath());
        yield oldEntry.key().equals(newEntry.key());
      }
      case ZERO_SIZE_FILE -> true; // Always matches (no content)
      case SYMLINK_NODE -> {
        checkNotNull(
            oldEntry.symlinkTarget(),
            "SYMLINK_NODE entry missing target: %s",
            oldEntry.relativePath());
        checkNotNull(
            newEntry.symlinkTarget(),
            "SYMLINK_NODE entry missing target: %s",
            newEntry.relativePath());
        yield oldEntry.symlinkTarget().equals(newEntry.symlinkTarget());
      }
    };
  }

  @VisibleForTesting
  @Nullable
  FetchResult.Entry getEntry(String relativePath) {
    return entriesByPath.get(relativePath);
  }

  public ImmutableMap<String, FetchResult.Entry> entriesByPath() {
    return entriesByPath;
  }

  public ImmutableSet<String> descendedDirectories() {
    return descendedDirectories;
  }
}
