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

import build.bazel.remote.execution.v2.DigestFunction;
import build.buildfarm.cas.cfc.CASFileCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import javax.annotation.Nullable;
import lombok.extern.java.Log;

/**
 * Carries CAS paths, reference keys, and the descended-directory paths from InputFetcher to
 * ProtoCoordinator for persistent workers. Holds CAS references that must be decremented via
 * close().
 *
 * <p>Implements AutoCloseable with idempotent close() — multiple calls are safe after refs have
 * been decremented. If ref cleanup fails, the FetchResult stays open so a later cleanup site can
 * retry.
 */
@Log
public class FetchResult implements AutoCloseable {
  /** The type of entry in the FetchResult. */
  public enum EntryType {
    /** A directory symlinked to a CAS materialized tree. Has a CAS ref via digest. */
    DIRECTORY,
    /** A file hardlinked from CAS. Has a CAS ref via key. */
    FILE,
    /** A zero-size file. No CAS entry, no ref. Created via Files.createFile(). */
    ZERO_SIZE_FILE,
    /** A protobuf-declared symlink. No CAS ref. Created via Files.createSymbolicLink(). */
    SYMLINK_NODE
  }

  /** A single entry representing one input to materialize in the worker exec root. */
  public record Entry(
      String relativePath,
      EntryType type,
      @Nullable Path casPath,
      @Nullable String key,
      @Nullable build.bazel.remote.execution.v2.Digest digest,
      @Nullable String symlinkTarget) {}

  private final ImmutableList<Entry> entries;

  /**
   * Relative paths of directories the input walk kept as real directories (descended into) rather
   * than linking as a unit — including those excluded only via a recursive output root, which are
   * absent from the exclusion set's {@code paths()}. ProtoCoordinator removes these bottom-up
   * during cleanup once their contained links are deleted, so the reused worker exec root does not
   * accumulate stale directories that would collide with a later request's directory symlink.
   */
  private final ImmutableSet<String> descendedDirectories;

  private final ImmutableList<String> refKeys;
  private final ImmutableList<build.bazel.remote.execution.v2.Digest> refDigests;
  private final DigestFunction.Value digestFunction;
  private final CASFileCache fileCache;

  /**
   * CAS paths for tool inputs, keyed by relative path. Tool inputs are fetched into CAS and
   * ref-counted like regular inputs, but kept out of {@link #entries()} because they are managed
   * through the shared tool root mechanism (see {@link
   * ProtoCoordinator#copyToolInputsIntoWorkerToolRoot}), not linked directly into the worker exec
   * root.
   */
  private final ImmutableMap<String, Path> toolInputCasPaths;

  /**
   * Relative paths of zero-size tool inputs. These have no CAS entry and no ref, but must be
   * created as empty files in the worker tool root by {@link
   * ProtoCoordinator#copyToolInputsIntoWorkerToolRoot}. Kept separate from {@link
   * #toolInputCasPaths} because there is no CAS path to store.
   */
  private final ImmutableSet<String> zeroSizeToolInputPaths;

  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final AtomicBoolean closing = new AtomicBoolean(false);

  public FetchResult(
      ImmutableList<Entry> entries,
      ImmutableSet<String> descendedDirectories,
      ImmutableList<String> refKeys,
      ImmutableList<build.bazel.remote.execution.v2.Digest> refDigests,
      DigestFunction.Value digestFunction,
      CASFileCache fileCache,
      ImmutableMap<String, Path> toolInputCasPaths,
      ImmutableSet<String> zeroSizeToolInputPaths) {
    this.entries = entries;
    this.descendedDirectories = descendedDirectories;
    this.refKeys = refKeys;
    this.refDigests = refDigests;
    this.digestFunction = digestFunction;
    this.fileCache = fileCache;
    this.toolInputCasPaths = toolInputCasPaths;
    this.zeroSizeToolInputPaths = zeroSizeToolInputPaths;
  }

  public ImmutableList<Entry> entries() {
    return entries;
  }

  public ImmutableSet<String> descendedDirectories() {
    return descendedDirectories;
  }

  public ImmutableMap<String, Path> toolInputCasPaths() {
    return toolInputCasPaths;
  }

  public ImmutableSet<String> zeroSizeToolInputPaths() {
    return zeroSizeToolInputPaths;
  }

  /**
   * Decrements all CAS references held by this FetchResult. Idempotent after successful cleanup.
   * Failed cleanup attempts leave this FetchResult open for a later retry.
   */
  @Override
  public void close() {
    if (closed.get() || !closing.compareAndSet(false, true)) {
      return;
    }

    boolean wasInterrupted = false;
    try {
      if (!refKeys.isEmpty() || !refDigests.isEmpty()) {
        fileCache.decrementReferences(refKeys, refDigests, digestFunction);
      }
      closed.set(true);
    } catch (IOException e) {
      log.log(Level.SEVERE, cleanupFailureMessage(), e);
    } catch (InterruptedException e) {
      wasInterrupted = true;
      log.log(Level.SEVERE, cleanupFailureMessage(), e);
    } finally {
      closing.set(false);
      if (wasInterrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }

  private String cleanupFailureMessage() {
    return "Failed to decrement CAS references during FetchResult cleanup ("
        + refKeys.size()
        + " file refs, "
        + refDigests.size()
        + " directory refs)";
  }

  public boolean isClosed() {
    return closed.get();
  }
}
