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

import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.DigestFunction;
import build.buildfarm.cas.cfc.CASFileCache;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.nio.file.Path;
import java.util.logging.Level;
import javax.annotation.Nullable;
import lombok.extern.java.Log;

/**
 * Carries CAS paths, reference keys, and the descended-directory paths from InputFetcher to
 * ProtoCoordinator for persistent workers. Holds CAS references that must be decremented via
 * close().
 *
 * <p>Lifecycle: OPEN → TRANSFERRED (refs handed to MaterializationState) or CLOSED (refs
 * decremented). Both are terminal states. close() only decrements from OPEN. If ref cleanup fails,
 * the FetchResult stays OPEN so a later cleanup site can retry.
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
      @Nullable Digest digest,
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
  private final ImmutableList<Digest> refDigests;
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

  /** Lifecycle states for CAS ref ownership. */
  enum State {
    OPEN,
    TRANSFERRED,
    CLOSED
  }

  private State state = State.OPEN;

  public FetchResult(
      ImmutableList<Entry> entries,
      ImmutableSet<String> descendedDirectories,
      ImmutableList<String> refKeys,
      ImmutableList<Digest> refDigests,
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

  public ImmutableList<String> refKeys() {
    return refKeys;
  }

  public ImmutableList<Digest> refDigests() {
    return refDigests;
  }

  public DigestFunction.Value digestFunction() {
    return digestFunction;
  }

  public CASFileCache fileCache() {
    return fileCache;
  }

  public ImmutableMap<String, Path> toolInputCasPaths() {
    return toolInputCasPaths;
  }

  public ImmutableSet<String> zeroSizeToolInputPaths() {
    return zeroSizeToolInputPaths;
  }

  /**
   * Decrements all CAS references held by this FetchResult. Only succeeds when transitioning from
   * OPEN to CLOSED. No-op if already TRANSFERRED or CLOSED.
   */
  @Override
  public synchronized void close() {
    if (state != State.OPEN) {
      return;
    }

    if (!refKeys.isEmpty() || !refDigests.isEmpty()) {
      try {
        fileCache.decrementReferences(refKeys, refDigests, digestFunction);
      } catch (RuntimeException | IOException | InterruptedException e) {
        // decrementReferences can throw unchecked (e.g. IllegalStateException) when the backing CAS
        // entry is already gone or its refcount underflows. Swallow and log like the checked cases:
        // close() is best-effort and runs from teardown finally-blocks/loops where throwing would
        // strand sibling cleanups. Stay OPEN so a later cleanup site can retry.
        log.log(Level.SEVERE, cleanupFailureMessage(), e);
        if (e instanceof InterruptedException) {
          Thread.currentThread().interrupt();
        }
        return;
      }
    }
    state = State.CLOSED;
  }

  private String cleanupFailureMessage() {
    return "Failed to decrement CAS references during FetchResult cleanup ("
        + refKeys.size()
        + " file refs, "
        + refDigests.size()
        + " directory refs)";
  }

  /**
   * Marks this FetchResult as transferred — the caller has taken ownership of the CAS refs (e.g.,
   * stored them in a MaterializationState). After this call, close() becomes a no-op — it will NOT
   * decrement refs. Returns false if close() already ran (refs already decremented), in which case
   * the caller must not assume ref ownership.
   */
  public synchronized boolean markTransferred() {
    if (state != State.OPEN) {
      return false;
    }
    state = State.TRANSFERRED;
    return true;
  }

  /** Returns true if this FetchResult has reached a terminal state (TRANSFERRED or CLOSED). */
  @VisibleForTesting
  public synchronized boolean isTerminal() {
    return state != State.OPEN;
  }
}
