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

package build.buildfarm.cas.cfc;

import java.io.IOException;
import org.jspecify.annotations.Nullable;

/**
 * Callback surface the {@link Evictor} uses to reach back into the cache for per-key-locked I/O,
 * storage lookup, and delegate-fallback uploads. Splits the concerns: {@code Evictor} owns the LRU
 * + bytes accounting + thread lifecycle; the backing implementation (today only {@link
 * CASFileCache}) owns the storage map, the file-system layout, and the delegate write integration.
 *
 * <p>All methods are called from the evictor thread or {@code asyncCleanupExecutor}-dispatched
 * tasks. Implementations must be thread-safe with respect to concurrent producer {@code charge} /
 * {@code recordAccess} / read-path callers — every method here corresponds to existing CFC code
 * that already had that property before the cutover.
 */
interface CasEvictorBacking {
  /**
   * Resolve a storage key to its {@link CASFileCache.Entry}, or null when absent. Used by the
   * evictor's accessEvents drain to re-position LRU entries. Should NOT take any lock — a
   * concurrent eviction race is acceptable and resolved by the drain-side state guard.
   */
  CASFileCache.@Nullable Entry lookupEntry(String key);

  /**
   * Run the per-key-locked atomic rename of {@code <key>} to {@code <key>_removed} (or {@code
   * Files.move} for {@code _dir} keys) and remove the entry from storage only after that detach
   * succeeds or the canonical path is already gone. Returns the removed Entry, or null when storage
   * no longer held one (treated as already-gone). The implementation must use the same per-key lock
   * that writer's {@code safeStorageInsertion} acquires, so that writer's {@code Files.createLink}
   * and the evictor's {@code createLink+delete} dance cannot interleave incoherently.
   */
  CASFileCache.@Nullable Entry safeStorageRemoval(String key) throws IOException;

  /**
   * Delete the {@code <key>_removed} sibling left behind by {@link #safeStorageRemoval}. Dispatched
   * by the evictor to its async-cleanup executor; on shutdown-race the evictor may run this inline.
   */
  void deleteExpiredKey(String key) throws IOException;

  /**
   * Synchronous portion of the delegate-fallback upload: open an InputStream against the
   * to-be-evicted file, hand it to {@code WritesHelper.streamIntoWriteFuture(...)}, return
   * promptly. The async byte-copy runs on the gRPC SerializingExecutor; this method does not await
   * its completion. Fire-and-forget — failures only surface in the per-key SEVERE log and the
   * {@code expire_entry_fallback_failure_total} counter.
   */
  void expireEntryFallback(String key, long size);

  /**
   * Invalidate any pending writes-in-progress future for this key's digest. Called only by the
   * evictor; runs on the evictor thread (the {@code RemovalListener} fires synchronously, so
   * implementations should ensure downstream listeners don't block the evictor sweep — e.g. by
   * dispatching the listener to a separate executor inside the CFC implementation).
   */
  void invalidateWriteForKey(String key, long size);

  /**
   * Race-loss recovery: a different {@link CASFileCache.Entry} now sits under {@code key} after the
   * evictor performed {@code safeStorageRemoval}. Restore it so its own state machine continues.
   * The evictor still discharges the victim's bytes itself.
   */
  void restoreEntryAfterRaceLoss(String key, CASFileCache.Entry replacement);

  /**
   * Return the on-disk byte cost of {@code entry} — the same value the producer-side {@link
   * CASFileCache#charge} added to the evictor's {@code totalBytes}. The evictor uses this on
   * discharge so accounting matches across the charge/discharge boundary. Default implementation
   * block-aligns the entry's logical size to the cache's {@code blockSize}.
   */
  long onDiskSize(CASFileCache.Entry entry);

  /**
   * Notify external listeners that the digest behind {@code key} has been fully evicted — i.e., no
   * other variant of the same digest (the flipped {@code _exec} suffix) remains in storage. A null
   * key or one that fails to parse is silently ignored.
   */
  void onDigestFullyExpired(String key, long size);

  /**
   * Subclass-level cleanup hook invoked by the evictor after {@link #safeStorageRemoval} on a
   * successfully-evicted Entry. Runs on the evictor thread.
   */
  void onEntryEvicted(CASFileCache.Entry entry);
}
