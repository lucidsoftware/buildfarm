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

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.jspecify.annotations.Nullable;

/**
 * Naming and parsing for sharded LRU snapshot files. Each shard persists its LRU to {@code
 * lru_num_shards_<N>_shard_<I>.txt} at the cache root, where {@code N} is the shard count at save
 * time and {@code I} is the shard index. The filename is self-describing: a startup load at a
 * different {@code N} detects stale files by name alone, without reading content, and migrates them
 * (see {@link CasShards}). Underscores (not dots) separate the fields so the only dot is the
 * extension — extra dots in a basename trip up some operating systems and file browsers. The
 * pre-2.2 single-file form is {@code lru.txt} and is absorbed by hash-routing its entries across
 * the current shards.
 */
final class LruSnapshotFiles {
  /** The pre-2.2 single (unsharded) snapshot filename, absorbed on first sharded startup. */
  static final String LEGACY_NAME = "lru.txt";

  private static final Pattern SHARD_FILE =
      Pattern.compile("^lru_num_shards_(\\d+)_shard_(\\d+)\\.txt$");

  /** Parsed (shardCount, index) of a sharded snapshot filename. */
  record Parsed(int shardCount, int index) {}

  private LruSnapshotFiles() {}

  /** The snapshot filename for shard {@code index} of {@code shardCount}. */
  static String name(int shardCount, int index) {
    return "lru_num_shards_" + shardCount + "_shard_" + index + ".txt";
  }

  /** Parse a basename into (shardCount, index), or null when it is not a sharded snapshot name. */
  static @Nullable Parsed parse(String basename) {
    Matcher m = SHARD_FILE.matcher(basename);
    if (!m.matches()) {
      return null;
    }
    try {
      return new Parsed(Integer.parseInt(m.group(1)), Integer.parseInt(m.group(2)));
    } catch (NumberFormatException e) {
      // Overflow of a 10+ digit run that matched \d+ but exceeds int range — treat as
      // not-a-snapshot so the caller leaves it untouched rather than crashing the scan.
      return null;
    }
  }

  /** True when {@code basename} is the legacy single-file snapshot or any sharded snapshot. */
  static boolean isSnapshot(String basename) {
    return basename.equals(LEGACY_NAME) || parse(basename) != null;
  }

  /**
   * True when {@code basename} is an {@link build.buildfarm.common.io.AtomicFileWriter} temp orphan
   * of a snapshot file — {@code <snapshot>.tmp.<uuid>} left behind by a crash mid-rename.
   */
  static boolean isSnapshotTmpOrphan(String basename) {
    int tmp = basename.indexOf(".tmp.");
    return tmp > 0 && isSnapshot(basename.substring(0, tmp));
  }
}
