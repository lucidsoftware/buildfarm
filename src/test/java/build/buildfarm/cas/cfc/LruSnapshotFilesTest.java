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

import static com.google.common.truth.Truth.assertThat;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link LruSnapshotFiles} naming and parsing. */
@RunWith(JUnit4.class)
public class LruSnapshotFilesTest {
  @Test
  public void nameAndParseRoundTrip() {
    for (int n : new int[] {1, 2, 4, 32}) {
      for (int i = 0; i < n; i++) {
        String name = LruSnapshotFiles.name(n, i);
        assertThat(name).isEqualTo("lru_num_shards_" + n + "_shard_" + i + ".txt");
        LruSnapshotFiles.Parsed parsed = LruSnapshotFiles.parse(name);
        assertThat(parsed).isNotNull();
        assertThat(parsed.shardCount()).isEqualTo(n);
        assertThat(parsed.index()).isEqualTo(i);
      }
    }
  }

  @Test
  public void parseReturnsNullForNonSnapshotNames() {
    // Real CAS entry filenames and unrelated files must not parse as sharded snapshots.
    assertThat(LruSnapshotFiles.parse("lru.txt")).isNull(); // legacy is not the sharded form
    assertThat(LruSnapshotFiles.parse("0123abcd")).isNull(); // bare digest
    assertThat(LruSnapshotFiles.parse("0123abcd_dir")).isNull();
    assertThat(LruSnapshotFiles.parse("0123abcd_exec")).isNull();
    assertThat(LruSnapshotFiles.parse("lru_num_shards_4_shard_0.csv")).isNull(); // wrong extension
    assertThat(LruSnapshotFiles.parse("lru_num_shards_x_shard_0.txt")).isNull(); // non-numeric N
    // A tmp orphan is not itself a snapshot name.
    assertThat(LruSnapshotFiles.parse("lru_num_shards_4_shard_0.txt.tmp.abcd")).isNull();
  }

  @Test
  public void parseAcceptsZeroShardCountForCallerToReject() {
    // parse() is purely syntactic; the n <= 0 rejection lives in the loader, which WARN-and-skips.
    // num_shards_0 must still parse (not return null) so the loader can recognize and skip it
    // rather than mis-treating a nonsensical snapshot as not-a-snapshot.
    LruSnapshotFiles.Parsed parsed = LruSnapshotFiles.parse("lru_num_shards_0_shard_0.txt");
    assertThat(parsed).isNotNull();
    assertThat(parsed.shardCount()).isEqualTo(0);
  }

  @Test
  public void parseReturnsNullOnIntOverflow() {
    // A 10+ digit run matches \d+ but overflows int; parse() returns null (treat as not-a-snapshot)
    // rather than throwing and crashing the startup scan.
    assertThat(LruSnapshotFiles.parse("lru_num_shards_99999999999_shard_0.txt")).isNull();
    assertThat(LruSnapshotFiles.parse("lru_num_shards_4_shard_99999999999.txt")).isNull();
  }

  @Test
  public void isSnapshotMatchesLegacyAndSharded() {
    assertThat(LruSnapshotFiles.isSnapshot("lru.txt")).isTrue();
    assertThat(LruSnapshotFiles.isSnapshot("lru_num_shards_4_shard_2.txt")).isTrue();
    assertThat(LruSnapshotFiles.isSnapshot("0123abcd_dir")).isFalse();
  }

  @Test
  public void isSnapshotTmpOrphanMatchesShardedAndLegacyTmpFiles() {
    // AtomicFileWriter leaves <snapshot>.tmp.<uuid> orphans on a crash mid-rename.
    assertThat(LruSnapshotFiles.isSnapshotTmpOrphan("lru_num_shards_4_shard_0.txt.tmp.deadbeef"))
        .isTrue();
    assertThat(LruSnapshotFiles.isSnapshotTmpOrphan("lru.txt.tmp.deadbeef")).isTrue();
    // A tmp file of a non-snapshot is not a snapshot orphan.
    assertThat(LruSnapshotFiles.isSnapshotTmpOrphan("0123abcd_dir.tmp.deadbeef")).isFalse();
    // No .tmp. segment at all.
    assertThat(LruSnapshotFiles.isSnapshotTmpOrphan("lru_num_shards_4_shard_0.txt")).isFalse();
  }
}
