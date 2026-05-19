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

package build.buildfarm.common.io;

import static com.google.common.base.Preconditions.checkState;
import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A writer that atomically presents a target file only on successful close.
 *
 * <p>Writes are performed to a temporary file with a unique UUID suffix. On successful close(), the
 * temp file is atomically renamed onto the target via {@code Files.move(ATOMIC_MOVE,
 * REPLACE_EXISTING)} — a single syscall ({@code renameat2}) on POSIX filesystems. If the filesystem
 * does not support atomic rename, falls back to a non-atomic delete + createLink with a WARNING log
 * so operators see the reduced durability.
 *
 * <p>Note: ATOMIC_MOVE is <i>metadata-atomic</i>, not durable. The rename commits atomically in the
 * directory's inode but the temp file's data may still be in the page cache when the rename lands.
 * A JVM crash is safe (the data flushes asynchronously); a host crash (power loss / kernel panic)
 * between the rename and the page-cache flush can leave the target pointing at truncated data.
 * Callers that need host-crash durability must {@code fsync} the temp file and the parent directory
 * before close — this class does neither, since its current consumers (LRU snapshots) are
 * rebuildable from a filesystem scan.
 *
 * <p>Usage:
 *
 * <pre>{@code
 * try (AtomicFileWriter atomicWriter = new AtomicFileWriter(target)) {
 *   writer.write("data");
 * } // Automatic atomic swap and cleanup on close
 * }</pre>
 *
 * Note: This resource must be told, prior to the close block, that it was successfully completed,
 * in order to accomplish the file being presented at the target. This is because there is no
 * ability in the AutoClosable to detect the exceptional state of closure. If success is not
 * indicated with an 'onSuccess' call at the time of the first close call, the temp file will be
 * deleted and no interaction with the target will occur.
 *
 * <p>No thread safety of onSuccess() and close() is guaranteed.
 */
public class AtomicFileWriter extends BufferedWriter {
  private static final Logger log = Logger.getLogger(AtomicFileWriter.class.getName());

  private final Path target;
  private final Path temp;
  private boolean closed = false;
  private boolean success = false;

  private static Path createSiblingRandomUUIDTemp(Path target) {
    String suffix = UUID.randomUUID().toString();
    String filename = target.getFileName().toString();
    return target.resolveSibling(filename + ".tmp." + suffix);
  }

  /**
   * Creates an AtomicFileWriter for the specified target path.
   *
   * @param target the final destination path for the file
   * @throws IOException if the temporary file cannot be created
   */
  public AtomicFileWriter(Path target) throws IOException {
    this(target, createSiblingRandomUUIDTemp(target));
  }

  private AtomicFileWriter(Path target, Path temp) throws IOException {
    super(Files.newBufferedWriter(temp));
    checkState(!target.equals(temp));
    this.target = target;
    this.temp = temp;
  }

  public void onSuccess() {
    success = true;
  }

  /**
   * Closes the writer and atomically replaces the target file.
   *
   * <p>On success, the temp file is renamed onto the target via {@link
   * java.nio.file.StandardCopyOption#ATOMIC_MOVE}; on filesystems that do not support atomic
   * rename, falls back to a non-atomic delete + createLink. Either way the temp file no longer
   * exists at the end of a successful close. On failure (close threw, onSuccess was never called,
   * or the move/createLink threw), the temp file is removed.
   *
   * @throws IOException if an error occurs during the atomic swap
   */
  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }
    closed = true;

    try {
      // Close writer first
      super.close();
      // Only attempt atomic swap if writer closed successfully
      if (success) {
        replace();
      }
    } finally {
      // After a successful atomic move the temp no longer exists; deleteIfExists handles both
      // that case and the failure paths where the temp survived.
      Files.deleteIfExists(temp);
    }
  }

  private void replace() throws IOException {
    try {
      Files.move(temp, target, ATOMIC_MOVE, REPLACE_EXISTING);
    } catch (AtomicMoveNotSupportedException e) {
      // Filesystem doesn't support atomic rename (rare on POSIX; possible on some networked or
      // cross-volume Windows setups). Fall back to the non-atomic delete + createLink sequence.
      // A JVM crash between the delete and createLink leaves the target gone — log loudly so
      // operators see the reduced durability rather than discovering it after a crash.
      log.log(
          Level.WARNING,
          "atomic move not supported for " + target + "; falling back to non-atomic replace",
          e);
      try {
        Files.delete(target);
      } catch (NoSuchFileException nsfe) {
        // Ignore - file may not exist
      }
      Files.createLink(target, temp);
    }
  }
}
