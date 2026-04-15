// Copyright 2023 The Buildfarm Authors. All rights reserved.
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

import static java.nio.file.StandardCopyOption.COPY_ATTRIBUTES;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import lombok.extern.java.Log;

/** Utility for concurrent move/copy/symlink of files. */
@Log
public final class FileAccessUtils {
  // singleton class with only static methods
  private FileAccessUtils() {}

  public static Path addPosixOwnerWrite(Path absPath) throws IOException {
    Set<PosixFilePermission> perms = Files.getPosixFilePermissions(absPath);

    ImmutableSet<PosixFilePermission> permsWithWrite =
        ImmutableSet.<PosixFilePermission>builder()
            .addAll(perms)
            .add(PosixFilePermission.OWNER_WRITE)
            .build();

    return Files.setAttribute(absPath, "posix:permissions", permsWithWrite);
  }

  private static final ConcurrentHashMap<Path, PathLock> fileLocks = new ConcurrentHashMap<>();

  // Used here as a simple lock for locking "files" (paths)
  private static final class PathLock {
    // Not used elsewhere
    private PathLock() {}
  }

  /**
   * Copies a file, creating necessary directories, replacing existing files. The resulting file is
   * set to be writeable, and we throw if we cannot set that. Thread-safe (within a process) against
   * writes to the same path.
   *
   * @param from
   * @param to
   * @throws IOException
   */
  public static void copyFile(Path from, Path to) throws IOException {
    Path absTo = to.toAbsolutePath();
    log.finer("copyFile: " + from + " to " + absTo);
    if (!Files.exists(from)) {
      throw new IOException("copyFile: source file doesn't exist: " + from);
    }
    IOException ioException =
        writeFileSafe(
            to,
            () -> {
              try {
                Files.copy(from, absTo, REPLACE_EXISTING, COPY_ATTRIBUTES);
                addPosixOwnerWrite(absTo);
                return null;
              } catch (IOException e) {
                return new IOException("copyFile() could not set writeable: " + absTo, e);
              }
            });
    if (ioException != null) {
      throw ioException;
    }
  }

  /**
   * Moves a file, creating necessary directories, replacing existing files. The resulting file is
   * set to be writeable, and we throw if we cannot set that. Thread-safe against writes to the same
   * path.
   *
   * @param from
   * @param to
   * @throws IOException
   */
  public static void moveFile(Path from, Path to) throws IOException {
    Path absTo = to.toAbsolutePath();
    log.finer("moveFile: " + from + " to " + absTo);
    if (!Files.exists(from)) {
      throw new IOException("moveFile: source file doesn't exist: " + from);
    }
    IOException ioException =
        writeFileSafe(
            absTo,
            () -> {
              try {
                Files.move(from, absTo, REPLACE_EXISTING);
                addPosixOwnerWrite(absTo);
                return null;
              } catch (IOException e) {
                return new IOException("moveFile() could not set writeable: " + absTo, e);
              }
            });
    if (ioException != null) {
      throw ioException;
    }
  }

  /**
   * Moves a directory and all its contents, creating necessary directories at the destination.
   * Individual files are moved using {@link #moveFile}, preserving thread-safety guarantees.
   *
   * @param from source directory
   * @param to destination directory
   * @throws IOException if the source doesn't exist or any file move fails
   */
  public static void moveDirectory(Path from, Path to) throws IOException {
    if (!Files.isDirectory(from)) {
      throw new IOException("moveDirectory: source is not a directory: " + from);
    }
    Files.walkFileTree(
        from,
        new SimpleFileVisitor<>() {
          @Override
          public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs)
              throws IOException {
            Files.createDirectories(to.resolve(from.relativize(dir)));
            return FileVisitResult.CONTINUE;
          }

          @Override
          public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
              throws IOException {
            moveFile(file, to.resolve(from.relativize(file)));
            return FileVisitResult.CONTINUE;
          }

          @Override
          public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
            if (exc != null) {
              throw exc;
            }
            deleteFileIfExists(dir);
            return FileVisitResult.CONTINUE;
          }
        });
  }

  /**
   * Creates a symbolic link, creating necessary parent directories. The link points to the target
   * path. Works for both file and directory targets. If the link path is already occupied (e.g. an
   * artifact a prior request left behind in a reused worker exec root), the existing
   * file/symlink/directory tree is removed and replaced. Thread-safe against writes to the same
   * path.
   *
   * @param target the file or directory to link to
   * @param link the symlink path to create
   * @throws IOException if symlink creation fails
   */
  public static void createSymlink(Path target, Path link) throws IOException {
    createLinkSafe(target, link, Files::createSymbolicLink, "createSymlink");
  }

  /**
   * Creates a hard link, creating necessary parent directories. If the link path is already
   * occupied (e.g. an artifact a prior request left behind in a reused worker exec root), the
   * existing file/symlink/directory tree is removed and replaced. Thread-safe against writes to the
   * same path.
   *
   * @param target the existing file to link to
   * @param link the hardlink path to create
   * @throws IOException if the target doesn't exist or hardlink creation fails
   */
  public static void createHardlink(Path target, Path link) throws IOException {
    createLinkSafe(target, link, Files::createLink, "createHardlink");
  }

  @FunctionalInterface
  private interface LinkCreator {
    void create(Path link, Path target) throws IOException;
  }

  @FunctionalInterface
  public interface FileCreator {
    void create(Path link) throws IOException;
  }

  /**
   * Creates a file or symlink at {@code link} via {@code creator}, creating necessary parent
   * directories. If the link path is already occupied (e.g. an artifact a prior request left behind
   * in a reused worker exec root), the existing file/symlink/directory tree is removed and
   * replaced. Like {@link #createSymlink}/{@link #createHardlink}, the create is attempted first
   * and the delete cost is paid only on a genuine collision, never on the common clear-path case.
   * Thread-safe against writes to the same path. Used for the entry types created directly via
   * {@code Files.*} (zero-size files and protobuf-declared symlink nodes), which have no CAS
   * target.
   *
   * @param link the path to create; the creator receives its absolute form
   * @param creator creates the entry at the (absolute) link path passed to it
   * @throws IOException if creation fails
   */
  public static void createReplacingOnConflict(Path link, FileCreator creator) throws IOException {
    Path absLink = link.toAbsolutePath();
    log.finer("createReplacingOnConflict: " + absLink);
    IOException ioException =
        writeFileSafe(
            absLink,
            () -> {
              try {
                creator.create(absLink);
                return null;
              } catch (FileAlreadyExistsException e) {
                // The path is occupied — typically an artifact a prior request left behind in this
                // reused worker exec root. Remove it and retry once. The worker is idle between
                // requests, so there is no concurrent reader of this path.
                try {
                  deletePathRecursive(absLink);
                  creator.create(absLink);
                  return null;
                } catch (IOException retryException) {
                  return new IOException(
                      "createReplacingOnConflict() failed replacing existing: " + absLink,
                      retryException);
                }
              } catch (IOException e) {
                return new IOException("createReplacingOnConflict() failed: " + absLink, e);
              }
            });
    if (ioException != null) {
      throw ioException;
    }
  }

  private static void createLinkSafe(Path target, Path link, LinkCreator creator, String methodName)
      throws IOException {
    Path absLink = link.toAbsolutePath();
    Path absTarget = target.toAbsolutePath();
    log.finer(methodName + ": " + absLink + " -> " + absTarget);
    IOException ioException =
        writeFileSafe(
            absLink,
            () -> {
              try {
                creator.create(absLink, absTarget);
                return null;
              } catch (FileAlreadyExistsException e) {
                // The link path is occupied — typically an artifact a prior request left behind in
                // this reused worker exec root. Remove it and retry once. We only pay this cost on
                // a genuine collision; the common clear-path case creates in a single syscall. The
                // worker is idle between requests, so there is no concurrent reader of this path.
                try {
                  deletePathRecursive(absLink);
                  creator.create(absLink, absTarget);
                  return null;
                } catch (IOException retryException) {
                  return new IOException(
                      methodName + "() failed replacing existing: " + absLink + " -> " + absTarget,
                      retryException);
                }
              } catch (IOException e) {
                return new IOException(
                    methodName + "() failed: " + absLink + " -> " + absTarget, e);
              }
            });
    if (ioException != null) {
      throw ioException;
    }
  }

  /**
   * Deletes whatever exists at {@code path} — a regular file, a symlink, or an entire directory
   * tree — without following symlinks and without creating parent directories. A no-op if the path
   * does not exist. Thread-safe against writes to the same path.
   *
   * @param path the path to remove
   * @throws IOException if deletion fails
   */
  public static void deleteExistingPath(Path path) throws IOException {
    Path absPath = path.toAbsolutePath();
    PathLock lock = fileLock(absPath);
    synchronized (lock) {
      try {
        deletePathRecursive(absPath);
      } finally {
        fileLocks.remove(absPath);
      }
    }
  }

  /**
   * Removes the file, symlink, or directory tree at {@code absPath} (a no-op if absent), never
   * following symlinks: a leftover symlink is unlinked, never resolved into its target (which for a
   * CAS-backed input would delete a shared CAS tree). The caller must already hold the path lock
   * for {@code absPath}.
   */
  private static void deletePathRecursive(Path absPath) throws IOException {
    if (Files.isDirectory(absPath, LinkOption.NOFOLLOW_LINKS)) {
      // A real directory (not a symlink to one): delete its contents bottom-up, then itself.
      // walkFileTree does not follow symlinks unless FOLLOW_LINKS is passed, so a symlink inside is
      // delivered to visitFile and unlinked, never descended into.
      Files.walkFileTree(
          absPath,
          new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                throws IOException {
              Files.deleteIfExists(file);
              return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc)
                throws IOException {
              if (exc != null) {
                throw exc;
              }
              Files.deleteIfExists(dir);
              return FileVisitResult.CONTINUE;
            }
          });
    } else {
      Files.deleteIfExists(absPath);
    }
  }

  /**
   * Deletes a file; Thread-safe against writes to the same path.
   *
   * @param toDelete
   * @throws IOException
   */
  public static void deleteFileIfExists(Path toDelete) throws IOException {
    Path absTo = toDelete.toAbsolutePath();
    PathLock toLock = fileLock(absTo);
    synchronized (toLock) {
      try {
        Files.deleteIfExists(absTo);
      } finally {
        fileLocks.remove(absTo);
      }
    }
  }

  /**
   * Thread-safe (not multi-process-safe) wrapper for locking paths before a write operation.
   *
   * <p>This method will create necessary parent directories.
   *
   * <p>It is up to the write operation to specify whether or not to overwrite existing files.
   */
  @SuppressWarnings("PMD.UnnecessaryLocalBeforeReturn")
  private static IOException writeFileSafe(Path absTo, Supplier<IOException> writeOp) {
    PathLock toLock = fileLock(absTo);
    synchronized (toLock) {
      try {
        // If 'absTo' is a symlink, checks if its target file exists
        Files.createDirectories(absTo.getParent());
        return writeOp.get();
      } catch (IOException e) {
        // PMD will complain about UnnecessaryLocalBeforeReturn
        // In this case, it is necessary to catch the exception
        return e;
      } finally {
        // Clean up to prevent too many locks.
        fileLocks.remove(absTo);
      }
    }
  }

  // "Logical" file lock
  private static PathLock fileLock(Path writeTo) {
    return fileLocks.computeIfAbsent(writeTo, k -> new PathLock());
  }
}
