// Copyright 2026 The Buildfarm Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package build.buildfarm.worker;

import build.bazel.remote.execution.v2.Compressor;
import build.bazel.remote.execution.v2.Directory;
import build.bazel.remote.execution.v2.DirectoryNode;
import build.bazel.remote.execution.v2.FileNode;
import build.bazel.remote.execution.v2.SymlinkNode;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.InputStreamFactory;
import build.buildfarm.v1test.Digest;
import com.google.protobuf.ByteString;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import lombok.extern.java.Log;

/** Controls the native Rust FUSE data plane. No FUSE callback executes in the JVM. */
@Log
public class FuseCAS {
  private static final int PROTOCOL_VERSION = 1;
  private static final int COMMAND_ADD_ROOT = 1;
  private static final int COMMAND_REMOVE_ROOT = 2;
  private static final int COMMAND_SHUTDOWN = 3;
  private static final int ENTRY_DIRECTORY = 1;
  private static final int ENTRY_FILE = 2;
  private static final int ENTRY_SYMLINK = 3;
  private static final int MAX_RESPONSE_BYTES = 16 * 1024 * 1024;
  private static final Path CONTAINER_BINARY = Path.of("/app/build_buildfarm/buildfarm-fuse");

  private record WireEntry(int kind, String path, boolean executable, long size, String value) {}

  private final Path mountPath;
  private final Path scratchPath;
  private final InputStreamFactory inputStreamFactory;
  private Process process;
  private DataInputStream fromRust;
  private DataOutputStream toRust;

  public FuseCAS(Path mountPath, InputStreamFactory inputStreamFactory) {
    this(
        mountPath,
        mountPath == null
            ? Path.of(System.getProperty("java.io.tmpdir"), "buildfarm-fuse-" + UUID.randomUUID())
            : mountPath.resolveSibling("fuse-scratch"),
        inputStreamFactory);
  }

  public FuseCAS(Path mountPath, Path scratchPath, InputStreamFactory inputStreamFactory) {
    this.mountPath = mountPath;
    this.scratchPath = scratchPath;
    this.inputStreamFactory = inputStreamFactory;
  }

  private static String configuredBinary() {
    String property = System.getProperty("buildfarm.fuse.path");
    if (property != null && !property.isBlank()) {
      return property;
    }
    String environment = System.getenv("BUILDFARM_FUSE_PATH");
    if (environment != null && !environment.isBlank()) {
      return environment;
    }
    if (Files.isExecutable(CONTAINER_BINARY)) {
      return CONTAINER_BINARY.toString();
    }
    String runfilesDir = System.getenv("RUNFILES_DIR");
    if (runfilesDir == null) {
      runfilesDir = System.getenv("TEST_SRCDIR");
    }
    if (runfilesDir != null) {
      for (String workspace : List.of("build_buildfarm", "_main")) {
        Path runfile =
            Path.of(
                runfilesDir,
                workspace,
                "src",
                "main",
                "rust",
                "fuse",
                "buildfarm-fuse");
        if (Files.isExecutable(runfile)) {
          return runfile.toString();
        }
      }
    }
    return "buildfarm-fuse";
  }

  private synchronized void ensureStarted() throws IOException {
    if (process != null && process.isAlive()) {
      return;
    }
    if (mountPath == null) {
      throw new IOException("a mount path is required for the Rust FUSE process");
    }
    Files.createDirectories(mountPath);
    Files.createDirectories(scratchPath);
    Process started =
        new ProcessBuilder(configuredBinary(), mountPath.toString(), scratchPath.toString())
            .redirectError(ProcessBuilder.Redirect.INHERIT)
            .start();
    DataInputStream input = new DataInputStream(new BufferedInputStream(started.getInputStream()));
    DataOutputStream output =
        new DataOutputStream(new BufferedOutputStream(started.getOutputStream()));
    process = started;
    fromRust = input;
    toRust = output;
    try {
      readResponse("start");
    } catch (IOException e) {
      started.destroyForcibly();
      clearProcess();
      throw e;
    }
  }

  private void clearProcess() {
    process = null;
    fromRust = null;
    toRust = null;
  }

  private void readResponse(String operation) throws IOException {
    int status;
    try {
      status = fromRust.readUnsignedByte();
    } catch (EOFException e) {
      int exit = process == null || process.isAlive() ? -1 : process.exitValue();
      throw new IOException("Rust FUSE process exited during " + operation + " (exit " + exit + ")", e);
    }
    int length = fromRust.readInt();
    if (length < 0 || length > MAX_RESPONSE_BYTES) {
      throw new IOException("invalid Rust FUSE response length: " + length);
    }
    byte[] message = fromRust.readNBytes(length);
    if (message.length != length) {
      throw new EOFException("truncated Rust FUSE response");
    }
    if (status != 0) {
      throw new IOException(operation + " failed: " + new String(message, StandardCharsets.UTF_8));
    }
  }

  private static void writeString(DataOutputStream output, String value) throws IOException {
    byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
    output.writeInt(bytes.length);
    output.write(bytes);
  }

  private synchronized void addRoot(String topdir, List<WireEntry> entries) throws IOException {
    ensureStarted();
    try {
      toRust.writeByte(COMMAND_ADD_ROOT);
      toRust.writeInt(PROTOCOL_VERSION);
      writeString(toRust, topdir);
      toRust.writeInt(entries.size());
      for (WireEntry entry : entries) {
        toRust.writeByte(entry.kind());
        writeString(toRust, entry.path());
        toRust.writeByte(entry.executable() ? 1 : 0);
        toRust.writeLong(entry.size());
        writeString(toRust, entry.value());
      }
      toRust.flush();
      readResponse("add root " + topdir);
    } catch (IOException e) {
      terminateFailedProcess();
      throw e;
    }
  }

  public void createInputRoot(
      String topdir,
      Digest inputRoot,
      Map<build.bazel.remote.execution.v2.Digest, Directory> directoriesIndex,
      BiFunction<Digest, Boolean, Path> resolver)
      throws IOException, InterruptedException {
    List<WireEntry> entries = new ArrayList<>();
    appendDirectory(
        Path.of(""), inputRoot, directoriesIndex, resolver, new HashSet<>(), entries);
    addRoot(topdir, entries);
  }

  private static void appendDirectory(
      Path path,
      Digest digest,
      Map<build.bazel.remote.execution.v2.Digest, Directory> directoriesIndex,
      BiFunction<Digest, Boolean, Path> resolver,
      Set<build.bazel.remote.execution.v2.Digest> ancestors,
      List<WireEntry> entries)
      throws IOException {
    build.bazel.remote.execution.v2.Digest reapiDigest = DigestUtil.toDigest(digest);
    Directory directory =
        digest.getSize() == 0 ? Directory.getDefaultInstance() : directoriesIndex.get(reapiDigest);
    if (directory == null) {
      throw new IOException("directory missing from input index: " + DigestUtil.toString(digest));
    }
    if (!ancestors.add(reapiDigest)) {
      throw new IOException("directory cycle in input index: " + DigestUtil.toString(digest));
    }
    try {
      for (DirectoryNode child : directory.getDirectoriesList()) {
        Path childPath = path.resolve(child.getName());
        entries.add(new WireEntry(ENTRY_DIRECTORY, childPath.toString(), false, 0, ""));
        appendDirectory(
            childPath,
            DigestUtil.fromDigest(child.getDigest(), digest.getDigestFunction()),
            directoriesIndex,
            resolver,
            ancestors,
            entries);
      }
      for (FileNode file : directory.getFilesList()) {
        Digest fileDigest = DigestUtil.fromDigest(file.getDigest(), digest.getDigestFunction());
        Path localPath =
            fileDigest.getSize() == 0 ? null : resolver.apply(fileDigest, file.getIsExecutable());
        if (fileDigest.getSize() != 0 && localPath == null) {
          throw new IOException(
              "input is not available in local CAS: " + DigestUtil.toString(fileDigest));
        }
        entries.add(
            new WireEntry(
                ENTRY_FILE,
                path.resolve(file.getName()).toString(),
                file.getIsExecutable(),
                fileDigest.getSize(),
                localPath == null ? "" : localPath.toString()));
      }
      for (SymlinkNode symlink : directory.getSymlinksList()) {
        entries.add(
            new WireEntry(
                ENTRY_SYMLINK,
                path.resolve(symlink.getName()).toString(),
                false,
                0,
                symlink.getTarget()));
      }
    } finally {
      ancestors.remove(reapiDigest);
    }
  }

  /** Supports the standalone mount tool by materializing its remotely streamed blobs first. */
  public void createInputRoot(String topdir, Digest inputRoot)
      throws IOException, InterruptedException {
    Map<build.bazel.remote.execution.v2.Digest, Directory> directories = new HashMap<>();
    loadDirectories(inputRoot, directories, new HashSet<>());
    Map<String, Path> materialized = new HashMap<>();
    createInputRoot(
        topdir,
        inputRoot,
        directories,
        (digest, executable) -> {
          if (digest.getSize() == 0) {
            return null;
          }
          String key = DigestUtil.toString(digest) + (executable ? "-x" : "");
          try {
            Path existing = materialized.get(key);
            if (existing != null) {
              return existing;
            }
            Path directory = scratchPath.resolve("standalone-inputs");
            Files.createDirectories(directory);
            Path path = Files.createTempFile(directory, "input-", null);
            try (InputStream input =
                inputStreamFactory.newInput(Compressor.Value.IDENTITY, digest, 0)) {
              Files.copy(input, path, StandardCopyOption.REPLACE_EXISTING);
            }
            if (executable) {
              path.toFile().setExecutable(true, true);
            }
            materialized.put(key, path);
            return path;
          } catch (IOException e) {
            return null;
          }
        });
  }

  private void loadDirectories(
      Digest digest,
      Map<build.bazel.remote.execution.v2.Digest, Directory> directories,
      Set<build.bazel.remote.execution.v2.Digest> ancestors)
      throws IOException, InterruptedException {
    if (digest.getSize() == 0) {
      return;
    }
    build.bazel.remote.execution.v2.Digest reapiDigest = DigestUtil.toDigest(digest);
    if (!ancestors.add(reapiDigest)) {
      throw new IOException("directory cycle: " + DigestUtil.toString(digest));
    }
    try {
      Directory directory;
      try (InputStream input =
          inputStreamFactory.newInput(Compressor.Value.IDENTITY, digest, 0)) {
        directory = Directory.parseFrom(ByteString.readFrom(input));
      }
      directories.put(reapiDigest, directory);
      for (DirectoryNode child : directory.getDirectoriesList()) {
        loadDirectories(
            DigestUtil.fromDigest(child.getDigest(), digest.getDigestFunction()),
            directories,
            ancestors);
      }
    } finally {
      ancestors.remove(reapiDigest);
    }
  }

  public synchronized void destroyInputRoot(String topdir)
      throws IOException, InterruptedException {
    if (process == null) {
      return;
    }
    try {
      toRust.writeByte(COMMAND_REMOVE_ROOT);
      writeString(toRust, topdir);
      toRust.flush();
      readResponse("remove root " + topdir);
    } catch (IOException e) {
      terminateFailedProcess();
      throw e;
    }
  }

  public synchronized void stop() {
    if (process == null) {
      return;
    }
    Process stopping = process;
    try {
      if (stopping.isAlive()) {
        toRust.writeByte(COMMAND_SHUTDOWN);
        toRust.flush();
        readResponse("shutdown");
      }
    } catch (IOException e) {
      log.warning("failed to shut down Rust FUSE process cleanly: " + e.getMessage());
    } finally {
      stopping.destroy();
      try {
        if (!stopping.waitFor(10, TimeUnit.SECONDS)) {
          stopping.destroyForcibly();
          stopping.waitFor(10, TimeUnit.SECONDS);
        }
      } catch (InterruptedException e) {
        stopping.destroyForcibly();
        Thread.currentThread().interrupt();
      }
      clearProcess();
    }
  }

  private void terminateFailedProcess() {
    if (process != null) {
      process.destroyForcibly();
      clearProcess();
    }
  }
}
