// Copyright 2018 The Buildfarm Authors. All rights reserved.
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

package build.buildfarm.worker;

import build.bazel.remote.execution.v2.Compressor;
import build.bazel.remote.execution.v2.DigestFunction;
import build.bazel.remote.execution.v2.Directory;
import build.bazel.remote.execution.v2.DirectoryNode;
import build.bazel.remote.execution.v2.FileNode;
import build.bazel.remote.execution.v2.SymlinkNode;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.InputStreamFactory;
import build.buildfarm.common.Watchdog;
import build.buildfarm.v1test.Digest;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.protobuf.Duration;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.logging.Level;
import jnr.constants.platform.Access;
import jnr.constants.platform.OpenFlags;
import jnr.ffi.Pointer;
import jnr.ffi.types.gid_t;
import jnr.ffi.types.mode_t;
import jnr.ffi.types.off_t;
import jnr.ffi.types.size_t;
import jnr.ffi.types.uid_t;
import lombok.extern.java.Log;
import ru.serce.jnrfuse.ErrorCodes;
import ru.serce.jnrfuse.FuseException;
import ru.serce.jnrfuse.FuseFillDir;
import ru.serce.jnrfuse.FuseStubFS;
import ru.serce.jnrfuse.struct.FileStat;
import ru.serce.jnrfuse.struct.FuseFileInfo;
import ru.serce.jnrfuse.struct.Statvfs;
import ru.serce.jnrfuse.struct.Timespec;

@Log
public class FuseCAS extends FuseStubFS {
  @FunctionalInterface
  interface LegacyInputStreamFactory {
    InputStream newInput(build.bazel.remote.execution.v2.Digest digest, long offset)
        throws IOException;
  }

  private final Path mountPath;
  private final Path scratchPath;
  private final InputStreamFactory inputStreamFactory;
  private final DirectoryEntry root;
  private final AtomicInteger fileHandleCounter = new AtomicInteger(1);
  private final Map<Integer, Entry> fileHandleEntries = new ConcurrentHashMap<>();
  private final Map<Digest, Map<String, Entry>> childrenCache = new ConcurrentHashMap<>();

  private transient boolean mounted = false;
  private transient long mounts = 0;
  private Watchdog unmounter;

  interface Entry {
    boolean isSymlink();

    boolean isDirectory();

    boolean isWritable();

    boolean isExecutable();

    long size();

    default int linkCount() {
      return 1;
    }
  }

  static class FileEntry implements Entry {
    final Digest digest;
    final boolean executable;
    final Path path;

    FileEntry(Digest digest, boolean executable) {
      this(digest, executable, null);
    }

    FileEntry(Digest digest, boolean executable, Path path) {
      this.digest = digest;
      this.executable = executable;
      this.path = path;
    }

    @Override
    public boolean isSymlink() {
      return false;
    }

    @Override
    public boolean isDirectory() {
      return false;
    }

    @Override
    public boolean isWritable() {
      return false;
    }

    @Override
    public boolean isExecutable() {
      return executable;
    }

    @Override
    public long size() {
      return digest.getSize();
    }
  }

  static class WriteFileEntry implements Entry {
    boolean executable;
    final Path path;
    private int openHandles = 0;
    private int links = 1;
    private boolean deletePending = false;

    WriteFileEntry(boolean executable, Path path) {
      this.executable = executable;
      this.path = path;
    }

    @Override
    public boolean isSymlink() {
      return false;
    }

    @Override
    public boolean isDirectory() {
      return false;
    }

    @Override
    public boolean isWritable() {
      return true;
    }

    @Override
    public boolean isExecutable() {
      return executable;
    }

    @Override
    public long size() {
      try {
        return Files.size(path);
      } catch (IOException e) {
        return 0;
      }
    }

    synchronized void opened() {
      openHandles++;
    }

    synchronized void released() {
      if (openHandles > 0) {
        openHandles--;
      }
      deleteIfUnused();
    }

    synchronized void linked() {
      links++;
    }

    synchronized void unlinked() {
      if (links > 0) {
        links--;
      }
      deletePending = links == 0;
      deleteIfUnused();
    }

    @Override
    public synchronized int linkCount() {
      return links;
    }

    private void deleteIfUnused() {
      if (deletePending && openHandles == 0) {
        try {
          Files.deleteIfExists(path);
        } catch (IOException e) {
          log.log(Level.WARNING, "could not delete FUSE output backing file " + path, e);
        }
      }
    }
  }

  class LocalDirectoryEntry extends DirectoryEntry {
    private final Map<String, Entry> children;

    LocalDirectoryEntry() {
      this.children = new ConcurrentHashMap<>();
    }

    LocalDirectoryEntry(Map<String, Entry> children) {
      this.children = new ConcurrentHashMap<>(children);
    }

    @Override
    public boolean isMutable() {
      return true;
    }

    @Override
    protected Map<String, Entry> getChildren() {
      return children;
    }
  }

  abstract class DirectoryEntry implements Entry {
    @Override
    public boolean isSymlink() {
      return false;
    }

    @Override
    public boolean isDirectory() {
      return true;
    }

    @Override
    public boolean isWritable() {
      return true;
    }

    @Override
    public boolean isExecutable() {
      return true;
    }

    @Override
    public long size() {
      return 0;
    }

    public abstract boolean isMutable();

    protected abstract Map<String, Entry> getChildren();

    public synchronized void forAllChildren(Consumer<String> onChild) {
      for (String child : getChildren().keySet()) {
        onChild.accept(child);
      }
    }

    public Entry getChild(String name) {
      return getChildren().get(name);
    }

    public void putChild(String name, Entry entry) {
      getChildren().put(name, entry);
    }

    public void removeChild(String name) {
      getChildren().remove(name);
    }

    public boolean hasChild(String name) {
      return getChildren().containsKey(name);
    }

    public synchronized DirectoryEntry subdir(String name) {
      Entry e = getChild(name);
      if (e == null) {
        e = new LocalDirectoryEntry();
        putChild(name, e);
      }
      return e.isDirectory() ? (DirectoryEntry) e : null;
    }
  }

  @FunctionalInterface
  interface DirectoryEntryChildrenFetcher {
    Map<String, Entry> apply(DirectoryEntry entry) throws IOException, InterruptedException;
  }

  class CASDirectoryEntry extends DirectoryEntry {
    private final DirectoryEntryChildrenFetcher childrenFetcher;

    CASDirectoryEntry(DirectoryEntryChildrenFetcher childrenFetcher) {
      this.childrenFetcher = childrenFetcher;
    }

    @Override
    public boolean isMutable() {
      return false;
    }

    @Override
    protected Map<String, Entry> getChildren() {
      try {
        return childrenFetcher.apply(this);
      } catch (InterruptedException | IOException e) {
        return Map.of();
      }
    }

    @Override
    public void putChild(String name, Entry entry) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void removeChild(String name) {
      throw new UnsupportedOperationException();
    }
  }

  static class SymlinkEntry implements Entry {
    final String target;

    SymlinkEntry(String target) {
      this.target = target;
    }

    @Override
    public boolean isSymlink() {
      return true;
    }

    @Override
    public boolean isDirectory() {
      return false;
    }

    @Override
    public boolean isWritable() {
      return false;
    }

    @Override
    public boolean isExecutable() {
      return false;
    }

    @Override
    public long size() {
      return target.getBytes(StandardCharsets.UTF_8).length;
    }
  }

  public FuseCAS(Path mountPath, InputStreamFactory inputStreamFactory) {
    this(
        mountPath,
        mountPath == null
            ? Path.of(System.getProperty("java.io.tmpdir"), "buildfarm-fuse-" + UUID.randomUUID())
            : mountPath.resolveSibling("fuse-scratch"),
        inputStreamFactory);
  }

  FuseCAS(Path mountPath, LegacyInputStreamFactory inputStreamFactory) {
    this(
        mountPath,
        (compressor, digest, offset) ->
            inputStreamFactory.newInput(DigestUtil.toDigest(digest), offset));
  }

  public FuseCAS(Path mountPath, Path scratchPath, InputStreamFactory inputStreamFactory) {
    this.mountPath = mountPath;
    this.scratchPath = scratchPath;
    this.inputStreamFactory = inputStreamFactory;
    root = new LocalDirectoryEntry();
  }

  public synchronized void stop() {
    if (unmounter != null) {
      unmounter.stop();
      unmounter = null;
    }
    if (mounted) {
      umount();
      mounted = false;
    }
    mounts = 0;
  }

  @FunctionalInterface
  interface DirectoryEntryPathConsumer {
    void accept(DirectoryEntry entry, String path) throws IOException;
  }

  private static void resolveTopdir(
      String topdir, DirectoryEntry root, DirectoryEntryPathConsumer onEntry)
      throws IOException, InterruptedException {
    String[] components = topdir.split("/");
    int baseIndex = components.length - 1;
    while (baseIndex >= 0 && components[baseIndex].isEmpty()) {
      baseIndex--;
    }
    if (baseIndex < 0) {
      throw new IllegalArgumentException("Cannot reference an inputRoot with empty root");
    }
    DirectoryEntry currentDir = root;
    for (int i = 0; currentDir != null && i < baseIndex; i++) {
      if (components[i].isEmpty()) {
        continue;
      }
      currentDir = currentDir.subdir(components[i]);
    }
    if (currentDir == null) {
      throw new IllegalArgumentException("Not a directory");
    }
    onEntry.accept(currentDir, components[baseIndex]);
  }

  private synchronized void incMounts() throws IOException {
    if (mounts > 0 || mountPath == null) {
      mounts++;
      return;
    }
    if (unmounter != null) {
      unmounter.stop();
      unmounter = null;
    }
    if (!mounted) {
      log.log(Level.INFO, "Mounting FuseCAS");
      String[] fuseOpts = {"-o", "max_write=131072", "-o", "big_writes"};
      try {
        mount(mountPath, /* blocking= */ false, /* debug= */ false, /* fuseOpts= */ fuseOpts);
      } catch (FuseException e) {
        throw new IOException(e);
      }
      mounted = true;
    }
    mounts = 1;
  }

  private synchronized void decMounts() {
    if (mounts == 0) {
      return;
    }
    if (--mounts == 0 && mountPath != null) {
      log.log(Level.INFO, "Scheduling FuseCAS unmount in 10s");
      unmounter =
          new Watchdog(Duration.newBuilder().setSeconds(10).setNanos(0).build(), this::unmountIdle);
      Thread unmountThread = new Thread(unmounter, "fuse-cas-unmounter");
      unmountThread.setDaemon(true);
      unmountThread.start();
    }
  }

  private synchronized void unmountIdle() {
    if (mounts == 0 && mounted) {
      log.log(Level.INFO, "Unmounting FuseCAS");
      umount();
      mounted = false;
      unmounter = null;
    }
  }

  private Map<String, Entry> fetchChildren(Digest digest) throws IOException, InterruptedException {
    Map<String, Entry> children = childrenCache.get(digest);
    if (children == null) {
      try {
        Directory directory =
            Directory.parseFrom(
                ByteString.readFrom(
                    inputStreamFactory.newInput(Compressor.Value.IDENTITY, digest, 0)));

        ImmutableMap.Builder<String, Entry> builder = new ImmutableMap.Builder<>();

        for (FileNode fileNode : directory.getFilesList()) {
          builder.put(
              fileNode.getName(),
              new FileEntry(
                  DigestUtil.fromDigest(fileNode.getDigest(), digest.getDigestFunction()),
                  fileNode.getIsExecutable()));
        }
        for (DirectoryNode directoryNode : directory.getDirectoriesList()) {
          builder.put(
              directoryNode.getName(),
              new CASDirectoryEntry(
                  fetchChildrenFunction(
                      DigestUtil.fromDigest(
                          directoryNode.getDigest(), digest.getDigestFunction()))));
        }
        for (SymlinkNode symlinkNode : directory.getSymlinksList()) {
          builder.put(symlinkNode.getName(), new SymlinkEntry(symlinkNode.getTarget()));
        }

        children = builder.build();
        childrenCache.put(digest, children);
      } catch (InvalidProtocolBufferException e) {
        log.log(Level.SEVERE, "error parsing directory " + DigestUtil.toString(digest), e);
      }
    }
    return children;
  }

  private DirectoryEntryChildrenFetcher fetchChildrenFunction(Digest digest) {
    return (dirEntry) -> fetchChildren(digest);
  }

  public void createInputRoot(String topdir, Digest inputRoot)
      throws IOException, InterruptedException {
    incMounts();
    boolean success = false;
    try {
      resolveTopdir(
          topdir,
          root,
          (currentDir, base) -> {
            if (currentDir.hasChild(base)) {
              throw new IOException("input root already exists: " + topdir);
            }
            currentDir.putChild(base, new CASDirectoryEntry(fetchChildrenFunction(inputRoot)));
          });
      success = true;
    } finally {
      if (!success) {
        decMounts();
      }
    }
  }

  void createInputRoot(String topdir, build.bazel.remote.execution.v2.Digest inputRoot)
      throws IOException, InterruptedException {
    createInputRoot(topdir, DigestUtil.fromDigest(inputRoot, DigestFunction.Value.SHA256));
  }

  /**
   * Installs an input tree whose files have already been placed and referenced in the local CAS.
   * The resolver must return the exact local path for a digest/executable pair. Directory parsing
   * and blob fetching are therefore never performed from a FUSE callback.
   */
  public void createInputRoot(
      String topdir,
      Digest inputRoot,
      Map<build.bazel.remote.execution.v2.Digest, Directory> directoriesIndex,
      BiFunction<Digest, Boolean, Path> resolver)
      throws IOException, InterruptedException {
    Map<String, Entry> children =
        indexedChildren(inputRoot, directoriesIndex, resolver, new HashSet<>());
    incMounts();
    boolean success = false;
    try {
      resolveTopdir(
          topdir,
          root,
          (currentDir, base) -> {
            if (currentDir.hasChild(base)) {
              throw new IOException("input root already exists: " + topdir);
            }
            currentDir.putChild(base, new LocalDirectoryEntry(children));
          });
      success = true;
    } finally {
      if (!success) {
        decMounts();
      }
    }
  }

  private Map<String, Entry> indexedChildren(
      Digest digest,
      Map<build.bazel.remote.execution.v2.Digest, Directory> directoriesIndex,
      BiFunction<Digest, Boolean, Path> resolver,
      Set<build.bazel.remote.execution.v2.Digest> ancestors)
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
      Map<String, Entry> children = new HashMap<>();
      for (FileNode file : directory.getFilesList()) {
        Digest fileDigest = DigestUtil.fromDigest(file.getDigest(), digest.getDigestFunction());
        Path localPath =
            fileDigest.getSize() == 0 ? null : resolver.apply(fileDigest, file.getIsExecutable());
        if (fileDigest.getSize() != 0 && localPath == null) {
          throw new IOException(
              "input is not available in local CAS: " + DigestUtil.toString(fileDigest));
        }
        children.put(file.getName(), new FileEntry(fileDigest, file.getIsExecutable(), localPath));
      }
      for (DirectoryNode child : directory.getDirectoriesList()) {
        Digest childDigest = DigestUtil.fromDigest(child.getDigest(), digest.getDigestFunction());
        children.put(
            child.getName(),
            new LocalDirectoryEntry(
                indexedChildren(childDigest, directoriesIndex, resolver, ancestors)));
      }
      for (SymlinkNode symlink : directory.getSymlinksList()) {
        children.put(symlink.getName(), new SymlinkEntry(symlink.getTarget()));
      }
      return children;
    } finally {
      ancestors.remove(reapiDigest);
    }
  }

  public void destroyInputRoot(String topdir) throws IOException, InterruptedException {
    AtomicBoolean removed = new AtomicBoolean(false);
    resolveTopdir(
        topdir,
        root,
        (parent, name) -> {
          Entry entry = parent.getChild(name);
          if (entry != null) {
            parent.removeChild(name);
            cleanupEntry(entry);
            removed.set(true);
          }
        });
    if (removed.get()) {
      decMounts();
    }
  }

  private static void cleanupEntry(Entry entry) {
    if (entry == null) {
      return;
    }
    if (entry instanceof WriteFileEntry writeFile) {
      writeFile.unlinked();
    } else if (entry instanceof LocalDirectoryEntry directory) {
      for (Entry child : directory.getChildren().values()) {
        cleanupEntry(child);
      }
    }
  }

  private DirectoryEntry containingDirectoryForCreate(String path) {
    int endIndex = path.lastIndexOf('/');
    if (endIndex == 0) {
      endIndex = 1;
    }
    return directoryForCreate(path.substring(0, endIndex));
  }

  private Entry resolve(String path) {
    Entry entry = root;
    if (path.equals("/")) {
      return entry;
    }
    if (path.isEmpty() || path.charAt(0) != '/') {
      throw new IllegalArgumentException("cannot resolve empty/relative paths");
    }
    for (String component : path.substring(1).split("/")) {
      if (!entry.isDirectory()) {
        return null;
      }
      DirectoryEntry dirEntry = (DirectoryEntry) entry;
      entry = dirEntry.getChild(component);
      if (entry == null) {
        return null;
      }
    }
    return entry;
  }

  private DirectoryEntry directoryForPath(String path) {
    Entry entry = resolve(path);
    if (entry != null && entry.isDirectory()) {
      return (DirectoryEntry) entry;
    }
    return null;
  }

  private DirectoryEntry directoryForCreate(String path) {
    if (path.equals("/")) {
      return root;
    }
    if (path.isEmpty() || path.charAt(0) != '/') {
      throw new IllegalArgumentException("cannot resolve empty/relative paths");
    }
    DirectoryEntry dirEntry = root;
    for (String component : path.substring(1).split("/")) {
      Entry entry = dirEntry.getChild(component);
      if (entry == null || !entry.isDirectory()) {
        return null;
      }
      DirectoryEntry childDirEntry = (DirectoryEntry) entry;
      if (!childDirEntry.isMutable()) {
        childDirEntry = new LocalDirectoryEntry(childDirEntry.getChildren());
        dirEntry.putChild(component, childDirEntry);
      }
      dirEntry = childDirEntry;
    }
    return dirEntry;
  }

  private String basename(String path) {
    return path.substring(path.lastIndexOf('/') + 1);
  }

  @SuppressWarnings("OctalInteger")
  @Override
  public int getattr(String path, FileStat stat) {
    Entry entry = resolve(path);

    if (entry == null) {
      return -ErrorCodes.ENOENT();
    }

    // stock block size
    stat.st_blksize.set(4096);

    if (entry.isSymlink()) {
      stat.st_mode.set(FileStat.S_IFLNK | 0777);
    } else if (entry.isDirectory()) {
      stat.st_mode.set(FileStat.S_IFDIR | 0755);
    } else {
      int mode;
      if (entry.isWritable()) {
        mode = entry.isExecutable() ? 0755 : 0644;
      } else {
        mode = entry.isExecutable() ? 0555 : 0444;
      }
      stat.st_mode.set(FileStat.S_IFREG | mode);
      stat.st_nlink.set(entry.linkCount());
    }
    long size = entry.size();
    long blksize = stat.st_blksize.get();
    long blocks = (size + blksize - 1) / blksize;
    stat.st_size.set(size);
    stat.st_blocks.set(blocks);
    return 0;
  }

  @Override
  public int readlink(String path, Pointer buf, @size_t long size) {
    Entry entry = resolve(path);

    if (entry == null) {
      return -ErrorCodes.ENOENT();
    }

    if (!entry.isSymlink()) {
      return -ErrorCodes.EINVAL();
    }

    SymlinkEntry symlinkEntry = (SymlinkEntry) entry;
    byte[] target = symlinkEntry.target.getBytes(StandardCharsets.UTF_8);
    int putsize = (int) Math.min(Math.max(0, size - 1), target.length);
    buf.put(0, target, 0, putsize);
    if (size > 0) {
      buf.putByte(putsize, (byte) 0);
    }
    return 0;
  }

  @Override
  public int symlink(String oldpath, String newpath) {
    DirectoryEntry dirEntry = containingDirectoryForCreate(newpath);

    if (dirEntry == null) {
      return -ErrorCodes.ENOENT();
    }

    String base = basename(newpath);
    if (dirEntry.hasChild(base)) {
      return -ErrorCodes.EEXIST();
    }
    dirEntry.putChild(base, new SymlinkEntry(oldpath));

    return 0;
  }

  @Override
  public int rename(String oldpath, String newpath) {
    if (oldpath.equals(newpath)) {
      return 0;
    }
    DirectoryEntry oldDirEntry = containingDirectoryForCreate(oldpath);
    DirectoryEntry newDirEntry = containingDirectoryForCreate(newpath);

    if (oldDirEntry == null || newDirEntry == null) {
      return -ErrorCodes.ENOENT();
    }

    // FIXME make this atomic
    String oldBase = basename(oldpath);
    String newBase = basename(newpath);
    Entry entry = oldDirEntry.getChild(oldBase);
    if (entry == null) {
      return -ErrorCodes.ENOENT();
    }
    Entry replaced = newDirEntry.getChild(newBase);
    if (replaced != null && replaced != entry) {
      cleanupEntry(replaced);
    }
    newDirEntry.putChild(newBase, entry);
    oldDirEntry.removeChild(oldBase);

    return 0;
  }

  @Override
  public int link(String oldpath, String newpath) {
    Entry entry = resolve(oldpath);
    if (entry == null) {
      return -ErrorCodes.ENOENT();
    }
    if (entry.isDirectory()) {
      return -ErrorCodes.EPERM();
    }
    DirectoryEntry newParent = containingDirectoryForCreate(newpath);
    if (newParent == null) {
      return -ErrorCodes.ENOENT();
    }
    String newBase = basename(newpath);
    if (newParent.hasChild(newBase)) {
      return -ErrorCodes.EEXIST();
    }
    if (entry instanceof WriteFileEntry writeFile) {
      writeFile.linked();
    }
    newParent.putChild(newBase, entry);
    return 0;
  }

  @Override
  public int chown(String path, @uid_t long uid, @gid_t long gid) {
    Entry entry = resolve(path);

    if (entry == null) {
      return -ErrorCodes.ENOENT();
    }

    if (uid == -1 && gid == -1) {
      return 0;
    }

    return -ErrorCodes.EPERM();
  }

  @Override
  public int truncate(String path, @off_t long size) {
    Entry entry = resolve(path);

    if (entry == null) {
      return -ErrorCodes.ENOENT();
    }

    if (!entry.isWritable()) {
      return -ErrorCodes.EPERM();
    }

    WriteFileEntry writeFileEntry = (WriteFileEntry) entry;
    try (FileChannel channel = FileChannel.open(writeFileEntry.path, StandardOpenOption.WRITE)) {
      long oldSize = channel.size();
      channel.truncate(size);
      if (size > oldSize) {
        channel.write(ByteBuffer.wrap(new byte[] {0}), size - 1);
      }
      return 0;
    } catch (IOException e) {
      return -ErrorCodes.EIO();
    }
  }

  @Override
  public int ftruncate(String path, @off_t long size, FuseFileInfo fi) {
    // FIXME we can do better on all of this by avoiding lookups
    // and actually using the FuseFileInfo

    return truncate(path, size);
  }

  @Override
  public int chmod(String path, @mode_t long mode) {
    Entry entry = resolve(path);

    if (entry == null) {
      return -ErrorCodes.ENOENT();
    }

    if (entry.isDirectory() || !entry.isWritable()) {
      return -ErrorCodes.EPERM();
    }

    WriteFileEntry writeFileEntry = (WriteFileEntry) entry;
    //noinspection OctalInteger
    writeFileEntry.executable = (mode & 0111) != 0;

    return 0;
  }

  @Override
  public int utimens(String path, Timespec[] timespec) {
    Entry entry = resolve(path);

    if (entry == null) {
      return -ErrorCodes.ENOENT();
    }

    if (!entry.isWritable()) {
      return -ErrorCodes.EPERM();
    }

    return 0;
  }

  @Override
  public int access(String path, int mode) {
    Entry entry = resolve(path);

    // FIXME complicated?  Access.F_OK
    if (entry == null) {
      return -ErrorCodes.ENOENT();
    }

    if ((mode & Access.X_OK.intValue()) != 0) {
      return entry.isExecutable() ? 0 : -ErrorCodes.EACCES();
    }

    if ((mode & Access.W_OK.intValue()) != 0) {
      return entry.isWritable() ? 0 : -ErrorCodes.EACCES();
    }

    return 0; // all readable
  }

  @Override
  public int unlink(String path) {
    DirectoryEntry dirEntry = containingDirectoryForCreate(path);

    if (dirEntry == null) {
      return -ErrorCodes.ENOENT();
    }

    String base = basename(path);
    Entry entry = dirEntry.getChild(base);

    if (entry == null) {
      return -ErrorCodes.ENOENT();
    }

    if (entry.isDirectory()) {
      return -ErrorCodes.EISDIR();
    }

    dirEntry.removeChild(base);
    cleanupEntry(entry);
    return 0;
  }

  @Override
  public int rmdir(String path) {
    DirectoryEntry parent = containingDirectoryForCreate(path);
    if (parent == null) {
      return -ErrorCodes.ENOENT();
    }
    String base = basename(path);
    Entry entry = parent.getChild(base);
    if (entry == null) {
      return -ErrorCodes.ENOENT();
    }
    if (!entry.isDirectory()) {
      return -ErrorCodes.ENOTDIR();
    }
    DirectoryEntry directory = (DirectoryEntry) entry;
    if (!directory.getChildren().isEmpty()) {
      return -ErrorCodes.ENOTEMPTY();
    }
    parent.removeChild(base);
    return 0;
  }

  @Override
  public int getxattr(String path, String name, Pointer value, @size_t long size) {
    // log.log(Level.INFO, "GETXATTR: " + name);
    // seen security.capability so far...
    return -ErrorCodes.EOPNOTSUPP();
  }

  @Override
  public int setxattr(String path, String name, Pointer value, @size_t long size, int flags) {
    return -ErrorCodes.EOPNOTSUPP();
  }

  @Override
  public int listxattr(String path, Pointer list, @size_t long size) {
    return -ErrorCodes.EOPNOTSUPP();
  }

  @Override
  public int removexattr(String path, String name) {
    return -ErrorCodes.EOPNOTSUPP();
  }

  @SuppressWarnings("OctalInteger")
  private Entry createImpl(String path, long mode) {
    // assume no intersection for now
    DirectoryEntry dirEntry = containingDirectoryForCreate(path);

    if (dirEntry == null) {
      return null;
    }

    if (scratchPath == null) {
      return null;
    }
    final Path output;
    try {
      Files.createDirectories(scratchPath);
      output = Files.createTempFile(scratchPath, "output-", null);
    } catch (IOException e) {
      return null;
    }
    Entry entry = new WriteFileEntry((mode & 0111) != 0, output);
    String base = basename(path);
    cleanupEntry(dirEntry.getChild(base));
    dirEntry.putChild(base, entry);

    return entry;
  }

  private int createFileHandle(Entry e) {
    int fh;
    do {
      fh = fileHandleCounter.getAndIncrement();
    } while (fileHandleEntries.containsKey(fh));
    fileHandleEntries.put(fh, e);
    if (e instanceof WriteFileEntry writeFile) {
      writeFile.opened();
    }
    return fh;
  }

  @Override
  public int mknod(String path, @mode_t long mode, long device) {
    if (resolve(path) != null) {
      return -ErrorCodes.EEXIST();
    }
    return createImpl(path, mode) == null ? -ErrorCodes.ENOENT() : 0;
  }

  @Override
  public int create(String path, @mode_t long mode, FuseFileInfo fi) {
    if (resolve(path) != null) {
      return -ErrorCodes.EEXIST();
    }
    Entry entry = createImpl(path, mode);

    if (entry == null) {
      return -ErrorCodes.ENOENT();
    }

    fi.fh.set(createFileHandle(entry));

    return 0;
  }

  @Override
  public int open(String path, FuseFileInfo fi) {
    int flags = fi.flags.intValue();
    boolean create = (flags & OpenFlags.O_CREAT.intValue()) != 0;
    boolean exclusive = (flags & OpenFlags.O_EXCL.intValue()) != 0;
    boolean truncate = (flags & OpenFlags.O_TRUNC.intValue()) != 0;
    Entry entry = resolve(path);
    if (entry != null && create && exclusive) {
      return -ErrorCodes.EEXIST();
    }
    if (entry == null && create) {
      // open(2) does not carry a mode through this callback; newly created files use 0666.
      entry = createImpl(path, 0666);
    } else if (entry != null && truncate) {
      if (entry.isDirectory()) {
        return -ErrorCodes.EISDIR();
      }
      entry = createImpl(path, 0666);
    }

    if (entry == null) {
      return -ErrorCodes.ENOENT();
    }

    fi.fh.set(createFileHandle(entry));

    return 0;
  }

  @Override
  public int release(String path, FuseFileInfo fi) {
    Entry released = fileHandleEntries.remove(fi.fh.intValue());
    if (released instanceof WriteFileEntry writeFile) {
      writeFile.released();
    }

    /*
    // Maybe do this, maybe not
    Entry entry = resolve(path);

    if (entry.isWritable()) {
      DirectoryEntry dirEntry = containingDirectoryForPath(path);
      WriteFileEntry writeFileEntry = (FileWriteEntry) entry;

      Digest digest = inputStreamFactory.putBlob(writeFileEntry.content);

      dirEntry.putChild(name, new FileEntry(digest, writeFileEntry.executable);
    }
    */

    return 0;
  }

  @Override
  public int write(
      String path, Pointer buf, @size_t long bufSize, @off_t long offset, FuseFileInfo fi) {
    Entry entry = fileHandleEntries.get(fi.fh.intValue());
    if (entry == null) {
      return -ErrorCodes.ENOENT();
    }

    if (!entry.isWritable()) {
      return -ErrorCodes.EPERM();
    }

    WriteFileEntry writeFileEntry = (WriteFileEntry) entry;
    int size = (int) bufSize;
    byte[] bytes = new byte[size];
    buf.get(0, bytes, 0, size);
    try (FileChannel channel = FileChannel.open(writeFileEntry.path, StandardOpenOption.WRITE)) {
      return channel.write(ByteBuffer.wrap(bytes), offset);
    } catch (IOException e) {
      return -ErrorCodes.EIO();
    }
  }

  @Override
  public int flush(String path, FuseFileInfo fi) {
    // noop

    return 0;
  }

  @Override
  public int fsync(String path, int isdatasync, FuseFileInfo fi) {
    Entry entry = fileHandleEntries.get(fi.fh.intValue());
    if (entry == null) {
      return -ErrorCodes.ENOENT();
    }
    if (!(entry instanceof WriteFileEntry writeFile)) {
      return 0;
    }
    try (FileChannel channel = FileChannel.open(writeFile.path, StandardOpenOption.WRITE)) {
      channel.force(isdatasync == 0);
      return 0;
    } catch (IOException e) {
      return -ErrorCodes.EIO();
    }
  }

  @Override
  public int statfs(String path, Statvfs stat) {
    try {
      FileStore fileStore = Files.getFileStore(scratchPath);
      long blockSize = 4096;
      long blocks = fileStore.getTotalSpace() / blockSize;
      long available = fileStore.getUsableSpace() / blockSize;
      stat.f_bsize.set(blockSize);
      stat.f_frsize.set(blockSize);
      stat.f_blocks.set(blocks);
      stat.f_bfree.set(available);
      stat.f_bavail.set(available);
      stat.f_files.set(blocks);
      stat.f_ffree.set(available);
      stat.f_favail.set(available);
      stat.f_namemax.set(255);
      return 0;
    } catch (IOException e) {
      return -ErrorCodes.EIO();
    }
  }

  @Override
  public int read(
      String path, Pointer buf, @size_t long size, @off_t long offset, FuseFileInfo fi) {
    Entry entry = fileHandleEntries.get(fi.fh.intValue());
    if (entry == null) {
      return -ErrorCodes.ENOENT();
    }

    if (entry.isDirectory()) {
      return -ErrorCodes.EISDIR();
    }

    Path localPath = entry.isWritable() ? ((WriteFileEntry) entry).path : ((FileEntry) entry).path;
    if (localPath == null) {
      FileEntry fileEntry = (FileEntry) entry;
      if (fileEntry.digest.getSize() == 0) {
        return 0;
      }
      // Compatibility for the standalone mount tool. Worker inputs always have a local path.
      try (InputStream in =
          inputStreamFactory.newInput(Compressor.Value.IDENTITY, fileEntry.digest, offset)) {
        byte[] bytes = in.readNBytes((int) size);
        buf.put(0, bytes, 0, bytes.length);
        return bytes.length;
      } catch (IOException e) {
        return -ErrorCodes.EIO();
      }
    }
    try (FileChannel channel = FileChannel.open(localPath, StandardOpenOption.READ)) {
      byte[] bytes = new byte[(int) size];
      int read = channel.read(ByteBuffer.wrap(bytes), offset);
      if (read < 0) {
        return 0;
      }
      buf.put(0, bytes, 0, read);
      return read;
    } catch (IOException e) {
      return -ErrorCodes.EIO();
    }
  }

  @Override
  public int mkdir(String path, @mode_t long mode) {
    // FIXME mode validation

    DirectoryEntry dirEntry = containingDirectoryForCreate(path);

    if (dirEntry == null) {
      return -ErrorCodes.ENOENT();
    }

    String base = basename(path);

    if (dirEntry.hasChild(base)) {
      return -ErrorCodes.EEXIST();
    }

    dirEntry.putChild(base, new LocalDirectoryEntry());

    return 0;
  }

  @Override
  public int readdir(
      String path, Pointer buf, FuseFillDir filter, @off_t long offset, FuseFileInfo fi) {
    DirectoryEntry dirEntry = directoryForPath(path);

    if (dirEntry == null) {
      return -ErrorCodes.ENOENT();
    }

    filter.apply(buf, ".", null, 0);
    filter.apply(buf, "..", null, 0);
    dirEntry.forAllChildren((child) -> filter.apply(buf, child, null, 0));
    return 0;
  }

  @SuppressWarnings("ConstantConditions")
  @Override
  public int fallocate(
      String path, int mode, @off_t long off, @off_t long length, FuseFileInfo fi) {
    if (mode != 0) {
      return -ErrorCodes.EOPNOTSUPP();
    }

    Entry entry = resolve(path);

    if (entry == null) {
      return -ErrorCodes.ENOENT();
    }

    if (entry.isDirectory()) {
      return -ErrorCodes.EISDIR();
    }

    if (!entry.isWritable()) {
      return -ErrorCodes.EPERM();
    }

    return truncate(path, off + length);
  }
}
