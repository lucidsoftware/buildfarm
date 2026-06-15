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

import static build.buildfarm.common.io.Utils.getInterruptiblyOrIOException;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static com.google.common.util.concurrent.MoreExecutors.shutdownAndAwaitTermination;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

import build.bazel.remote.execution.v2.Compressor;
import build.bazel.remote.execution.v2.DigestFunction;
import build.bazel.remote.execution.v2.Directory;
import build.bazel.remote.execution.v2.DirectoryNode;
import build.bazel.remote.execution.v2.FileNode;
import build.bazel.remote.execution.v2.RequestMetadata;
import build.bazel.remote.execution.v2.SymlinkNode;
import build.buildfarm.cas.ContentAddressableStorage;
import build.buildfarm.cas.cfc.CASFileCache.Entry;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.HashFunction;
import build.buildfarm.common.Write.NullWrite;
import build.buildfarm.common.io.Directories;
import build.buildfarm.v1test.Digest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AccessDeniedException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileStore;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.stubbing.Answer;

class DirectoryEntryCFCTest {
  protected static final DigestUtil DIGEST_UTIL = new DigestUtil(HashFunction.SHA256);

  protected CASFileCache fileCache;
  private final Path root;
  private Map<Digest, ByteString> blobs;
  private ExecutorService putService;

  @Mock private Consumer<Digest> onPut;

  @Mock private Consumer<Iterable<Digest>> onExpire;

  @Mock private ContentAddressableStorage delegate;

  private ExecutorService expireService;

  protected ConcurrentMap<String, Entry> storage;

  protected DirectoryEntryCFCTest(Path fileSystemRoot) {
    this.root = fileSystemRoot.resolve("cache");
  }

  /**
   * Whether {@code computeDirectory} admits an existing on-disk {@code _dir} tree at startup on
   * this test's filesystem. True only on the native variant: {@code computeDirectory} sizes
   * directories from real on-disk dir sizes, which Jimfs does not model, so on Jimfs the tree may
   * not admit. The startup-reconstruction test asserts the directory admits when this is true (so a
   * real admission regression on a native filesystem fails the test) and tolerates non-admission
   * otherwise.
   */
  protected boolean startupAdmitsExistingDirectoryTrees() {
    return false;
  }

  @Before
  public void setUp() throws IOException, InterruptedException {
    MockitoAnnotations.initMocks(this);
    when(delegate.getWrite(
            any(Compressor.Value.class),
            any(Digest.class),
            any(UUID.class),
            any(RequestMetadata.class)))
        .thenReturn(new NullWrite());
    when(delegate.newInput(any(Compressor.Value.class), any(Digest.class), any(Long.class)))
        .thenThrow(new NoSuchFileException("null sink delegate"));
    doAnswer(
            (Answer<Iterable<Digest>>)
                invocation -> {
                  return (Iterable<Digest>) invocation.getArguments()[0];
                })
        .when(delegate)
        .findMissingBlobs(any(Iterable.class), any(DigestFunction.Value.class));
    blobs = Maps.newHashMap();
    putService = newSingleThreadExecutor();
    storage = Maps.newConcurrentMap();
    expireService = newSingleThreadExecutor();
    fileCache =
        new DirectoryEntryCFC(
            root,
            /* maxSizeInBytes= */ 1024 * 1024,
            /* maxEntrySizeInBytes= */ 1024 * 1024,
            /* hexBucketLevels= */ 1,
            expireService,
            /* accessRecorder= */ directExecutor(),
            storage,
            /* zstdBufferPool= */ null,
            onPut,
            onExpire,
            delegate,
            /* delegateSkipLoad= */ false,
            (compressor, digest, offset) -> {
              ByteString content = blobs.get(digest);
              if (content == null) {
                return fileCache.newTransparentInput(compressor, digest, offset);
              }
              checkArgument(compressor == Compressor.Value.IDENTITY);
              return content.substring((int) offset).newInput();
            });
    fileCache.initializeRootDirectory();
    // Force a known block size so tests are hermetic and don't depend on the host filesystem.
    fileCache.setBlockSizeForTesting(4096);
    // Phase 2.1: start the evictor so eviction-triggering operations work in the fixture.
    fileCache.evictorForTesting().start();
  }

  @After
  public void tearDown() throws IOException, InterruptedException {
    FileStore fileStore = Files.getFileStore(root);
    fileCache.evictorForTesting().stop();
    if (!shutdownAndAwaitTermination(putService, 1, SECONDS)) {
      throw new RuntimeException("could not shut down put service");
    }
    if (!shutdownAndAwaitTermination(expireService, 1, SECONDS)) {
      throw new RuntimeException("could not shut down expire service");
    }
    Directories.remove(root, fileStore);
  }

  @Test
  public void estimateDirectorySizeOnDisk_emptyDirectory_returnsOneBlock() {
    Directory emptyDir = Directory.getDefaultInstance();
    assertThat(CASFileCache.estimateDirectorySizeOnDisk(emptyDir, 4096)).isEqualTo(4096);
  }

  @Test
  public void estimateDirectorySizeOnDisk_fewEntries_returnsOneBlock() {
    Directory dir =
        Directory.newBuilder()
            .addFiles(
                FileNode.newBuilder()
                    .setName("file1")
                    .setDigest(DigestUtil.toDigest(DIGEST_UTIL.empty()))
                    .build())
            .addDirectories(
                DirectoryNode.newBuilder()
                    .setName("subdir")
                    .setDigest(DigestUtil.toDigest(DIGEST_UTIL.empty()))
                    .build())
            .build();
    // 2 entries * 32 bytes = 64 bytes, fits in one 4096-byte block
    assertThat(CASFileCache.estimateDirectorySizeOnDisk(dir, 4096)).isEqualTo(4096);
  }

  @Test
  public void estimateDirectorySizeOnDisk_manyEntries_scalesWithEntryCount() {
    Directory.Builder dir = Directory.newBuilder();
    for (int i = 0; i < 200; i++) {
      dir.addFiles(
          FileNode.newBuilder()
              .setName("file" + i)
              .setDigest(DigestUtil.toDigest(DIGEST_UTIL.empty()))
              .build());
    }
    // 200 entries * 32 bytes = 6400 bytes, needs 2 blocks of 4096
    assertThat(CASFileCache.estimateDirectorySizeOnDisk(dir.build(), 4096)).isEqualTo(8192);
  }

  @Test
  public void estimateDirectorySizeOnDisk_countsAllEntryTypes() {
    Directory.Builder dir = Directory.newBuilder();
    for (int i = 0; i < 80; i++) {
      dir.addFiles(
          FileNode.newBuilder()
              .setName("file" + i)
              .setDigest(DigestUtil.toDigest(DIGEST_UTIL.empty()))
              .build());
    }
    for (int i = 0; i < 60; i++) {
      dir.addDirectories(
          DirectoryNode.newBuilder()
              .setName("dir" + i)
              .setDigest(DigestUtil.toDigest(DIGEST_UTIL.empty()))
              .build());
    }
    for (int i = 0; i < 60; i++) {
      dir.addSymlinks(SymlinkNode.newBuilder().setName("link" + i).setTarget("target").build());
    }
    // 200 total entries * 32 bytes = 6400 bytes, needs 2 blocks
    assertThat(CASFileCache.estimateDirectorySizeOnDisk(dir.build(), 4096)).isEqualTo(8192);
  }

  // -- estimateSizeOnDisk tests --

  @Test
  public void estimateSizeOnDisk_zeroSize_returnsZero() {
    assertThat(CASFileCache.estimateSizeOnDisk(0, 4096, /* isHardlink= */ false)).isEqualTo(0);
  }

  @Test
  public void estimateSizeOnDisk_oneByte_returnsOneBlock() {
    assertThat(CASFileCache.estimateSizeOnDisk(1, 4096, /* isHardlink= */ false)).isEqualTo(4096);
  }

  @Test
  public void estimateSizeOnDisk_justUnderOneBlock_returnsOneBlock() {
    assertThat(CASFileCache.estimateSizeOnDisk(4095, 4096, /* isHardlink= */ false))
        .isEqualTo(4096);
  }

  @Test
  public void estimateSizeOnDisk_exactlyOneBlock_returnsOneBlock() {
    // Idempotent: an already-aligned value should not round up to the next block.
    assertThat(CASFileCache.estimateSizeOnDisk(4096, 4096, /* isHardlink= */ false))
        .isEqualTo(4096);
  }

  @Test
  public void estimateSizeOnDisk_oneByteOverBlock_returnsTwoBlocks() {
    assertThat(CASFileCache.estimateSizeOnDisk(4097, 4096, /* isHardlink= */ false))
        .isEqualTo(8192);
  }

  @Test
  public void estimateSizeOnDisk_negativeSize_throws() {
    assertThrows(
        IllegalArgumentException.class,
        () -> CASFileCache.estimateSizeOnDisk(-1, 4096, /* isHardlink= */ false));
  }

  @Test
  public void estimateSizeOnDisk_differentBlockSizes() {
    assertThat(CASFileCache.estimateSizeOnDisk(100, 512, /* isHardlink= */ false)).isEqualTo(512);
    assertThat(CASFileCache.estimateSizeOnDisk(100, 1, /* isHardlink= */ false)).isEqualTo(100);
  }

  @Test
  public void estimateSizeOnDisk_invalidBlockSize_throws() {
    assertThrows(
        IllegalArgumentException.class,
        () -> CASFileCache.estimateSizeOnDisk(100, 0, /* isHardlink= */ false));
    assertThrows(
        IllegalArgumentException.class,
        () -> CASFileCache.estimateSizeOnDisk(100, -1, /* isHardlink= */ false));
  }

  @Test
  public void estimateSizeOnDisk_isIdempotent() {
    // Applying estimateSizeOnDisk twice should give the same result as applying it once.
    // This matters because directory Entry.size values are pre-aligned, and discharge()
    // applies estimateSizeOnDisk again.
    long[] sizes = {0, 1, 100, 4095, 4096, 4097, 8192, 10000};
    long[] blockSizes = {512, 1024, 4096, 8192};
    for (long blockSize : blockSizes) {
      for (long size : sizes) {
        long once = CASFileCache.estimateSizeOnDisk(size, blockSize, /* isHardlink= */ false);
        long twice = CASFileCache.estimateSizeOnDisk(once, blockSize, /* isHardlink= */ false);
        assertThat(twice).isEqualTo(once);
      }
    }
  }

  @Test
  public void estimateSizeOnDisk_isHardlinkTrue_returnsZero() {
    // Hardlinks reuse an existing inode's blocks, so the physical cost is ~0 regardless of
    // logical size or block size.
    long[] sizes = {0, 1, 100, 4095, 4096, 4097, 1L << 20};
    long[] blockSizes = {1, 512, 1024, 4096, 8192};
    for (long blockSize : blockSizes) {
      for (long size : sizes) {
        assertThat(CASFileCache.estimateSizeOnDisk(size, blockSize, /* isHardlink= */ true))
            .isEqualTo(0);
      }
    }
  }

  @Test
  public void estimateSizeOnDisk_isHardlinkTrue_zeroSize_returnsZero() {
    // Zero-size boundary with the hardlink branch still returns 0.
    assertThat(CASFileCache.estimateSizeOnDisk(0, 4096, /* isHardlink= */ true)).isEqualTo(0);
  }

  @Test
  public void estimateSizeOnDisk_isHardlinkTrue_largeSize_returnsZero() {
    // Integer.MAX_VALUE-scale sizes with the hardlink branch still return 0 and do not overflow.
    assertThat(CASFileCache.estimateSizeOnDisk(Integer.MAX_VALUE, 4096, /* isHardlink= */ true))
        .isEqualTo(0);
    assertThat(
            CASFileCache.estimateSizeOnDisk(
                (long) Integer.MAX_VALUE + 1, 4096, /* isHardlink= */ true))
        .isEqualTo(0);
  }

  @Test
  public void estimateSizeOnDisk_isHardlinkTrue_isIdempotent() {
    // Applying estimateSizeOnDisk twice with isHardlink=true still yields 0, mirroring the
    // existing estimateSizeOnDisk_isIdempotent test.
    long[] sizes = {0, 1, 100, 4095, 4096, 4097, 8192, 10000};
    long[] blockSizes = {512, 1024, 4096, 8192};
    for (long blockSize : blockSizes) {
      for (long size : sizes) {
        long once = CASFileCache.estimateSizeOnDisk(size, blockSize, /* isHardlink= */ true);
        long twice = CASFileCache.estimateSizeOnDisk(once, blockSize, /* isHardlink= */ true);
        assertThat(twice).isEqualTo(once);
      }
    }
  }

  @Test
  public void directoryChargeDischargeSizeReturnsToZero() throws IOException, InterruptedException {
    // Put a directory, verify it charges sizeInBytes, then evict it and verify size returns to
    // zero.
    ByteString file = ByteString.copyFromUtf8("Hello Directory");
    Digest fileDigest = DIGEST_UTIL.compute(file);
    blobs.put(fileDigest, file);

    Directory directory =
        Directory.newBuilder()
            .addFiles(
                FileNode.newBuilder()
                    .setName("file")
                    .setDigest(DigestUtil.toDigest(fileDigest))
                    .build())
            .build();
    Digest dirDigest = DIGEST_UTIL.compute(directory);
    Map<build.bazel.remote.execution.v2.Digest, Directory> directoriesIndex =
        ImmutableMap.of(DigestUtil.toDigest(dirDigest), directory);

    getInterruptiblyOrIOException(fileCache.putDirectory(dirDigest, directoriesIndex, putService));

    long sizeAfterPut = fileCache.size();
    assertThat(sizeAfterPut).isGreaterThan(0);

    // Dereference the directory entry so it becomes evictable.
    fileCache.decrementReferences(
        ImmutableList.of(),
        ImmutableList.of(DigestUtil.toDigest(dirDigest)),
        DIGEST_UTIL.getDigestFunction());

    // Put a large blob that forces eviction of the directory entry.
    byte[] bigData = new byte[(int) (fileCache.maxSize() - 100)];
    ByteString bigBlob = ByteString.copyFrom(bigData);
    Digest bigDigest = DIGEST_UTIL.compute(bigBlob);
    blobs.put(bigDigest, bigBlob);
    fileCache.put(bigDigest, false);

    // After eviction and new charge, size should exactly equal the big blob's on-disk size —
    // no drift from asymmetric charge/discharge of the directory entry.
    assertThat(fileCache.size())
        .isEqualTo(CASFileCache.estimateSizeOnDisk(bigData.length, 4096, /* isHardlink= */ false));
  }

  @Test
  public void putDirectoryIncludesDirectoryOverheadInSize()
      throws IOException, InterruptedException {
    ByteString file = ByteString.copyFromUtf8("Peanut Butter");
    Digest fileDigest = DIGEST_UTIL.compute(file);
    blobs.put(fileDigest, file);

    Directory subDirectory = Directory.getDefaultInstance();
    Digest subdirDigest = DIGEST_UTIL.compute(subDirectory);
    Directory directory =
        Directory.newBuilder()
            .addFiles(
                FileNode.newBuilder()
                    .setName("file")
                    .setDigest(DigestUtil.toDigest(fileDigest))
                    .build())
            .addDirectories(
                DirectoryNode.newBuilder()
                    .setName("subdir")
                    .setDigest(DigestUtil.toDigest(subdirDigest))
                    .build())
            .build();
    Digest dirDigest = DIGEST_UTIL.compute(directory);
    Map<build.bazel.remote.execution.v2.Digest, Directory> directoriesIndex =
        ImmutableMap.of(
            DigestUtil.toDigest(dirDigest), directory,
            DigestUtil.toDigest(subdirDigest), subDirectory);

    getInterruptiblyOrIOException(fileCache.putDirectory(dirDigest, directoriesIndex, putService));

    // Phase 3 hardlinks files into _dir entries, so the blob content is charged exactly once (as
    // the
    // standalone CAS entry); inside the directory it is a hardlink contributing 0 on-disk blocks.
    // The
    // total is therefore the standalone block plus the directory-table overhead of the two
    // directories (root + empty subdir). Pinning the exact value catches a regression to byte-copy
    // materialization, which would re-introduce a second full charge for the file's blocks.
    long standalone = CASFileCache.estimateSizeOnDisk(file.size(), 4096, /* isHardlink= */ false);
    long directoryOverhead =
        CASFileCache.estimateDirectorySizeOnDisk(directory, 4096)
            + CASFileCache.estimateDirectorySizeOnDisk(subDirectory, 4096);
    assertThat(fileCache.size()).isEqualTo(standalone + directoryOverhead);
  }

  // === Phase 3: hardlink-based directory materialization ===

  /** Builds a single-file directory {@code D} containing {@code file} named "file" (non-exec). */
  private PutDirectoryFixture putSingleFileDirectory(ByteString file)
      throws IOException, InterruptedException {
    Digest fileDigest = DIGEST_UTIL.compute(file);
    blobs.put(fileDigest, file);
    Directory directory =
        Directory.newBuilder()
            .addFiles(
                FileNode.newBuilder()
                    .setName("file")
                    .setDigest(DigestUtil.toDigest(fileDigest))
                    .build())
            .build();
    Digest dirDigest = DIGEST_UTIL.compute(directory);
    Map<build.bazel.remote.execution.v2.Digest, Directory> directoriesIndex =
        ImmutableMap.of(DigestUtil.toDigest(dirDigest), directory);
    getInterruptiblyOrIOException(fileCache.putDirectory(dirDigest, directoriesIndex, putService));
    return new PutDirectoryFixture(fileDigest, dirDigest);
  }

  private record PutDirectoryFixture(Digest fileDigest, Digest dirDigest) {}

  private Object fileKeyOf(Path path) throws IOException {
    return Files.readAttributes(path, BasicFileAttributes.class).fileKey();
  }

  @Test
  public void linkAndReference_succeeds_hardlinkSharesFileKey()
      throws IOException, InterruptedException {
    PutDirectoryFixture f = putSingleFileDirectory(ByteString.copyFromUtf8("hardlink me"));

    Path standalone = fileCache.getPath(CASFileCache.getKey(f.fileDigest(), false));
    Path inDirectory = fileCache.getDirectoryPath(f.dirDigest()).resolve("file");

    // A hardlink shares the inode: same fileKey. A byte-copy would yield distinct fileKeys.
    assertThat(fileKeyOf(inDirectory)).isEqualTo(fileKeyOf(standalone));
    // The source Entry now carries exactly one CAS-directory hardlink and is pinned against
    // eviction even though no one holds a referenceCount on it.
    Entry source = storage.get(CASFileCache.getKey(f.fileDigest(), false));
    assertThat(source.casDirectoryHardlinkCount()).isEqualTo(1);
  }

  @Test
  public void linkAndReference_oneFileTwoDirectories_collapsesToSingleInodeWithCountTwo()
      throws IOException, InterruptedException {
    ByteString file = ByteString.copyFromUtf8("shared toolchain file");
    Digest fileDigest = DIGEST_UTIL.compute(file);
    blobs.put(fileDigest, file);

    // Two distinct directories (different names around the same file) reference the same file.
    Directory d1 =
        Directory.newBuilder()
            .addFiles(
                FileNode.newBuilder()
                    .setName("alpha")
                    .setDigest(DigestUtil.toDigest(fileDigest))
                    .build())
            .build();
    Directory d2 =
        Directory.newBuilder()
            .addFiles(
                FileNode.newBuilder()
                    .setName("beta")
                    .setDigest(DigestUtil.toDigest(fileDigest))
                    .build())
            .build();
    Digest d1Digest = DIGEST_UTIL.compute(d1);
    Digest d2Digest = DIGEST_UTIL.compute(d2);
    Map<build.bazel.remote.execution.v2.Digest, Directory> index =
        ImmutableMap.of(
            DigestUtil.toDigest(d1Digest), d1,
            DigestUtil.toDigest(d2Digest), d2);

    getInterruptiblyOrIOException(fileCache.putDirectory(d1Digest, index, putService));
    getInterruptiblyOrIOException(fileCache.putDirectory(d2Digest, index, putService));

    Entry source = storage.get(CASFileCache.getKey(fileDigest, false));
    assertThat(source.casDirectoryHardlinkCount()).isEqualTo(2);
    // Both directories hardlink the same inode, so the inode index holds exactly one mapping.
    assertThat(((DirectoryEntryCFC) fileCache).casInodeIndexForTesting().size()).isEqualTo(1);
  }

  @Test
  public void linkAndReference_fileSystemException_fallsBackToCopy()
      throws IOException, InterruptedException {
    // ENOLINK-class failure: hardlink unavailable, so the file is byte-copied onto a fresh inode.
    ((DirectoryEntryCFC) fileCache)
        .setFileLinkerForTesting(
            (source, destination) -> {
              throw new FileSystemException(destination.toString(), null, "simulated ENOLINK");
            });

    ByteString file = ByteString.copyFromUtf8("copy fallback content");
    PutDirectoryFixture f = putSingleFileDirectory(file);

    Path standalone = fileCache.getPath(CASFileCache.getKey(f.fileDigest(), false));
    Path inDirectory = fileCache.getDirectoryPath(f.dirDigest()).resolve("file");

    // The copy lives on a fresh inode (distinct fileKey) but byte-matches the source.
    assertThat(fileKeyOf(inDirectory)).isNotEqualTo(fileKeyOf(standalone));
    assertThat(Files.readAllBytes(inDirectory)).isEqualTo(file.toByteArray());
    // No CAS-directory hardlink was created, so the source's counter stays 0.
    Entry source = storage.get(CASFileCache.getKey(f.fileDigest(), false));
    assertThat(source.casDirectoryHardlinkCount()).isEqualTo(0);
  }

  @Test
  public void linkAndReference_unsupportedOperation_fallsBackToCopy()
      throws IOException, InterruptedException {
    ((DirectoryEntryCFC) fileCache)
        .setFileLinkerForTesting(
            (source, destination) -> {
              throw new UnsupportedOperationException("hardlinks unavailable");
            });

    ByteString file = ByteString.copyFromUtf8("unsupported hardlink content");
    PutDirectoryFixture f = putSingleFileDirectory(file);

    Path inDirectory = fileCache.getDirectoryPath(f.dirDigest()).resolve("file");
    assertThat(Files.readAllBytes(inDirectory)).isEqualTo(file.toByteArray());
    Entry source = storage.get(CASFileCache.getKey(f.fileDigest(), false));
    assertThat(source.casDirectoryHardlinkCount()).isEqualTo(0);
  }

  @Test
  public void linkAndReference_nullSourceFileKey_fallsBackToCopy()
      throws IOException, InterruptedException {
    ((DirectoryEntryCFC) fileCache).setSourceFileKeyReaderForTesting(source -> null);

    ByteString file = ByteString.copyFromUtf8("null file key fallback");
    PutDirectoryFixture f = putSingleFileDirectory(file);

    Path standalone = fileCache.getPath(CASFileCache.getKey(f.fileDigest(), false));
    Path inDirectory = fileCache.getDirectoryPath(f.dirDigest()).resolve("file");

    assertThat(Files.readAllBytes(inDirectory)).isEqualTo(file.toByteArray());
    assertThat(fileKeyOf(inDirectory)).isNotEqualTo(fileKeyOf(standalone));
    Entry source = storage.get(CASFileCache.getKey(f.fileDigest(), false));
    assertThat(source.casDirectoryHardlinkCount()).isEqualTo(0);
    assertThat(((DirectoryEntryCFC) fileCache).casInodeIndexForTesting().size()).isEqualTo(0);
  }

  @Test
  public void linkAndReference_noSuchFileException_fallsBackToReFetch()
      throws IOException, InterruptedException {
    // First link attempt simulates the Phase-2.1 evictor rename race (NoSuchFileException); the
    // re-fetch path re-materializes the source and links it on the retry.
    AtomicInteger linkCalls = new AtomicInteger();
    ((DirectoryEntryCFC) fileCache)
        .setFileLinkerForTesting(
            (source, destination) -> {
              if (linkCalls.getAndIncrement() == 0) {
                throw new NoSuchFileException(source.toString());
              }
              Files.createLink(destination, source);
            });

    ByteString file = ByteString.copyFromUtf8("refetched content");
    PutDirectoryFixture f = putSingleFileDirectory(file);

    Path inDirectory = fileCache.getDirectoryPath(f.dirDigest()).resolve("file");
    assertThat(Files.readAllBytes(inDirectory)).isEqualTo(file.toByteArray());
    // The retried link did go through, so more than one link attempt was made.
    assertThat(linkCalls.get()).isAtLeast(2);
    // The re-fetch path re-materialized the source as a CAS entry and hardlinked dst to it, so it
    // is pinned exactly like the happy path (proves the re-fetch ran and recorded the hardlink, not
    // merely that some link succeeded). The dir file shares the source inode.
    Entry source = storage.get(CASFileCache.getKey(f.fileDigest(), false));
    assertThat(source.casDirectoryHardlinkCount()).isEqualTo(1);
    assertThat(fileKeyOf(inDirectory))
        .isEqualTo(fileKeyOf(fileCache.getPath(CASFileCache.getKey(f.fileDigest(), false))));
  }

  @Test
  public void linkAndReference_noSuchFileForDestinationDoesNotRefetch()
      throws IOException, InterruptedException {
    AtomicInteger linkCalls = new AtomicInteger();
    ((DirectoryEntryCFC) fileCache)
        .setFileLinkerForTesting(
            (source, destination) -> {
              linkCalls.incrementAndGet();
              throw new NoSuchFileException(destination.toString());
            });

    assertThrows(
        Exception.class,
        () -> putSingleFileDirectory(ByteString.copyFromUtf8("missing destination")));

    assertThat(linkCalls.get()).isEqualTo(1);
  }

  @Test
  public void linkAndReference_reFetchFileSystemException_fallsBackToCopy()
      throws IOException, InterruptedException {
    AtomicInteger linkCalls = new AtomicInteger();
    ((DirectoryEntryCFC) fileCache)
        .setFileLinkerForTesting(
            (source, destination) -> {
              if (linkCalls.getAndIncrement() == 0) {
                throw new NoSuchFileException(source.toString());
              }
              throw new FileSystemException(destination.toString(), null, "simulated EMLINK");
            });

    ByteString file = ByteString.copyFromUtf8("refetch copy fallback");
    PutDirectoryFixture f = putSingleFileDirectory(file);

    Path inDirectory = fileCache.getDirectoryPath(f.dirDigest()).resolve("file");
    assertThat(Files.readAllBytes(inDirectory)).isEqualTo(file.toByteArray());
    Entry source = storage.get(CASFileCache.getKey(f.fileDigest(), false));
    assertThat(source.casDirectoryHardlinkCount()).isEqualTo(0);
    assertThat(linkCalls.get()).isAtLeast(2);
  }

  @Test
  public void linkAndReference_reFetchNullSourceFileKey_fallsBackToCopy()
      throws IOException, InterruptedException {
    AtomicInteger linkCalls = new AtomicInteger();
    AtomicInteger fileKeyReads = new AtomicInteger();
    ((DirectoryEntryCFC) fileCache)
        .setFileLinkerForTesting(
            (source, destination) -> {
              linkCalls.incrementAndGet();
              throw new NoSuchFileException(source.toString());
            });
    ((DirectoryEntryCFC) fileCache)
        .setSourceFileKeyReaderForTesting(
            source -> fileKeyReads.getAndIncrement() == 0 ? new Object() : null);

    ByteString file = ByteString.copyFromUtf8("refetch null file key fallback");
    PutDirectoryFixture f = putSingleFileDirectory(file);

    Path standalone = fileCache.getPath(CASFileCache.getKey(f.fileDigest(), false));
    Path inDirectory = fileCache.getDirectoryPath(f.dirDigest()).resolve("file");

    assertThat(Files.readAllBytes(inDirectory)).isEqualTo(file.toByteArray());
    assertThat(fileKeyOf(inDirectory)).isNotEqualTo(fileKeyOf(standalone));
    Entry source = storage.get(CASFileCache.getKey(f.fileDigest(), false));
    assertThat(source.casDirectoryHardlinkCount()).isEqualTo(0);
    assertThat(((DirectoryEntryCFC) fileCache).casInodeIndexForTesting().size()).isEqualTo(0);
    assertThat(linkCalls.get()).isEqualTo(1);
    assertThat(fileKeyReads.get()).isAtLeast(2);
  }

  @Test
  public void linkAndReference_failureDecrementsReferenceExactlyOnce()
      throws IOException, InterruptedException {
    // After a copy-fallback materialization the source must be released exactly once: not pinned by
    // referenceCount (nothing else references it) and not double-released (which would underflow).
    ((DirectoryEntryCFC) fileCache)
        .setFileLinkerForTesting(
            (source, destination) -> {
              throw new FileSystemException(destination.toString(), null, "simulated ENOLINK");
            });

    PutDirectoryFixture f = putSingleFileDirectory(ByteString.copyFromUtf8("release once"));

    Entry source = storage.get(CASFileCache.getKey(f.fileDigest(), false));
    assertThat(source.refCount()).isEqualTo(0);
    assertThat(source.isEvictable()).isTrue();
  }

  @Test
  public void putDirectory_duplicateChildDirectoryRejectedBeforeMaterialization() throws Exception {
    ByteString file = ByteString.copyFromUtf8("duplicate child hardlink");
    Digest fileDigest = DIGEST_UTIL.compute(file);
    blobs.put(fileDigest, file);

    Directory child =
        Directory.newBuilder()
            .addFiles(
                FileNode.newBuilder()
                    .setName("file")
                    .setDigest(DigestUtil.toDigest(fileDigest))
                    .build())
            .build();
    Digest childDigest = DIGEST_UTIL.compute(child);
    Directory root =
        Directory.newBuilder()
            .addDirectories(
                DirectoryNode.newBuilder()
                    .setName("dup")
                    .setDigest(DigestUtil.toDigest(childDigest))
                    .build())
            .addDirectories(
                DirectoryNode.newBuilder()
                    .setName("dup")
                    .setDigest(DigestUtil.toDigest(childDigest))
                    .build())
            .build();
    Digest rootDigest = DIGEST_UTIL.compute(root);
    Map<build.bazel.remote.execution.v2.Digest, Directory> index =
        ImmutableMap.of(
            DigestUtil.toDigest(rootDigest), root,
            DigestUtil.toDigest(childDigest), child);

    assertThrows(
        Exception.class,
        () -> getInterruptiblyOrIOException(fileCache.putDirectory(rootDigest, index, putService)));

    Entry source = storage.get(CASFileCache.getKey(fileDigest, false));
    if (source != null) {
      assertThat(source.casDirectoryHardlinkCount()).isEqualTo(0);
    }
    assertThat(((DirectoryEntryCFC) fileCache).casInodeIndexForTesting().size()).isEqualTo(0);
    assertNoTempDirectorySiblings(rootDigest);
  }

  @Test
  public void putDirectory_failureCleansRecordedHardlinkPins()
      throws IOException, InterruptedException {
    ByteString first = ByteString.copyFromUtf8("first linked before failure");
    ByteString second = ByteString.copyFromUtf8("second fails");
    Digest firstDigest = DIGEST_UTIL.compute(first);
    Digest secondDigest = DIGEST_UTIL.compute(second);
    blobs.put(firstDigest, first);
    blobs.put(secondDigest, second);

    Directory directory =
        Directory.newBuilder()
            .addFiles(
                FileNode.newBuilder()
                    .setName("first")
                    .setDigest(DigestUtil.toDigest(firstDigest))
                    .build())
            .addFiles(
                FileNode.newBuilder()
                    .setName("second")
                    .setDigest(DigestUtil.toDigest(secondDigest))
                    .build())
            .build();
    Digest dirDigest = DIGEST_UTIL.compute(directory);
    Map<build.bazel.remote.execution.v2.Digest, Directory> index =
        ImmutableMap.of(DigestUtil.toDigest(dirDigest), directory);

    ((DirectoryEntryCFC) fileCache)
        .setFileLinkerForTesting(
            (source, destination) -> {
              if (destination.getFileName().toString().equals("second")) {
                throw new AccessDeniedException(destination.toString());
              }
              Files.createLink(destination, source);
            });

    assertThrows(
        Exception.class,
        () -> getInterruptiblyOrIOException(fileCache.putDirectory(dirDigest, index, putService)));

    Entry firstSource = storage.get(CASFileCache.getKey(firstDigest, false));
    Entry secondSource = storage.get(CASFileCache.getKey(secondDigest, false));
    assertThat(firstSource.casDirectoryHardlinkCount()).isEqualTo(0);
    assertThat(secondSource.casDirectoryHardlinkCount()).isEqualTo(0);
    assertThat(((DirectoryEntryCFC) fileCache).casInodeIndexForTesting().size()).isEqualTo(0);

    ((DirectoryEntryCFC) fileCache)
        .setFileLinkerForTesting((source, destination) -> Files.createLink(destination, source));
    getInterruptiblyOrIOException(fileCache.putDirectory(dirDigest, index, putService));

    assertThat(firstSource.casDirectoryHardlinkCount()).isEqualTo(1);
    assertThat(secondSource.casDirectoryHardlinkCount()).isEqualTo(1);
  }

  @Test
  public void putDirectory_synchronousTreeFailureWaitsForMaterializersBeforeCleanup()
      throws Exception {
    ByteString file = ByteString.copyFromUtf8("linked before missing child");
    Digest fileDigest = DIGEST_UTIL.compute(file);
    blobs.put(fileDigest, file);
    Digest missingChildDigest = DIGEST_UTIL.compute(ByteString.copyFromUtf8("missing child"));

    Directory directory =
        Directory.newBuilder()
            .addFiles(
                FileNode.newBuilder()
                    .setName("file")
                    .setDigest(DigestUtil.toDigest(fileDigest))
                    .build())
            .addDirectories(
                DirectoryNode.newBuilder()
                    .setName("child")
                    .setDigest(DigestUtil.toDigest(missingChildDigest))
                    .build())
            .build();
    Digest dirDigest = DIGEST_UTIL.compute(directory);
    Map<build.bazel.remote.execution.v2.Digest, Directory> index =
        ImmutableMap.of(DigestUtil.toDigest(dirDigest), directory);

    CountDownLatch linkStarted = new CountDownLatch(1);
    CountDownLatch releaseLink = new CountDownLatch(1);
    ((DirectoryEntryCFC) fileCache)
        .setFileLinkerForTesting(
            (source, destination) -> {
              linkStarted.countDown();
              try {
                releaseLink.await();
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException(e);
              }
              Files.createLink(destination, source);
            });

    ListenableFuture<CASFileCache.PathResult> future =
        fileCache.putDirectory(dirDigest, index, putService);

    assertThat(linkStarted.await(5, SECONDS)).isTrue();
    assertThat(future.isDone()).isFalse();

    releaseLink.countDown();
    assertThrows(Exception.class, () -> getInterruptiblyOrIOException(future));

    Entry source = storage.get(CASFileCache.getKey(fileDigest, false));
    assertThat(source.casDirectoryHardlinkCount()).isEqualTo(0);
    assertThat(((DirectoryEntryCFC) fileCache).casInodeIndexForTesting().size()).isEqualTo(0);
    assertNoTempDirectorySiblings(dirDigest);
  }

  private void assertNoTempDirectorySiblings(Digest dirDigest) throws IOException {
    Path dirPath = fileCache.getDirectoryPath(dirDigest);
    try (DirectoryStream<Path> stream =
        Files.newDirectoryStream(dirPath.getParent(), dirPath.getFileName() + ".tmp.*")) {
      assertThat(stream).isEmpty();
    }
  }

  @Test
  public void evictor_fileWithCasDirectoryHardlinks_evictableOnlyAfterDirectoryEvicts()
      throws IOException, InterruptedException {
    // This test also guards the deadlock fix: the source is reclaimed only after the directory's
    // async tree-walk decrements its hardlink count and wakes the shard via requestEvictionSweep.
    // Without that wake the evictor parks in stuckAboveLow with the source skipped, the charger
    // below never gets its space, and this test hangs (times out) rather than failing fast.
    PutDirectoryFixture f = putSingleFileDirectory(ByteString.copyFromUtf8("pin me"));
    String fileKey = CASFileCache.getKey(f.fileDigest(), false);
    String dirKey = fileCache.getDirectoryKey(f.dirDigest());

    // Release the directory's own reference; it is now eligible for eviction, but the source file
    // remains pinned by its CAS-directory hardlink (the sweep skips casDirectoryHardlinkCount > 0
    // entries even though refCount == 0).
    fileCache.decrementReferences(
        ImmutableList.of(),
        ImmutableList.of(DigestUtil.toDigest(f.dirDigest())),
        DIGEST_UTIL.getDigestFunction());
    assertThat(storage.get(fileKey).casDirectoryHardlinkCount()).isEqualTo(1);

    // Force eviction by charging a blob that needs the whole cache. The directory must evict first,
    // then its tree-walk releases the source's hardlink, then the source evicts. put() only returns
    // once enough space is freed, which (since the blob needs the whole cache) requires BOTH the
    // directory and the source to be gone — so the post-conditions below hold deterministically
    // without waiting on the evictor.
    byte[] big = new byte[(int) (fileCache.maxSize() - 100)];
    Digest bigDigest = DIGEST_UTIL.compute(ByteString.copyFrom(big));
    blobs.put(bigDigest, ByteString.copyFrom(big));
    fileCache.put(bigDigest, false);

    assertThat(storage.get(dirKey)).isNull();
    assertThat(storage.get(fileKey)).isNull();
    assertThat(((DirectoryEntryCFC) fileCache).casInodeIndexForTesting().size()).isEqualTo(0);
  }

  @Test
  public void putDirectory_initialFailureDoesNotPoisonFetchers()
      throws IOException, InterruptedException {
    // The file blob is absent from the delegate, so the first putDirectory fails. A failed fetch
    // future must not stay cached (cache poisoning); the second call, after the blob is available,
    // must start a fresh fetch and succeed.
    ByteString file = ByteString.copyFromUtf8("eventually available");
    Digest fileDigest = DIGEST_UTIL.compute(file);
    Directory directory =
        Directory.newBuilder()
            .addFiles(
                FileNode.newBuilder()
                    .setName("file")
                    .setDigest(DigestUtil.toDigest(fileDigest))
                    .build())
            .build();
    Digest dirDigest = DIGEST_UTIL.compute(directory);
    Map<build.bazel.remote.execution.v2.Digest, Directory> index =
        ImmutableMap.of(DigestUtil.toDigest(dirDigest), directory);

    assertThrows(
        Exception.class,
        () -> getInterruptiblyOrIOException(fileCache.putDirectory(dirDigest, index, putService)));

    // Make the blob available and retry; a poisoned fetcher cache would replay the failure.
    blobs.put(fileDigest, file);
    getInterruptiblyOrIOException(fileCache.putDirectory(dirDigest, index, putService));

    Path inDirectory = fileCache.getDirectoryPath(dirDigest).resolve("file");
    assertThat(Files.readAllBytes(inDirectory)).isEqualTo(file.toByteArray());
  }

  @Test
  public void startupScan_existingDirectoryTrees_populatesInodeMap() throws Exception {
    // Reproduce the on-disk state a prior run leaves behind: a standalone CAS file plus a _dir tree
    // whose file is a hardlink to it. After startup, the inode index must be reconstructed so the
    // source is re-pinned against eviction while the directory still references its inode.
    byte[] content = "startup hardlinked file".getBytes(StandardCharsets.UTF_8);
    Digest fileDigest = DIGEST_UTIL.compute(ByteString.copyFrom(content));
    String fileKey = CASFileCache.getKey(fileDigest, false);
    Path standalone = fileCache.getPath(fileKey);
    Files.write(standalone, content);

    Directory directory =
        Directory.newBuilder()
            .addFiles(
                FileNode.newBuilder()
                    .setName("file")
                    .setDigest(DigestUtil.toDigest(fileDigest))
                    .build())
            .build();
    Digest dirDigest = DIGEST_UTIL.compute(directory);
    Path dirPath = fileCache.getDirectoryPath(dirDigest);
    Files.createDirectories(dirPath);
    // Hardlink the directory's file to the standalone CAS inode (nlink becomes 2).
    Files.createLink(dirPath.resolve("file"), standalone);

    getInterruptiblyOrIOException(fileCache.start(/* skipLoad= */ false));

    Entry source = storage.get(fileKey);
    assertThat(source).isNotNull();
    Entry directoryEntry = storage.get(fileCache.getDirectoryKey(dirDigest));
    if (startupAdmitsExistingDirectoryTrees()) {
      // On a native filesystem computeDirectory admits the tree (it sizes directories from real
      // on-disk dir sizes), so a null here is a real admission regression, not a provider quirk.
      // Fail rather than silently passing through the early return below.
      assertThat(directoryEntry).isNotNull();
    } else if (directoryEntry == null) {
      // Jimfs does not model on-disk directory sizes, so computeDirectory may not admit the tree.
      // With no _dir entry there is nothing to hold a pin, and the index must stay empty.
      assertThat(source.casDirectoryHardlinkCount()).isEqualTo(0);
      assertThat(((DirectoryEntryCFC) fileCache).casInodeIndexForTesting().size()).isEqualTo(0);
      return;
    }
    Object standaloneFileKey = fileKeyOf(standalone);
    Object directoryFileKey = fileKeyOf(dirPath.resolve("file"));
    if (standaloneFileKey != null && standaloneFileKey.equals(directoryFileKey)) {
      assertThat(source.casDirectoryHardlinkCount()).isEqualTo(1);
      assertThat(((DirectoryEntryCFC) fileCache).casInodeIndexForTesting().size()).isEqualTo(1);
    } else {
      // Some test providers expose nlink but not a stable BasicFileAttributes.fileKey(). In that
      // case startup cannot safely map the directory hardlink back to its standalone source, so it
      // charges the linked inode to the directory instead of reconstructing a pin.
      assertThat(source.casDirectoryHardlinkCount()).isEqualTo(0);
      assertThat(((DirectoryEntryCFC) fileCache).casInodeIndexForTesting().size()).isEqualTo(0);
      assertThat(directoryEntry.size)
          .isAtLeast(
              CASFileCache.estimateSizeOnDisk(content.length, 4096, /* isHardlink= */ false));
    }
    // When reconstructed, the pin keeps the source skipped by the evictor even though nothing holds
    // a referenceCount on it.
    assertThat(source.refCount()).isEqualTo(0);
  }

  @Test
  public void startupScan_snapshotFileWithDirectoryTrees_populatesInodeMap() throws Exception {
    byte[] content = "startup snapshot hardlinked file".getBytes(StandardCharsets.UTF_8);
    Digest fileDigest = DIGEST_UTIL.compute(ByteString.copyFrom(content));
    String fileKey = CASFileCache.getKey(fileDigest, false);
    Path standalone = fileCache.getPath(fileKey);
    Files.write(standalone, content);

    Directory directory =
        Directory.newBuilder()
            .addFiles(
                FileNode.newBuilder()
                    .setName("file")
                    .setDigest(DigestUtil.toDigest(fileDigest))
                    .build())
            .build();
    Digest dirDigest = DIGEST_UTIL.compute(directory);
    String dirKey = fileCache.getDirectoryKey(dirDigest);
    Path dirPath = fileCache.getDirectoryPath(dirDigest);
    Files.createDirectories(dirPath);
    Files.createLink(dirPath.resolve("file"), standalone);

    String snapshot = fileKey + "," + content.length + "\n" + dirKey + ",1\n";
    Files.write(
        root.resolve(LruSnapshotFiles.name(1, 0)), snapshot.getBytes(StandardCharsets.UTF_8));

    getInterruptiblyOrIOException(fileCache.start(/* skipLoad= */ false));

    Entry source = storage.get(fileKey);
    assertThat(source).isNotNull();
    Entry directoryEntry = storage.get(dirKey);
    if (startupAdmitsExistingDirectoryTrees()) {
      assertThat(directoryEntry).isNotNull();
    } else if (directoryEntry == null) {
      assertThat(source.casDirectoryHardlinkCount()).isEqualTo(0);
      return;
    }
    assertThat(source.casDirectoryHardlinkCount()).isEqualTo(1);
    assertThat(((DirectoryEntryCFC) fileCache).casInodeIndexForTesting().size()).isEqualTo(1);
  }

  @Test
  public void put_thenReference_doesNotTouchCasDirectoryHardlinkCount() throws Exception {
    // The two counters are independent. A plain reference (the same referenceCount path exec-root
    // hardlinks use) never touches casDirectoryHardlinkCount — only DirectoryEntryCFC's tree
    // materialization does. Guards against a future change conflating the two.
    ByteString blob = ByteString.copyFromUtf8("plain reference");
    Digest digest = DIGEST_UTIL.compute(blob);
    blobs.put(digest, blob);
    fileCache.put(digest, false); // creates and references the entry

    String key = CASFileCache.getKey(digest, false);
    Entry entry = storage.get(key);
    assertThat(entry.casDirectoryHardlinkCount()).isEqualTo(0);
    assertThat(entry.refCount()).isAtLeast(1);

    assertThat(fileCache.referenceIfExists(key)).isTrue();
    assertThat(entry.casDirectoryHardlinkCount()).isEqualTo(0);

    // Release both references; the counter is still untouched.
    fileCache.decrementReference(key);
    fileCache.decrementReference(key);
    assertThat(entry.casDirectoryHardlinkCount()).isEqualTo(0);
  }

  @Test
  public void linkAndReference_reFetchExhausted_propagatesAfterMaxAttempts() {
    // Every link attempt simulates the Phase-2.1 evictor rename race, so the bounded re-fetch loop
    // exhausts its budget and the failure propagates to the action instead of livelocking. The
    // initial link plus MAX_REFETCH_ATTEMPTS re-fetch attempts each invoke the linker exactly once;
    // guards the retry bound against an off-by-one or an unbounded loop.
    AtomicInteger linkCalls = new AtomicInteger();
    ((DirectoryEntryCFC) fileCache)
        .setFileLinkerForTesting(
            (source, destination) -> {
              linkCalls.incrementAndGet();
              throw new NoSuchFileException(source.toString());
            });

    assertThrows(
        Exception.class,
        () -> putSingleFileDirectory(ByteString.copyFromUtf8("always races away")));

    assertThat(linkCalls.get()).isEqualTo(1 + DirectoryEntryCFC.MAX_REFETCH_ATTEMPTS);
  }

  @Test
  public void startupScan_directoryHardlinkWithoutIndexedSource_skipsPinReconstruction()
      throws Exception {
    // Reproduce the on-disk state after a standalone source was evicted while CAS-directory
    // hardlinks to its inode survive (or a warm-snapshot source the full scan never indexed): the
    // _dir files have nlink > 1 but no source Entry is in the startup inode index. Reconstruction
    // must skip the pin (logged FINE) rather than fail, and the directory must still admit.
    byte[] content = "orphaned hardlink".getBytes(StandardCharsets.UTF_8);
    Digest fileDigest = DIGEST_UTIL.compute(ByteString.copyFrom(content));
    Directory directory =
        Directory.newBuilder()
            .addFiles(
                FileNode.newBuilder()
                    .setName("a")
                    .setDigest(DigestUtil.toDigest(fileDigest))
                    .build())
            .addFiles(
                FileNode.newBuilder()
                    .setName("b")
                    .setDigest(DigestUtil.toDigest(fileDigest))
                    .build())
            .build();
    Digest dirDigest = DIGEST_UTIL.compute(directory);
    Path dirPath = fileCache.getDirectoryPath(dirDigest);
    Files.createDirectories(dirPath);
    // Two files hardlinked to each other (nlink == 2) and no standalone CAS file for the inode.
    Files.write(dirPath.resolve("a"), content);
    Files.createLink(dirPath.resolve("b"), dirPath.resolve("a"));
    // Guard against a vacuous pass: the walk must actually take the nlink > 1 hardlink branch.
    assertThat(Files.getAttribute(dirPath.resolve("a"), "unix:nlink")).isEqualTo(2);

    // Startup must not throw despite the hardlinked files having no indexed source.
    getInterruptiblyOrIOException(fileCache.start(/* skipLoad= */ false));

    // No source Entry was indexed, so no pin is reconstructed and the index stays empty.
    assertThat(((DirectoryEntryCFC) fileCache).casInodeIndexForTesting().size()).isEqualTo(0);
    Entry directoryEntry = storage.get(fileCache.getDirectoryKey(dirDigest));
    assertThat(directoryEntry).isNotNull();
    assertThat(directoryEntry.size)
        .isAtLeast(CASFileCache.estimateSizeOnDisk(content.length, 4096, /* isHardlink= */ false));
  }

  @Test
  public void startupScan_invalidDirectoryDoesNotApplyPendingHardlinkPins() throws Exception {
    byte[] content = "valid standalone source".getBytes(StandardCharsets.UTF_8);
    Digest fileDigest = DIGEST_UTIL.compute(ByteString.copyFrom(content));
    String fileKey = CASFileCache.getKey(fileDigest, false);
    Path standalone = fileCache.getPath(fileKey);
    Files.write(standalone, content);

    Digest dirDigest = DIGEST_UTIL.compute(ByteString.copyFromUtf8("invalid hardlink dir"));
    String dirKey = fileCache.getDirectoryKey(dirDigest);
    Path dirPath = fileCache.getDirectoryPath(dirDigest);
    Files.createDirectories(dirPath);
    Files.createLink(dirPath.resolve("hardlink"), standalone);
    Files.write(dirPath.resolve("oversized"), new byte[(int) fileCache.maxEntrySize() + 1]);

    getInterruptiblyOrIOException(fileCache.start(/* skipLoad= */ false));

    Entry source = storage.get(fileKey);
    assertThat(source).isNotNull();
    assertThat(source.casDirectoryHardlinkCount()).isEqualTo(0);
    assertThat(storage.get(dirKey)).isNull();
    assertThat(((DirectoryEntryCFC) fileCache).casInodeIndexForTesting().size()).isEqualTo(0);
  }

  @RunWith(JUnit4.class)
  @SuppressWarnings("PMD.TestClassWithoutTestCases")
  public static class JimfsDirectoryEntryCFCTest extends DirectoryEntryCFCTest {
    public JimfsDirectoryEntryCFCTest() {
      super(
          Iterables.getFirst(
              Jimfs.newFileSystem(
                      Configuration.unix().toBuilder()
                          .setAttributeViews("basic", "owner", "posix", "unix")
                          .build())
                  .getRootDirectories(),
              null));
    }
  }

  // Native filesystem test variant (needed for computeDirectory which uses real dir sizes)
  @RunWith(JUnit4.class)
  @SuppressWarnings("PMD.TestClassWithoutTestCases")
  public static class NativeDirectoryEntryCFCTest extends DirectoryEntryCFCTest {
    private final Path tempDir;

    public NativeDirectoryEntryCFCTest() throws IOException {
      this(createTempDirectory());
    }

    private NativeDirectoryEntryCFCTest(Path tempDir) {
      super(tempDir);
      this.tempDir = tempDir;
    }

    @Override
    protected boolean startupAdmitsExistingDirectoryTrees() {
      return true;
    }

    private static Path createTempDirectory() throws IOException {
      if (Thread.interrupted()) {
        throw new RuntimeException(new InterruptedException());
      }
      return Files.createTempDirectory("native-dir-entry-cfc-test");
    }

    @After
    @Override
    public void tearDown() throws IOException, InterruptedException {
      super.tearDown();
      Files.deleteIfExists(tempDir);
    }

    @Test
    public void computeDirectoryIncludesDirectoryOverhead() throws Exception {
      byte[] content = "test content".getBytes(StandardCharsets.UTF_8);
      // The key must go through the entry path strategy (hex bucket directories).
      String dirKey = fileCache.getDirectoryKey(DIGEST_UTIL.compute(ByteString.copyFrom(content)));
      Path dirEntry = fileCache.getPath(dirKey);
      Files.createDirectories(dirEntry.resolve("subdir"));
      Files.write(dirEntry.resolve("file.txt"), content);

      getInterruptiblyOrIOException(fileCache.start(/* skipLoad= */ false));

      Entry entry = storage.get(dirKey);
      assertThat(entry).isNotNull();
      // On a real filesystem, directory attrs.size() returns at least one block (typically 4096).
      // The entry size should be greater than just the file content because it includes
      // the directory overhead for both the root _dir directory and the "subdir" subdirectory.
      assertThat(entry.size).isGreaterThan((long) content.length);
    }
  }
}
