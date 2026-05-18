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

package build.buildfarm.cas.cfc;

import static build.buildfarm.common.io.Utils.getInterruptiblyOrIOException;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static com.google.common.util.concurrent.MoreExecutors.shutdownAndAwaitTermination;
import static java.lang.Thread.State.TERMINATED;
import static java.lang.Thread.State.WAITING;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import build.bazel.remote.execution.v2.Compressor;
import build.bazel.remote.execution.v2.DigestFunction;
import build.bazel.remote.execution.v2.Directory;
import build.bazel.remote.execution.v2.DirectoryNode;
import build.bazel.remote.execution.v2.FileNode;
import build.bazel.remote.execution.v2.RequestMetadata;
import build.buildfarm.cas.ContentAddressableStorage;
import build.buildfarm.cas.ContentAddressableStorage.Blob;
import build.buildfarm.cas.DigestMismatchException;
import build.buildfarm.cas.cfc.CASFileCache.CancellableOutputStream;
import build.buildfarm.cas.cfc.CASFileCache.Entry;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.HashFunction;
import build.buildfarm.common.InputStreamFactory;
import build.buildfarm.common.Write;
import build.buildfarm.common.Write.NullWrite;
import build.buildfarm.common.io.Directories;
import build.buildfarm.common.io.EvenMoreFiles;
import build.buildfarm.common.io.FeedbackOutputStream;
import build.buildfarm.v1test.Digest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;
import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.ByteString;
import io.grpc.Deadline;
import io.grpc.Status;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.nio.channels.ClosedChannelException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.stubbing.Answer;

@SuppressWarnings("PMD.TestClassWithoutTestCases")
class CASFileCacheTest {
  private final DigestUtil DIGEST_UTIL = new DigestUtil(HashFunction.SHA256);

  private CASFileCache fileCache;
  private final Path root;
  private final boolean storeFileDirsIndexInMemory;
  private Map<Digest, ByteString> blobs;
  private ExecutorService putService;

  @Mock private Consumer<Digest> onPut;

  @Mock private Consumer<Iterable<Digest>> onExpire;

  @Mock private ContentAddressableStorage delegate;

  private ExecutorService expireService;

  private ConcurrentMap<String, Entry> storage;

  protected CASFileCacheTest(Path fileSystemRoot, boolean storeFileDirsIndexInMemory) {
    this.root = fileSystemRoot.resolve("cache");
    this.storeFileDirsIndexInMemory = storeFileDirsIndexInMemory;
  }

  private static final long TEST_BLOCK_SIZE = 4096;

  // Convenience method to call estimateSizeOnDisk with the test's block size
  private static long estimateSizeOnDisk(long logicalSize) {
    return CASFileCache.estimateSizeOnDisk(logicalSize, TEST_BLOCK_SIZE, /* isHardlink= */ false);
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
            /* maxSizeInBytes= */ 24576,
            /* maxEntrySizeInBytes= */ 24576,
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
    // do this so that we can remove the cache root dir
    fileCache.initializeRootDirectory();
    // Force a known block size so tests are more hermetic and don't depend on the host filesystem
    fileCache.setBlockSizeForTesting(TEST_BLOCK_SIZE);
  }

  @After
  public void tearDown() throws IOException, InterruptedException {
    FileStore fileStore = Files.getFileStore(root);
    // bazel appears to have a problem with us creating directories under
    // windows that are marked as no-delete. clean up after ourselves with
    // our utils
    if (!shutdownAndAwaitTermination(putService, 1, SECONDS)) {
      throw new RuntimeException("could not shut down put service");
    }
    if (!shutdownAndAwaitTermination(expireService, 1, SECONDS)) {
      throw new RuntimeException("could not shut down expire service");
    }
    Directories.remove(root, fileStore);
  }

  @Test
  public void putCreatesFile() throws IOException, InterruptedException {
    ByteString blob = ByteString.copyFromUtf8("Hello, World");
    Digest blobDigest = DIGEST_UTIL.compute(blob);
    blobs.put(blobDigest, blob);
    Path path = fileCache.put(blobDigest, false).path();
    assertThat(Files.exists(path)).isTrue();
  }

  @Test(expected = IllegalStateException.class)
  public void putEmptyFileThrowsIllegalStateException() throws IOException, InterruptedException {
    InputStreamFactory mockInputStreamFactory = mock(InputStreamFactory.class);
    ByteString blob = ByteString.copyFromUtf8("");
    Digest blobDigest = DIGEST_UTIL.compute(blob);
    // supply an empty input stream if called for test clarity
    when(mockInputStreamFactory.newInput(Compressor.Value.IDENTITY, blobDigest, /* offset= */ 0))
        .thenReturn(ByteString.EMPTY.newInput());
    try {
      fileCache.put(blobDigest, false);
    } finally {
      verifyNoInteractions(mockInputStreamFactory);
    }
  }

  @Test
  public void putCreatesExecutable() throws IOException, InterruptedException {
    ByteString blob = ByteString.copyFromUtf8("executable");
    Digest blobDigest = DIGEST_UTIL.compute(blob);
    blobs.put(blobDigest, blob);
    Path path = fileCache.put(blobDigest, true).path();
    assertThat(Files.isExecutable(path)).isTrue();
  }

  @Test
  public void putDirectoryCreatesTree() throws IOException, InterruptedException {
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
    Path dirPath =
        getInterruptiblyOrIOException(
                fileCache.putDirectory(dirDigest, directoriesIndex, putService))
            .path();
    assertThat(Files.isDirectory(dirPath)).isTrue();
    assertThat(Files.exists(dirPath.resolve("file"))).isTrue();
    assertThat(Files.isDirectory(dirPath.resolve("subdir"))).isTrue();
  }

  @Test
  public void putDirectoryIOExceptionRollsBack() throws IOException, InterruptedException {
    ByteString file = ByteString.copyFromUtf8("Peanut Butter");
    Digest fileDigest = DIGEST_UTIL.compute(file);
    // omitting blobs.put to incur IOException
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
    boolean exceptionHandled = false;
    try {
      getInterruptiblyOrIOException(
          fileCache.putDirectory(dirDigest, directoriesIndex, putService));
    } catch (PutDirectoryException e) {
      exceptionHandled = true;
    }
    assertThat(exceptionHandled).isTrue();
    assertThat(Files.exists(fileCache.getDirectoryPath(dirDigest))).isFalse();
  }

  @Test
  public void expireUnreferencedEntryRemovesBlobFile() throws IOException, InterruptedException {
    byte[] bigData = new byte[22000]; // on-disk: 24576 bytes (6 blocks), fills the 24576-byte cache
    ByteString bigBlob = ByteString.copyFrom(bigData);
    Digest bigDigest = DIGEST_UTIL.compute(bigBlob);
    blobs.put(bigDigest, bigBlob);
    Path bigPath = fileCache.put(bigDigest, false).path();

    decrementReference(bigPath);

    byte[] strawData = new byte[30]; // on-disk: 4096, takes us beyond 24576-byte cache limit
    ByteString strawBlob = ByteString.copyFrom(strawData);
    Digest strawDigest = DIGEST_UTIL.compute(strawBlob);
    blobs.put(strawDigest, strawBlob);
    Path strawPath = fileCache.put(strawDigest, false).path();

    assertThat(Files.exists(bigPath)).isFalse();
    assertThat(Files.exists(strawPath)).isTrue();
  }

  @Test
  public void startEmptyCas() throws Exception {
    // start the file cache with no files.
    // the cache should start without any initial files in the cache.
    fileCache.start(false).get();

    assertThat(storage).isEmpty();
  }

  @Test
  public void startCasAssumeDirectory() throws Exception {
    // create a "_dir" file on the root
    Path path = root.resolve("foobar_dir");
    ByteString blob = ByteString.copyFromUtf8("content");
    Files.write(path, blob.toByteArray());

    // start the CAS with a file whose name indicates its a directory
    // the cache should start and consider it a compute directory
    fileCache.start(false).get();

    // check the current state to ensure no files were processed
    assertThat(Files.exists(path)).isFalse();
    assertThat(storage).isEmpty();
  }

  @Test
  public void startLoadsExistingBlob() throws Exception {
    FileStore fileStore = Files.getFileStore(root);
    ByteString blob = ByteString.copyFromUtf8("blob");
    Digest blobDigest = DIGEST_UTIL.compute(blob);
    Path path = fileCache.getPath(fileCache.getKey(blobDigest, false));
    Path execPath = fileCache.getPath(fileCache.getKey(blobDigest, true));
    Files.write(path, blob.toByteArray());
    EvenMoreFiles.setReadOnlyPerms(path, false, fileStore);
    Files.write(execPath, blob.toByteArray());
    EvenMoreFiles.setReadOnlyPerms(execPath, true, fileStore);

    fileCache.start(false).get();

    assertThat(storage.size()).isEqualTo(2);

    // explicitly not providing blob via blobs, this would throw if fetched from factory
    //
    // FIXME https://github.com/google/truth/issues/285 assertThat(Path) is ambiguous
    assertThat(fileCache.put(blobDigest, false).path().equals(path)).isTrue();
    assertThat(fileCache.put(blobDigest, true).path().equals(execPath)).isTrue();
  }

  @Test
  public void startLoadsExistingBlobWithBlockAlignedSize() throws Exception {
    // Simulate a restart: write blobs directly to disk, then start() the cache.
    // Verify that sizeInBytes reflects block-aligned on-disk sizes, not logical sizes.
    FileStore fileStore = Files.getFileStore(root);

    // Two blobs with different sizes, both smaller than one block.
    ByteString blob1 = ByteString.copyFromUtf8("small");
    Digest digest1 = DIGEST_UTIL.compute(blob1);
    Path path1 = fileCache.getPath(fileCache.getKey(digest1, false));
    Files.write(path1, blob1.toByteArray());
    EvenMoreFiles.setReadOnlyPerms(path1, false, fileStore);

    ByteString blob2 = ByteString.copyFromUtf8("another small blob");
    Digest digest2 = DIGEST_UTIL.compute(blob2);
    Path path2 = fileCache.getPath(fileCache.getKey(digest2, false));
    Files.write(path2, blob2.toByteArray());
    EvenMoreFiles.setReadOnlyPerms(path2, false, fileStore);

    fileCache.start(false).get();

    assertThat(storage.size()).isEqualTo(2);
    // Each blob is < 4096 bytes, so each occupies one full block on disk.
    // Total size should be 2 * 4096 = 8192, NOT the sum of logical sizes.
    assertThat(fileCache.size())
        .isEqualTo(estimateSizeOnDisk(blob1.size()) + estimateSizeOnDisk(blob2.size()));
    assertThat(fileCache.size()).isEqualTo(2 * TEST_BLOCK_SIZE);
  }

  @Test
  public void startSkipsLoadingExistingBlob() throws Exception {
    FileStore fileStore = Files.getFileStore(root);
    ByteString blob = ByteString.copyFromUtf8("blob");
    Digest blobDigest = DIGEST_UTIL.compute(blob);
    Path path = fileCache.getPath(fileCache.getKey(blobDigest, false));
    Path execPath = fileCache.getPath(fileCache.getKey(blobDigest, true));
    Files.write(path, blob.toByteArray());
    EvenMoreFiles.setReadOnlyPerms(path, false, fileStore);
    Files.write(execPath, blob.toByteArray());
    EvenMoreFiles.setReadOnlyPerms(execPath, true, fileStore);

    fileCache.start(/* skipLoad= */ true).get();

    // check the current state to ensure our two files were processed
    assertThat(storage).isEmpty();
    assertThat(Files.exists(path)).isFalse();
    assertThat(Files.exists(execPath)).isFalse();
  }

  @Test
  public void startRemovesInvalidEntries() throws Exception {
    Path tooFewComponents = root.resolve("00").resolve("toofewcomponents");
    Path tooManyComponents = root.resolve("00").resolve("too_many_components_here");
    Path invalidDigest = root.resolve("00").resolve("digest");
    ByteString validBlob = ByteString.copyFromUtf8("valid");
    Digest validDigest = DIGEST_UTIL.compute(ByteString.copyFromUtf8("valid"));
    Path invalidExec = fileCache.getPath(CASFileCache.getKey(validDigest, false) + "_regular");

    Files.write(tooFewComponents, ImmutableList.of("Too Few Components"), StandardCharsets.UTF_8);
    Files.write(tooManyComponents, ImmutableList.of("Too Many Components"), StandardCharsets.UTF_8);
    Files.write(invalidDigest, ImmutableList.of("Digest is not valid"), StandardCharsets.UTF_8);
    Files.write(
        invalidExec, validBlob.toByteArray()); // content would match but for invalid exec field

    fileCache.start(/* skipLoad= */ false).get();

    assertThat(Files.exists(tooFewComponents)).isFalse();
    assertThat(Files.exists(tooManyComponents)).isFalse();
    assertThat(Files.exists(invalidDigest)).isFalse();
    assertThat(Files.exists(invalidExec)).isFalse();
  }

  @Test
  public void newInputRemovesNonExistentEntry() throws IOException, InterruptedException {
    Digest nonexistentDigest = DIGEST_UTIL.compute(ByteString.copyFromUtf8("file does not exist"));
    String nonexistentKey = fileCache.getKey(nonexistentDigest, false);
    // Use the orphan factory: refCount = 0 with self-pointered LRU links, matching the
    // post-startup-scan state. The previous test poked entry.before/after directly; the
    // orphan factory is now the supported construction path for this shape.
    Entry entry = Entry.orphan(nonexistentKey, 1, Deadline.after(10, SECONDS));
    storage.put(nonexistentKey, entry);
    NoSuchFileException noSuchFileException = null;
    try (InputStream ignored =
        fileCache.newInput(Compressor.Value.IDENTITY, nonexistentDigest, 0)) {
      fail("should not get here");
    } catch (NoSuchFileException e) {
      noSuchFileException = e;
    }

    assertThat(noSuchFileException).isNotNull();
    assertThat(storage.containsKey(nonexistentKey)).isFalse();
  }

  @Test
  public void expireEntryWaitsForUnreferencedEntry()
      throws ExecutionException, IOException, InterruptedException {
    byte[] bigData = new byte[22000]; // on-disk: 24576 bytes (6 blocks), fills the 24576-byte cache
    Arrays.fill(bigData, (byte) 1);
    ByteString bigContent = ByteString.copyFrom(bigData);
    Digest bigDigest = DIGEST_UTIL.compute(bigContent);
    blobs.put(bigDigest, bigContent);
    Path bigPath = fileCache.put(bigDigest, /* isExecutable= */ false).path();

    AtomicBoolean started = new AtomicBoolean(false);
    ExecutorService service = newSingleThreadExecutor();
    Future<Void> putFuture =
        service.submit(
            () -> {
              started.set(true);
              ByteString content = ByteString.copyFromUtf8("CAS Would Exceed Max Size");
              Digest digest = DIGEST_UTIL.compute(content);
              blobs.put(digest, content);
              fileCache.put(digest, /* isExecutable= */ false);
              return null;
            });
    while (!started.get()) {
      MICROSECONDS.sleep(1);
    }
    // minimal test to ensure that we're blocked
    assertThat(putFuture.isDone()).isFalse();
    decrementReference(bigPath);
    try {
      putFuture.get();
    } finally {
      if (!shutdownAndAwaitTermination(service, 1, SECONDS)) {
        throw new RuntimeException("could not shut down service");
      }
    }
  }

  @Test
  public void waitForLastUnreferencedEntrySkipsWalkAtInfoLevel()
      throws ExecutionException, IOException, InterruptedException {
    try (LogCapture capture = new LogCapture(CASFileCache.class, Level.INFO)) {
      runOverflowingPutAndAwaitWaitCycle(capture);

      List<LogRecord> records = capture.snapshot();
      assertThat(records).isNotEmpty();
      // Production path emits only the new lightweight INFO summary; the detailed walk
      // (header / per-unreferenced-entry / min/max summary) is now FINE-only and must
      // not appear at INFO.
      assertThat(records.stream().anyMatch(r -> formatted(r).contains("waiting:"))).isTrue();
      assertThat(records.stream().noneMatch(r -> formatted(r).contains("header("))).isTrue();
      assertThat(records.stream().noneMatch(r -> formatted(r).contains("unreferenced entry(")))
          .isTrue();
      assertThat(
              records.stream().noneMatch(r -> formatted(r).contains("unreferenced list is empty")))
          .isTrue();
    }
  }

  @Test
  public void waitForLastUnreferencedEntryEmitsDetailAtFineLevel()
      throws ExecutionException, IOException, InterruptedException {
    try (LogCapture capture = new LogCapture(CASFileCache.class, Level.FINE)) {
      runOverflowingPutAndAwaitWaitCycle(capture);

      List<LogRecord> records = capture.snapshot();
      assertThat(records).isNotEmpty();
      // FINE level reproduces the original detailed walk: a "header(" line per
      // iteration plus a "unreferenced list is empty ... min(..) max(..)" summary
      // per iteration. Production INFO summary line must NOT appear at FINE because
      // the two paths are mutually exclusive.
      assertThat(records.stream().anyMatch(r -> formatted(r).contains("header("))).isTrue();
      assertThat(
              records.stream().anyMatch(r -> formatted(r).contains("unreferenced list is empty")))
          .isTrue();
      assertThat(records.stream().noneMatch(r -> formatted(r).contains("waiting:"))).isTrue();
    }
  }

  // Fills the cache with one referenced entry, then submits a second put that will park
  // inside waitForLastUnreferencedEntry (LRU is empty while the first entry is held).
  // Awaits at least one captured log record so the assertions see the wait-cycle output,
  // then releases the held entry and drains the put.
  private void runOverflowingPutAndAwaitWaitCycle(LogCapture capture)
      throws ExecutionException, IOException, InterruptedException {
    byte[] bigData = new byte[22000];
    ByteString bigBlob = ByteString.copyFrom(bigData);
    Digest bigDigest = DIGEST_UTIL.compute(bigBlob);
    blobs.put(bigDigest, bigBlob);
    Path bigPath = fileCache.put(bigDigest, /* isExecutable= */ false).path();

    ExecutorService service = newSingleThreadExecutor();
    Future<Void> putFuture =
        service.submit(
            () -> {
              ByteString content = ByteString.copyFromUtf8("CAS Would Exceed Max Size");
              Digest digest = DIGEST_UTIL.compute(content);
              blobs.put(digest, content);
              fileCache.put(digest, /* isExecutable= */ false);
              return null;
            });
    capture.awaitRecord(SECONDS.toNanos(5));
    decrementReference(bigPath);
    try {
      putFuture.get();
    } finally {
      if (!shutdownAndAwaitTermination(service, 1, SECONDS)) {
        throw new RuntimeException("could not shut down service");
      }
    }
  }

  @Test
  @SuppressWarnings("GuardedBy")
  public void waitForLastUnreferencedEntryStillThrowsWhenStorageEmpty()
      throws InterruptedException {
    // Storage starts empty in the test fixture; the LRU header is unlinked so the
    // method enters the wait loop and immediately throws on the empty-storage check.
    IllegalStateException thrown = null;
    fileCache.lruLockForTesting().lock();
    try {
      fileCache.waitForLastUnreferencedEntry(1024L);
    } catch (IllegalStateException e) {
      thrown = e;
    } finally {
      fileCache.lruLockForTesting().unlock();
    }
    assertThat(thrown).isNotNull();
    assertThat(thrown).hasMessageThat().contains("there are no keys to wait for expiration on");
  }

  @Test
  @SuppressWarnings("GuardedBy")
  public void waitForLastUnreferencedEntryExitsOnSizeDropBelowLimit()
      throws IOException, InterruptedException {
    // Populate one referenced entry so storage isn't empty; with the entry referenced,
    // the LRU is empty (header.after == header) and the method enters the wait loop.
    byte[] bigData = new byte[22000];
    ByteString bigBlob = ByteString.copyFrom(bigData);
    Digest bigDigest = DIGEST_UTIL.compute(bigBlob);
    blobs.put(bigDigest, bigBlob);
    fileCache.put(bigDigest, /* isExecutable= */ false);

    AtomicReference<Entry> result = new AtomicReference<>();
    AtomicReference<Throwable> error = new AtomicReference<>();
    Thread caller =
        new Thread(
            () -> {
              try {
                fileCache.lruLockForTesting().lock();
                try {
                  // Force totalBytes above the cap so the await-then-check sees the
                  // over-cap state initially.
                  fileCache.setSizeInBytesForTesting(fileCache.maxSize() + TEST_BLOCK_SIZE);
                  result.set(fileCache.waitForLastUnreferencedEntry(1024L));
                } finally {
                  fileCache.lruLockForTesting().unlock();
                }
              } catch (Throwable t) {
                error.set(t);
              }
            });
    caller.start();
    // Detect "caller is parked on lruCondition" via hasWaiters() under the lock.
    // Thread.State.WAITING is a scheduler hint that GC pauses or vCPU contention
    // can leave stale; hasWaiters() is atomic w.r.t. the condition. Once true,
    // drop totalBytes back to the cap and signal in the same critical section so
    // the await-recheck branch returns null.
    long deadline = System.nanoTime() + SECONDS.toNanos(5);
    while (true) {
      fileCache.lruLockForTesting().lock();
      try {
        if (fileCache.lruLockForTesting().hasWaiters(fileCache.lruConditionForTesting())) {
          fileCache.setSizeInBytesForTesting(fileCache.maxSize());
          fileCache.lruConditionForTesting().signalAll();
          break;
        }
      } finally {
        fileCache.lruLockForTesting().unlock();
      }
      if (System.nanoTime() > deadline) {
        caller.interrupt();
        fail("caller did not enter Condition.await() within budget");
      }
      Thread.yield();
    }
    caller.join(SECONDS.toMillis(5));
    if (caller.isAlive()) {
      // Interrupt before fail so a parked caller doesn't sit on lruCondition through
      // tearDown and cascade noise into subsequent tests — matching the discipline
      // applied to the sibling stall-regression tests.
      caller.interrupt();
      fail("caller still parked on lruCondition after signalAll");
    }
    assertThat(error.get()).isNull();
    assertThat(result.get()).isNull();
  }

  /** Captures JUL log records for a target logger at a configured level. */
  private static final class LogCapture implements AutoCloseable {
    private final Logger logger;
    private final Handler handler;
    private final Level previousLevel;
    private final boolean previousUseParentHandlers;
    private final List<LogRecord> records = new CopyOnWriteArrayList<>();

    LogCapture(Class<?> targetClass, Level level) {
      this.logger = Logger.getLogger(targetClass.getName());
      this.previousLevel = logger.getLevel();
      this.previousUseParentHandlers = logger.getUseParentHandlers();
      this.handler =
          new Handler() {
            @Override
            public void publish(LogRecord record) {
              records.add(record);
            }

            @Override
            public void flush() {}

            @Override
            public void close() {}
          };
      handler.setLevel(Level.ALL);
      logger.setLevel(level);
      logger.setUseParentHandlers(false);
      logger.addHandler(handler);
    }

    void awaitRecord(long timeoutNanos) throws InterruptedException {
      long deadline = System.nanoTime() + timeoutNanos;
      while (records.isEmpty()) {
        if (System.nanoTime() >= deadline) {
          throw new AssertionError(
              "no log record arrived within " + NANOSECONDS.toMillis(timeoutNanos) + "ms");
        }
        MICROSECONDS.sleep(100);
      }
    }

    List<LogRecord> snapshot() {
      return new ArrayList<>(records);
    }

    @Override
    public void close() {
      logger.removeHandler(handler);
      logger.setLevel(previousLevel);
      logger.setUseParentHandlers(previousUseParentHandlers);
    }
  }

  // CASFileCache pre-formats log messages with String.format, so LogRecord.getParameters()
  // is always null and getMessage() is the final string.
  private static String formatted(LogRecord r) {
    String msg = r.getMessage();
    return msg == null ? "" : msg;
  }

  @Test
  public void containsRecordsAccess() throws Exception {
    fileCache.start(false).get();

    ByteString contentOne = ByteString.copyFromUtf8("one");
    Digest digestOne = DIGEST_UTIL.compute(contentOne);
    blobs.put(digestOne, contentOne);
    ByteString contentTwo = ByteString.copyFromUtf8("two");
    Digest digestTwo = DIGEST_UTIL.compute(contentTwo);
    blobs.put(digestTwo, contentTwo);
    ByteString contentThree = ByteString.copyFromUtf8("three");
    Digest digestThree = DIGEST_UTIL.compute(contentThree);
    blobs.put(digestThree, contentThree);

    String pathOne =
        fileCache.put(digestOne, /* isExecutable= */ false).path().getFileName().toString();
    String pathTwo =
        fileCache.put(digestTwo, /* isExecutable= */ false).path().getFileName().toString();
    String pathThree =
        fileCache.put(digestThree, /* isExecutable= */ false).path().getFileName().toString();
    fileCache.decrementReferences(
        ImmutableList.of(pathOne, pathTwo, pathThree),
        ImmutableList.of(),
        DIGEST_UTIL.getDigestFunction());
    /* sentinel <- three <- two <- one <- sentinel */
    assertThat(storage.get(pathOne).after).isEqualTo(storage.get(pathTwo));
    assertThat(storage.get(pathTwo).after).isEqualTo(storage.get(pathThree));

    /* sentinel <- one <- three <- two <- sentinel */
    assertThat(
            fileCache.findMissingBlobs(
                ImmutableList.of(DigestUtil.toDigest(digestOne)), digestOne.getDigestFunction()))
        .isEmpty();
    assertThat(storage.get(pathTwo).after).isEqualTo(storage.get(pathThree));
    assertThat(storage.get(pathThree).after).isEqualTo(storage.get(pathOne));
  }

  @Test
  public void mismatchedSizeIsNotContained() throws InterruptedException {
    ByteString content = ByteString.copyFromUtf8("mismatched");
    Blob blob = new Blob(content, DIGEST_UTIL);
    Digest digest = blob.getDigest();
    fileCache.put(blob);

    Digest mismatchedDigest = digest.toBuilder().setSize(digest.getSize() + 1).build();
    assertThat(fileCache.contains(digest, /* result= */ null)).isTrue();
    assertThat(fileCache.contains(mismatchedDigest, /* result= */ null)).isFalse();
  }

  @Test
  public void negativeSizeIsContainedAndPopulatesResult() throws InterruptedException {
    ByteString content = ByteString.copyFromUtf8("lookup");
    Blob blob = new Blob(content, DIGEST_UTIL);
    Digest digest = blob.getDigest();
    fileCache.put(blob);

    build.bazel.remote.execution.v2.Digest.Builder result =
        build.bazel.remote.execution.v2.Digest.newBuilder();
    Digest lookupDigest = digest.toBuilder().setSize(-1).build();
    assertThat(fileCache.contains(lookupDigest, result)).isTrue();
    assertThat(result.build()).isEqualTo(DigestUtil.toDigest(digest));
  }

  Write getWrite(Digest digest) throws IOException {
    return fileCache.getWrite(
        Compressor.Value.IDENTITY, digest, UUID.randomUUID(), RequestMetadata.getDefaultInstance());
  }

  @Test
  public void writeAddsEntry() throws IOException {
    ByteString content = ByteString.copyFromUtf8("Hello, World");
    Digest digest = DIGEST_UTIL.compute(content);

    AtomicBoolean notified = new AtomicBoolean(false);
    Write write = getWrite(digest);
    write.getFuture().addListener(() -> notified.set(true), directExecutor());
    try (OutputStream out = write.getOutput(1, SECONDS, () -> {})) {
      content.writeTo(out);
    }
    assertThat(notified.get()).isTrue();
    String key = fileCache.getKey(digest, false);
    assertThat(storage.get(key)).isNotNull();
    try (InputStream in = Files.newInputStream(fileCache.getPath(key))) {
      assertThat(ByteString.readFrom(in)).isEqualTo(content);
    }
  }

  @Test
  public void asyncWriteCompletionDischargesWriteSize() throws IOException {
    ByteString content = ByteString.copyFromUtf8("Hello, World");
    Digest digest = DIGEST_UTIL.compute(content);

    Write completingWrite = getWrite(digest);
    Write incompleteWrite = getWrite(digest);
    AtomicBoolean notified = new AtomicBoolean(false);
    // both should be size committed
    incompleteWrite.getFuture().addListener(() -> notified.set(true), directExecutor());
    OutputStream incompleteOut = incompleteWrite.getOutput(1, SECONDS, () -> {});
    try (OutputStream out = completingWrite.getOutput(1, SECONDS, () -> {})) {
      assertThat(fileCache.size()).isEqualTo(estimateSizeOnDisk(digest.getSize()) * 2);
      content.writeTo(out);
    }
    assertThat(notified.get()).isTrue();
    if (!shutdownAndAwaitTermination(expireService, 1, SECONDS)) {
      throw new RuntimeException("could not shut down expire service");
    }
    assertThat(fileCache.size()).isEqualTo(estimateSizeOnDisk(digest.getSize()));
    assertThat(incompleteWrite.getCommittedSize()).isEqualTo(digest.getSize());
    assertThat(incompleteWrite.isComplete()).isTrue();
    incompleteOut.close(); // redundant
  }

  @Test
  public void cancelDischargesWriteSize() throws IOException {
    ByteString content = ByteString.copyFromUtf8("Hello, World");
    Digest digest = DIGEST_UTIL.compute(content);

    Write cancellingWrite = getWrite(digest);
    OutputStream out = cancellingWrite.getOutput(1, SECONDS, () -> {});
    assertThat(out).isInstanceOf(CancellableOutputStream.class);
    CancellableOutputStream cancelOut = (CancellableOutputStream) out;
    assertThat(fileCache.size()).isEqualTo(estimateSizeOnDisk(digest.getSize()));
    cancelOut.cancel();
    assertThat(fileCache.size()).isEqualTo(0);
    assertThat(cancellingWrite.getCommittedSize()).isEqualTo(0);
    assertThat(cancellingWrite.isComplete()).isFalse();
  }

  @Test
  public void cancelNegatesProgressAndCanRestart() throws IOException {
    ByteString content = ByteString.copyFromUtf8("Hello, World");
    Digest digest = DIGEST_UTIL.compute(content);

    Write cancellingWrite = getWrite(digest);
    AtomicBoolean notified = new AtomicBoolean(false);
    cancellingWrite.getFuture().addListener(() -> notified.set(true), directExecutor());
    OutputStream out = cancellingWrite.getOutput(1, SECONDS, () -> {});
    assertThat(out).isInstanceOf(CancellableOutputStream.class);
    CancellableOutputStream cancelOut = (CancellableOutputStream) out;
    assertThat(fileCache.size()).isEqualTo(estimateSizeOnDisk(digest.getSize()));
    content.substring(0, 6).writeTo(out);
    assertThat(cancellingWrite.getCommittedSize()).isEqualTo(6);
    assertThat(cancellingWrite.isComplete()).isFalse();
    cancelOut.cancel();
    assertThat(cancellingWrite.getCommittedSize()).isEqualTo(0);
    assertThat(cancellingWrite.isComplete()).isFalse();
    try (OutputStream restartedOut = cancellingWrite.getOutput(1, SECONDS, () -> {})) {
      content.writeTo(restartedOut);
    }
    assertThat(notified.get()).isTrue();
    assertThat(fileCache.size()).isEqualTo(estimateSizeOnDisk(digest.getSize()));
    assertThat(cancellingWrite.getCommittedSize()).isEqualTo(digest.getSize());
    assertThat(cancellingWrite.isComplete()).isTrue();
  }

  @Test
  public void incompleteWriteFileIsResumed() throws IOException {
    ByteString content = ByteString.copyFromUtf8("Hello, World");
    Digest digest = DIGEST_UTIL.compute(content);

    UUID writeId = UUID.randomUUID();
    String key = fileCache.getKey(digest, false);
    Path writePath = fileCache.getPath(key).resolveSibling(key + "." + writeId);
    try (OutputStream out = Files.newOutputStream(writePath)) {
      content.substring(0, 6).writeTo(out);
    }
    Write write =
        fileCache.getWrite(
            Compressor.Value.IDENTITY, digest, writeId, RequestMetadata.getDefaultInstance());
    AtomicBoolean notified = new AtomicBoolean(false);
    write.getFuture().addListener(() -> notified.set(true), directExecutor());
    assertThat(write.getCommittedSize()).isEqualTo(6);
    try (OutputStream out = write.getOutput(6, 1, SECONDS, () -> {})) {
      content.substring(6, 9).writeTo(out);
    }
    // ensure that we can continue via a full call to getOutput
    assertThat(write.getCommittedSize()).isEqualTo(9);
    try (OutputStream out = write.getOutput(9, 1, SECONDS, () -> {})) {
      content.substring(9).writeTo(out);
    }
    assertThat(notified.get()).isTrue();
    assertThat(write.getCommittedSize()).isEqualTo(digest.getSize());
    assertThat(write.isComplete()).isTrue();
  }

  @Test
  public void writeOutputSynchronizesOnOutput() throws IOException {
    ByteString content = ByteString.copyFromUtf8("Hello, World");
    Digest digest = DIGEST_UTIL.compute(content);

    AtomicBoolean writeClosed = new AtomicBoolean(false);
    Write write = getWrite(digest);
    OutputStream out = write.getOutput(1, SECONDS, () -> {});
    // write is open and should block other output acquisition
    Thread closer =
        new Thread(
            () -> {
              try {
                MICROSECONDS.sleep(1);
                writeClosed.set(true);
                out.close();
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });
    closer.start();
    try (OutputStream ignored = write.getOutput(1, SECONDS, () -> {})) {
      assertThat(writeClosed.get()).isTrue();
    }
    write.reset(); // ensure that the output stream is closed
  }

  @Test
  public void writeOutputFutureIsSerialized() throws Exception {
    ByteString content = ByteString.copyFromUtf8("Hello, World");
    Digest digest = DIGEST_UTIL.compute(content);

    Write write = getWrite(digest);
    ListenableFuture<FeedbackOutputStream> firstOut = write.getOutputFuture(1, SECONDS, () -> {});
    ListenableFuture<FeedbackOutputStream> secondOut = write.getOutputFuture(1, SECONDS, () -> {});
    assertThat(firstOut.isDone()).isTrue();
    assertThat(secondOut.isDone()).isFalse();
    // close the first output
    firstOut.get().close();
    assertThat(secondOut.isDone()).isTrue();
    secondOut.get().close();
    write.reset(); // ensure that the output stream is closed
  }

  @Test(expected = DigestMismatchException.class)
  public void invalidContentThrowsDigestMismatch() throws IOException {
    ByteString content = ByteString.copyFromUtf8("Hello, World");
    Digest digest = DIGEST_UTIL.compute(content);

    Write write = getWrite(digest);
    try (OutputStream out = write.getOutput(1, SECONDS, () -> {})) {
      ByteString.copyFromUtf8("H3110, W0r1d").writeTo(out);
    }
  }

  @Test
  public void readRemovesNonexistentEntry() throws IOException, InterruptedException {
    ByteString content = ByteString.copyFromUtf8("Hello, World");
    Blob blob = new Blob(content, DIGEST_UTIL);

    fileCache.put(blob);
    String key = fileCache.getKey(blob.getDigest(), /* isExecutable= */ false);
    // putCreatesFile verifies this
    Files.delete(fileCache.getPath(key));
    // update entry with expired deadline
    storage.get(key).existsDeadline = Deadline.after(0, SECONDS);

    assertThrows(
        NoSuchFileException.class,
        () -> fileCache.newInput(Compressor.Value.IDENTITY, blob.getDigest(), /* offset= */ 0));
    assertThat(storage.containsKey(key)).isFalse();
  }

  @Test
  public void emptyWriteIsComplete() throws IOException {
    Write write = getWrite(DIGEST_UTIL.compute(ByteString.EMPTY));
    assertThat(write.isComplete()).isTrue();
  }

  class UnsupportedWrite implements Write {
    @Override
    public long getCommittedSize() {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isComplete() {
      throw new UnsupportedOperationException();
    }

    @Override
    public FeedbackOutputStream getOutput(
        long offset, long deadlineAfter, TimeUnit deadlineAfterUnits, Runnable onReadyHandler)
        throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public ListenableFuture<FeedbackOutputStream> getOutputFuture(
        long offset, long deadlineAfter, TimeUnit deadlineAfterUnits, Runnable onReadyHandler) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void reset() {
      throw new UnsupportedOperationException();
    }

    @Override
    public ListenableFuture<Long> getFuture() {
      throw new UnsupportedOperationException();
    }
  }

  @Test
  public void expireInterruptCausesExpirySequenceHalt() throws IOException, InterruptedException {
    Blob expiringBlob;
    try (ByteString.Output out = ByteString.newOutput(22000)) {
      for (int i = 0; i < 22000; i++) {
        out.write(0);
      }
      expiringBlob = new Blob(out.toByteString(), DIGEST_UTIL);
      fileCache.put(expiringBlob);
    }
    Digest expiringDigest = expiringBlob.getDigest();

    // set the delegate to throw interrupted on write output creation
    Write interruptingWrite =
        new UnsupportedWrite() {
          boolean canReset = false;

          // WritesHelper.streamIntoWriteFuture probes isComplete() before calling getOutput(). We
          // need isComplete to return false so the helper proceeds to getOutput() where this
          // mock's InterruptedException triggers for this test.
          @Override
          public boolean isComplete() {
            return false;
          }

          @Override
          public FeedbackOutputStream getOutput(
              long offset, long deadlineAfter, TimeUnit deadlineAfterUnits, Runnable onReadyHandler)
              throws IOException {
            canReset = true;
            throw new IOException(new InterruptedException());
          }

          @Override
          public void reset() {
            if (!canReset) {
              throw new UnsupportedOperationException();
            }
          }
        };
    when(delegate.getWrite(
            eq(Compressor.Value.IDENTITY),
            eq(expiringDigest),
            any(UUID.class),
            any(RequestMetadata.class)))
        .thenReturn(interruptingWrite);

    // FIXME we should have a guarantee that we did not iterate over another expiration
    InterruptedException sequenceException = null;
    try {
      fileCache.put(new Blob(ByteString.copyFromUtf8("Hello, World"), DIGEST_UTIL));
      fail("should not get here");
    } catch (InterruptedException e) {
      sequenceException = e;
    }
    assertThat(sequenceException).isNotNull();

    verify(delegate, times(1))
        .getWrite(
            eq(Compressor.Value.IDENTITY),
            eq(expiringDigest),
            any(UUID.class),
            any(RequestMetadata.class));
    assertThat(storage).isEmpty();
  }

  @Test
  public void delegateWriteCompleteIsNotAnError() throws IOException, InterruptedException {
    Blob expiringBlob;
    try (ByteString.Output out = ByteString.newOutput(22000)) {
      for (int i = 0; i < 22000; i++) {
        out.write(0);
      }
      expiringBlob = new Blob(out.toByteString(), DIGEST_UTIL);
      fileCache.put(expiringBlob);
    }
    Digest expiringDigest = expiringBlob.getDigest();

    // set the delegate to throw on stream create, indicate write complete after
    Write completingWrite =
        new UnsupportedWrite() {
          boolean completed = false;

          @Override
          public FeedbackOutputStream getOutput(
              long offset, long deadlineAfter, TimeUnit deadlineAfterUnits, Runnable onReadyHandler)
              throws IOException {
            completed = true;
            throw new IOException("indicates already complete");
          }

          @Override
          public boolean isComplete() {
            return completed;
          }
        };
    when(delegate.getWrite(
            eq(Compressor.Value.IDENTITY),
            eq(expiringDigest),
            any(UUID.class),
            any(RequestMetadata.class)))
        .thenReturn(completingWrite);

    Blob blob = new Blob(ByteString.copyFromUtf8("Hello, World"), DIGEST_UTIL);
    fileCache.put(blob);

    verify(delegate, times(1))
        .getWrite(
            eq(Compressor.Value.IDENTITY),
            eq(expiringDigest),
            any(UUID.class),
            any(RequestMetadata.class));
    assertThat(completingWrite.isComplete()).isTrue();
    assertThat(storage.keySet()).containsExactly(blob.getDigest().getHash());
  }

  void decrementReference(Path path) throws IOException, InterruptedException {
    fileCache.decrementReferences(
        ImmutableList.of(path.getFileName().toString()),
        ImmutableList.of(),
        DIGEST_UTIL.getDigestFunction());
  }

  @Test
  public void legacyDecrementReferencesShouldReleaseDirectoryInputsOutsideMonitor()
      throws Exception {
    class ObservingLegacyDirectoryCFC extends LegacyDirectoryCFC {
      final AtomicBoolean observeReleases = new AtomicBoolean();
      final AtomicBoolean releasedWhileHoldingMonitor = new AtomicBoolean();

      ObservingLegacyDirectoryCFC(Path legacyRoot) {
        super(
            legacyRoot,
            /* maxSizeInBytes= */ 24576,
            /* maxEntrySizeInBytes= */ 24576,
            /* hexBucketLevels= */ 1,
            /* storeFileDirsIndexInMemory= */ true,
            /* execRootFallback= */ false,
            expireService,
            /* accessRecorder= */ directExecutor(),
            Maps.newConcurrentMap(),
            LegacyDirectoryCFC.DIRECTORIES_INDEX_NAME_MEMORY,
            /* zstdBufferPool= */ null,
            onPut,
            onExpire,
            delegate,
            /* delegateSkipLoad= */ false,
            (compressor, digest, offset) -> {
              ByteString content = blobs.get(digest);
              assertThat(content).isNotNull();
              checkArgument(compressor == Compressor.Value.IDENTITY);
              return content.substring((int) offset).newInput();
            });
      }

      @Override
      protected int decrementInputReferences(Iterable<String> inputFiles) {
        ImmutableList<String> inputs = ImmutableList.copyOf(inputFiles);
        if (observeReleases.get() && !inputs.isEmpty() && Thread.holdsLock(this)) {
          releasedWhileHoldingMonitor.set(true);
        }
        return super.decrementInputReferences(inputs);
      }
    }

    ObservingLegacyDirectoryCFC legacy =
        new ObservingLegacyDirectoryCFC(root.resolve("legacy-monitor"));
    legacy.initializeRootDirectory();
    legacy.setBlockSizeForTesting(TEST_BLOCK_SIZE);
    try {
      ByteString content = ByteString.copyFromUtf8("legacy-directory-input");
      Digest fileDigest = DIGEST_UTIL.compute(content);
      blobs.put(fileDigest, content);
      Directory directory =
          Directory.newBuilder()
              .addFiles(
                  FileNode.newBuilder()
                      .setName("input")
                      .setDigest(DigestUtil.toDigest(fileDigest))
                      .build())
              .build();
      Digest directoryDigest = DIGEST_UTIL.compute(directory);
      build.bazel.remote.execution.v2.Digest directoryApiDigest =
          DigestUtil.toDigest(directoryDigest);

      getInterruptiblyOrIOException(
          legacy.putDirectory(
              directoryDigest, ImmutableMap.of(directoryApiDigest, directory), putService));

      legacy.observeReleases.set(true);
      legacy.decrementReferences(
          ImmutableList.of(),
          ImmutableList.of(directoryApiDigest),
          DIGEST_UTIL.getDigestFunction());

      assertThat(legacy.releasedWhileHoldingMonitor.get()).isFalse();
    } finally {
      legacy.stop();
    }
  }

  @Test
  public void duplicateExpiredEntrySuppressesDigestExpiration()
      throws IOException, InterruptedException {
    Blob expiringBlob;
    try (ByteString.Output out = ByteString.newOutput(10000)) {
      for (int i = 0; i < 10000; i++) {
        out.write(0);
      }
      expiringBlob = new Blob(out.toByteString(), DIGEST_UTIL);
    }
    blobs.put(expiringBlob.getDigest(), expiringBlob.getData());
    decrementReference(
        fileCache
            .put(expiringBlob.getDigest(), /* isExecutable= */ false)
            .path()); // expected eviction
    blobs.clear();
    decrementReference(
        fileCache
            .put(expiringBlob.getDigest(), /* isExecutable= */ true)
            .path()); // should be fed from storage directly, not through delegate

    fileCache.put(new Blob(ByteString.copyFromUtf8("Hello, World"), DIGEST_UTIL));

    verifyNoInteractions(onExpire);
    // assert expiration of non-executable digest
    String expiringKey = fileCache.getKey(expiringBlob.getDigest(), /* isExecutable= */ false);
    assertThat(storage.containsKey(expiringKey)).isFalse();
    assertThat(Files.exists(fileCache.getPath(expiringKey))).isFalse();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void interruptDeferredDuringExpirations() throws IOException, InterruptedException {
    Blob expiringBlob;
    try (ByteString.Output out = ByteString.newOutput(22000)) {
      for (int i = 0; i < 22000; i++) {
        out.write(0);
      }
      expiringBlob = new Blob(out.toByteString(), DIGEST_UTIL);
    }
    fileCache.put(expiringBlob);
    // state of CAS
    //   22000-byte key (24576 bytes on disk, fills the cache)

    AtomicReference<Throwable> exRef = new AtomicReference<>(null);
    // 0 = not blocking
    // 1 = blocking
    // 2 = delegate write
    AtomicInteger writeState = new AtomicInteger(0);
    // this will ensure that the discharge task is blocked until we release it
    Future<Void> blockingExpiration =
        expireService.submit(
            () -> {
              writeState.getAndIncrement();
              while (writeState.get() != 0) {
                try {
                  MICROSECONDS.sleep(1);
                } catch (InterruptedException e) {
                  // ignore
                }
              }
              return null;
            });
    when(delegate.getWrite(
            eq(Compressor.Value.IDENTITY),
            eq(expiringBlob.getDigest()),
            any(UUID.class),
            any(RequestMetadata.class)))
        .thenReturn(
            new NullWrite() {
              @Override
              public FeedbackOutputStream getOutput(
                  long offset,
                  long deadlineAfter,
                  TimeUnit deadlineAfterUnits,
                  Runnable onReadyHandler)
                  throws IOException {
                try {
                  while (writeState.get() != 1) {
                    MICROSECONDS.sleep(1);
                  }
                  writeState.getAndIncrement(); // move into output stream state
                  SECONDS.sleep(1); // inspire a long enough delay to be interrupted
                } catch (InterruptedException e) {
                  throw new IOException(e);
                }
                return super.getOutput(offset, deadlineAfter, deadlineAfterUnits, onReadyHandler);
              }
            });
    Thread expiringThread =
        new Thread(
            () -> {
              try {
                fileCache.put(new Blob(ByteString.copyFromUtf8("Hello, World"), DIGEST_UTIL));
              } catch (InterruptedException e) {
                throw new RuntimeException(e);
              }
              fail("should not get here");
            });
    expiringThread.setUncaughtExceptionHandler((t, e) -> exRef.set(e));
    // wait for blocking state
    while (writeState.get() != 1) {
      MICROSECONDS.sleep(1);
    }
    expiringThread.start();
    while (writeState.get() != 2) {
      MICROSECONDS.sleep(1);
    }
    // expiry has been initiated, thread should be waiting
    MICROSECONDS.sleep(10); // just trying to ensure that we've reached the future wait point
    // hopefully this will be scheduled *after* the discharge task
    Future<Void> completedExpiration = expireService.submit(() -> null);
    // interrupt it
    expiringThread.interrupt();

    assertThat(expiringThread.isAlive()).isTrue();
    assertThat(completedExpiration.isDone()).isFalse();
    writeState.set(0);
    while (!blockingExpiration.isDone()) {
      MICROSECONDS.sleep(1);
    }
    expiringThread.join();
    // CAS should now be empty due to expiration and failed put
    while (!completedExpiration.isDone()) {
      MICROSECONDS.sleep(1);
    }
    assertThat(fileCache.size()).isEqualTo(0);
    Throwable t = exRef.get();
    assertThat(t).isNotNull();
    t = t.getCause();
    assertThat(t).isNotNull();
    assertThat(t).isInstanceOf(InterruptedException.class);
  }

  @Test
  public void readThroughSwitchesToLocalOnComplete() throws IOException, InterruptedException {
    ByteString content = ByteString.copyFromUtf8("Hello, World");
    Blob blob = new Blob(content, DIGEST_UTIL);
    when(delegate.newInput(eq(Compressor.Value.IDENTITY), eq(blob.getDigest()), eq(0L)))
        .thenReturn(content.newInput());
    InputStream in = fileCache.newInput(Compressor.Value.IDENTITY, blob.getDigest(), 0L);
    byte[] buf = new byte[content.size()];
    // advance to the middle of the content
    assertThat(in.read(buf, 0, 6)).isEqualTo(6);
    assertThat(ByteString.copyFrom(buf, 0, 6)).isEqualTo(content.substring(0, 6));
    verify(delegate, times(1)).newInput(Compressor.Value.IDENTITY, blob.getDigest(), 0L);
    // trigger the read through to complete immediately by supplying the blob
    fileCache.put(blob);
    // read the remaining content
    int remaining = content.size() - 6;
    assertThat(in.read(buf, 6, remaining)).isEqualTo(remaining);
    assertThat(ByteString.copyFrom(buf)).isEqualTo(content);
    in.close();
  }

  @Test
  public void readThroughSwitchedToLocalContinues() throws Exception {
    ByteString content = ByteString.copyFromUtf8("Hello, World");
    Blob blob = new Blob(content, DIGEST_UTIL);
    ExecutorService service = newSingleThreadExecutor();
    SettableFuture<Void> writeComplete = SettableFuture.create();
    // we need to register callbacks on the shared write future
    Write write =
        new NullWrite() {
          @Override
          public ListenableFuture<Long> getFuture() {
            return Futures.transform(
                writeComplete, result -> blob.getDigest().getSize(), directExecutor());
          }

          @Override
          public FeedbackOutputStream getOutput(
              long offset,
              long deadlineAfter,
              TimeUnit deadlineAfterUnits,
              Runnable onReadyHandler) {
            return new FeedbackOutputStream() {
              int offset = 0;

              @Override
              public void write(int b) {
                throw new UnsupportedOperationException();
              }

              @Override
              public void write(byte[] buf, int ofs, int len) throws IOException {
                // hangs on second read
                if (offset == 6) {
                  service.submit(() -> writeComplete.set(null));
                  throw new ClosedChannelException();
                }
                offset += len;
              }

              @Override
              public boolean isReady() {
                return true;
              }
            };
          }
        };
    when(delegate.getWrite(
            eq(Compressor.Value.IDENTITY),
            eq(blob.getDigest()),
            any(UUID.class),
            any(RequestMetadata.class)))
        .thenReturn(write);
    when(delegate.newInput(eq(Compressor.Value.IDENTITY), eq(blob.getDigest()), eq(0L)))
        .thenReturn(content.newInput());
    // the switch will reset to this point
    InputStream switchedIn = content.newInput();
    switchedIn.skip(6);
    when(delegate.newInput(eq(Compressor.Value.IDENTITY), eq(blob.getDigest()), eq(6L)))
        .thenReturn(switchedIn);
    InputStream in =
        fileCache.newReadThroughInput(Compressor.Value.IDENTITY, blob.getDigest(), 0, write);
    byte[] buf = new byte[content.size()];
    // advance to the middle of the content
    assertThat(in.read(buf, 0, 6)).isEqualTo(6);
    assertThat(ByteString.copyFrom(buf, 0, 6)).isEqualTo(content.substring(0, 6));
    verify(delegate, times(1)).newInput(Compressor.Value.IDENTITY, blob.getDigest(), 0L);
    // read the remaining content
    int remaining = content.size() - 6;
    assertThat(in.read(buf, 6, remaining)).isEqualTo(remaining);
    assertThat(ByteString.copyFrom(buf)).isEqualTo(content);
    if (!shutdownAndAwaitTermination(service, 1, SECONDS)) {
      throw new RuntimeException("could not shut down service");
    }
  }

  @Test
  public void findMissingBlobsFiltersEmptyBlobs() throws Exception {
    Digest emptyDigest = Digest.getDefaultInstance();
    assertThat(
            fileCache.findMissingBlobs(
                ImmutableList.of(DigestUtil.toDigest(emptyDigest)), DigestFunction.Value.UNKNOWN))
        .isEmpty();
  }

  @Test
  public void findMissingBlobsPopulatesUnknownSize() throws Exception {
    Blob blob = new Blob(ByteString.copyFromUtf8("content"), DIGEST_UTIL);
    Digest queryDigest = blob.getDigest().toBuilder().setSize(-1).build();
    Iterable<build.bazel.remote.execution.v2.Digest> digests =
        ImmutableList.of(DigestUtil.toDigest(queryDigest));
    build.bazel.remote.execution.v2.Digest responseDigest =
        Iterables.getOnlyElement(
            fileCache.findMissingBlobs(digests, queryDigest.getDigestFunction()));
    assertThat(responseDigest).isEqualTo(DigestUtil.toDigest(queryDigest));

    // populate the digest
    fileCache.put(blob);

    responseDigest =
        Iterables.getOnlyElement(
            fileCache.findMissingBlobs(digests, queryDigest.getDigestFunction()));
    assertThat(responseDigest).isEqualTo(DigestUtil.toDigest(blob.getDigest()));
  }

  @Test
  public void copyExternalInputRetries() throws Exception {
    CASFileCache flakyExternalCAS =
        new DirectoryEntryCFC(
            root,
            /* maxSizeInBytes= */ 24576,
            /* maxEntrySizeInBytes= */ 24576,
            /* hexBucketLevels= */ 1,
            expireService,
            /* accessRecorder= */ directExecutor(),
            storage,
            /* zstdBufferPool= */ null,
            /* onPut= */ digest -> {},
            /* onExpire= */ digests -> {},
            /* delegate= */ null,
            /* delegateSkipLoad= */ false,
            new InputStreamFactory() {
              boolean throwUnavailable = true;

              @Override
              public InputStream newInput(Compressor.Value compressor, Digest digest, long offset)
                  throws IOException {
                ByteString content = blobs.get(digest);
                if (throwUnavailable) {
                  throwUnavailable = false;
                  return new InputStream() {
                    int count = 0;

                    @Override
                    public int read(byte[] buf) throws IOException {
                      return read(buf, 0, buf.length);
                    }

                    @Override
                    public int read() {
                      throw new UnsupportedOperationException();
                    }

                    @Override
                    public int read(byte[] buf, int offset, int len) throws IOException {
                      if (count >= digest.getSize() / 2) {
                        throw new IOException(Status.UNAVAILABLE.asRuntimeException());
                      }
                      len = Math.min((int) digest.getSize() / 2 - count, len);
                      content.substring(count, count + len).copyTo(buf, offset);
                      count += len;
                      return len;
                    }
                  };
                }
                return content.substring((int) offset).newInput();
              }
            });
    flakyExternalCAS.initializeRootDirectory();
    flakyExternalCAS.setBlockSizeForTesting(TEST_BLOCK_SIZE);
    ByteString blob = ByteString.copyFromUtf8("Flaky Entry");
    Digest blobDigest = DIGEST_UTIL.compute(blob);
    blobs.put(blobDigest, blob);
    Path path = flakyExternalCAS.put(blobDigest, false).path();
    assertThat(Files.exists(path)).isTrue(); // would not have been created if not valid
  }

  @Test
  public void newInputThrowsNoSuchFileExceptionWithoutDelegate() throws Exception {
    CASFileCache undelegatedCAS =
        new DirectoryEntryCFC(
            root,
            /* maxSizeInBytes= */ 24576,
            /* maxEntrySizeInBytes= */ 24576,
            /* hexBucketLevels= */ 1,
            expireService,
            /* accessRecorder= */ directExecutor(),
            storage,
            /* zstdBufferPool= */ null,
            /* onPut= */ digest -> {},
            /* onExpire= */ digests -> {},
            /* delegate= */ null,
            /* delegateSkipLoad= */ false,
            (compressor, digest, offset) -> {
              ByteString content = blobs.get(digest);
              if (content == null) {
                return fileCache.newTransparentInput(compressor, digest, offset);
              }
              checkArgument(compressor == Compressor.Value.IDENTITY);
              return content.substring((int) offset).newInput();
            });
    undelegatedCAS.initializeRootDirectory();
    undelegatedCAS.setBlockSizeForTesting(TEST_BLOCK_SIZE);
    ByteString blob = ByteString.copyFromUtf8("Missing Entry");
    Digest blobDigest = DIGEST_UTIL.compute(blob);
    assertThrows(
        NoSuchFileException.class,
        () -> undelegatedCAS.newInput(Compressor.Value.IDENTITY, blobDigest, /* offset= */ 0));
  }

  @Test
  public void testConcurrentWrites() throws Exception {
    ByteString blob = ByteString.copyFromUtf8("concurrent write");
    Digest digest = DIGEST_UTIL.compute(blob);
    UUID uuid = UUID.randomUUID();
    // The same instance of Write will be passed to both the threads, so that the both threads
    // try to get same output stream.
    Write write =
        fileCache.getWrite(
            Compressor.Value.IDENTITY, digest, uuid, RequestMetadata.getDefaultInstance());

    CyclicBarrier barrier = new CyclicBarrier(3);
    // awaitThreadState throws AssertionError, which is a Throwable but not an Exception.
    // Without capture, a timed-out probe inside write2 would silently terminate the worker
    // and the TERMINATED-state assertion below would pass on a thread that never reached
    // its critical-section call — masking the race this test exists to pin.
    AtomicReference<Throwable> write1Error = new AtomicReference<>();
    AtomicReference<Throwable> write2Error = new AtomicReference<>();

    Thread write1 =
        new Thread(
            () -> {
              try {
                ConcurrentWriteStreamObserver writeStreamObserver =
                    new ConcurrentWriteStreamObserver(write);
                writeStreamObserver.registerCallback();
                barrier.await(); // let both the threads get same write stream.
                writeStreamObserver.ownStream(); // let other thread get the ownership of stream
                writeStreamObserver.write(blob);
                writeStreamObserver.close();
              } catch (Exception e) {
                // write completion races are expected here, matching the original test's
                // tolerance of one path observing the write already done.
              } catch (Throwable t) {
                write1Error.set(t);
              }
            },
            "FirstRequest");
    Thread write2 =
        new Thread(
            () -> {
              try {
                ConcurrentWriteStreamObserver writeStreamObserver =
                    new ConcurrentWriteStreamObserver(write);
                writeStreamObserver.registerCallback();
                writeStreamObserver.ownStream(); // this thread will get the ownership of stream
                barrier.await(); // let both the threads get same write stream.
                // Wait for the first thread to actually park inside getOutput's wait()
                // rather than racing on a thread-state probe with a fixed timeout. The
                // observation is what the downstream assertion is anchored to.
                awaitThreadState(write1, WAITING, SECONDS.toNanos(5));
                writeStreamObserver.write(blob);
                writeStreamObserver.close();
              } catch (Exception e) {
                // see comment in write1.
              } catch (Throwable t) {
                write2Error.set(t);
              }
            },
            "SecondRequest");
    write1.start();
    write2.start();
    barrier.await(); // let both the requests reach the critical section

    // With the barrier handshake and the WAITING-state probe above, completion is
    // deterministic; the long timeout exists only to surface a hung rendezvous as a
    // distinct failure rather than a test runner timeout.
    write1.join(SECONDS.toMillis(30));
    write2.join(SECONDS.toMillis(30));

    assertThat(write1Error.get()).isNull();
    assertThat(write2Error.get()).isNull();
    assertThat(write1.getState()).isEqualTo(TERMINATED);
    assertThat(write2.getState()).isEqualTo(TERMINATED);
  }

  private static void awaitThreadState(Thread t, Thread.State target, long timeoutNanos) {
    long deadline = System.nanoTime() + timeoutNanos;
    while (t.getState() != target) {
      if (System.nanoTime() > deadline) {
        throw new AssertionError(
            "thread "
                + t.getName()
                + " did not reach state "
                + target
                + " within budget (current state: "
                + t.getState()
                + ")");
      }
      Thread.yield();
    }
  }

  // -- Block-aligned size tracking tests --

  @Test
  public void expireEntryShouldRemoveFromStorageWhenFallbackThrowsRuntimeException()
      throws IOException, InterruptedException {
    // expireEntryFallback runs BEFORE safeStorageRemoval. A RuntimeException from
    // delegate.getWrite (anything other than EntryLimitException, which is the only
    // checked-caught throw) used to escape past safeStorageRemoval, so the entry
    // strands in storage as a permanent zombie: state=EVICTED, file on disk,
    // unevictable + unacquirable. The fix wraps the fallback+invalidateWrite call in
    // log-and-continue so storage cleanup runs unconditionally. Pin the behavior by
    // forcing the fallback to throw RuntimeException and asserting the evicted entry
    // is gone from both storage and disk.
    byte[] data1 = new byte[22000]; // on-disk: 24576 = maxSizeInBytes
    Arrays.fill(data1, (byte) 1);
    ByteString blob1 = ByteString.copyFrom(data1);
    Digest digest1 = DIGEST_UTIL.compute(blob1);
    blobs.put(digest1, blob1);
    Path path1 = fileCache.put(digest1, false).path();
    decrementReference(path1);
    String key1 = path1.getFileName().toString();
    assertThat(storage.containsKey(key1)).isTrue();

    // Reconfigure delegate.getWrite to throw a RuntimeException (not the
    // EntryLimitException expireEntryFallback explicitly catches). Without the fix,
    // this would escape expireEntry's body before safeStorageRemoval. Use doThrow so
    // the override doesn't reset the other stubs (newInput/findMissingBlobs) the put
    // path needs.
    doThrow(new RuntimeException("injected fallback failure"))
        .when(delegate)
        .getWrite(
            any(Compressor.Value.class),
            any(Digest.class),
            any(UUID.class),
            any(RequestMetadata.class));

    // Second put triggers eviction of blob1.
    byte[] data2 = new byte[100];
    Arrays.fill(data2, (byte) 2);
    ByteString blob2 = ByteString.copyFrom(data2);
    Digest digest2 = DIGEST_UTIL.compute(blob2);
    blobs.put(digest2, blob2);
    Path path2 = fileCache.put(digest2, false).path();
    decrementReference(path2);

    // Eviction must have cleaned up storage despite the fallback throw.
    assertThat(storage.containsKey(key1)).isFalse();
    assertThat(Files.exists(path1)).isFalse();
    // The new entry made it in.
    assertThat(storage.containsKey(path2.getFileName().toString())).isTrue();
  }

  @Test
  public void evictionTriggersAtBlockAlignedThreshold() throws IOException, InterruptedException {
    // A large blob fills the cache (on-disk: 24576 = maxSizeInBytes).
    // Adding a small blob should trigger eviction.
    byte[] data1 = new byte[22000]; // on-disk: 24576 = maxSizeInBytes
    Arrays.fill(data1, (byte) 1);
    ByteString blob1 = ByteString.copyFrom(data1);
    Digest digest1 = DIGEST_UTIL.compute(blob1);
    blobs.put(digest1, blob1);
    Path path1 = fileCache.put(digest1, false).path();
    decrementReference(path1);

    assertThat(Files.exists(path1)).isTrue();
    assertThat(fileCache.size()).isEqualTo(estimateSizeOnDisk(22000));

    // Second blob triggers eviction: 24576 + 4096 = 28672 > 24576
    byte[] data2 = new byte[100];
    Arrays.fill(data2, (byte) 2);
    ByteString blob2 = ByteString.copyFrom(data2);
    Digest digest2 = DIGEST_UTIL.compute(blob2);
    blobs.put(digest2, blob2);
    fileCache.put(digest2, false);

    // Oldest unreferenced entry (blob1) should be evicted
    assertThat(Files.exists(path1)).isFalse();
  }

  @Test
  public void chargeAndDischargeSizeReturnsToZero() throws IOException, InterruptedException {
    // Write a blob, dereference it, write a cache-filling blob to evict it, verify size is exact.
    byte[] data = new byte[100];
    ByteString blob = ByteString.copyFrom(data);
    Digest digest = DIGEST_UTIL.compute(blob);
    blobs.put(digest, blob);
    Path path = fileCache.put(digest, false).path();
    decrementReference(path);

    // Write a large blob that forces eviction of the first
    byte[] bigData = new byte[22000]; // on-disk: 24576 = maxSizeInBytes
    Arrays.fill(bigData, (byte) 99);
    ByteString bigBlob = ByteString.copyFrom(bigData);
    Digest bigDigest = DIGEST_UTIL.compute(bigBlob);
    blobs.put(bigDigest, bigBlob);
    Path bigPath = fileCache.put(bigDigest, false).path();

    // First blob should be evicted, only big blob remains
    assertThat(Files.exists(path)).isFalse();
    assertThat(fileCache.size()).isEqualTo(estimateSizeOnDisk(22000));
  }

  @Test
  public void maxEntrySizeUsesLogicalSizeNotBlockAligned() throws Exception {
    // To test that maxEntrySizeInBytes compares against logical size (not on-disk size),
    // we need: logical < maxEntrySizeInBytes < on-disk.
    // Use maxEntrySizeInBytes=2048, blob=1500 bytes (logical), on-disk=4096.
    // If the check correctly uses logical size: 1500 < 2048 → accepted.
    // If it incorrectly used on-disk size: 4096 > 2048 → rejected.
    CASFileCache smallEntryCache =
        new DirectoryEntryCFC(
            root,
            /* maxSizeInBytes= */ 8192,
            /* maxEntrySizeInBytes= */ 2048,
            /* hexBucketLevels= */ 1,
            expireService,
            /* accessRecorder= */ directExecutor(),
            Maps.newConcurrentMap(),
            /* zstdBufferPool= */ null,
            /* onPut= */ digest -> {},
            /* onExpire= */ digests -> {},
            /* delegate= */ null,
            /* delegateSkipLoad= */ false,
            (compressor, digest, offset) -> {
              ByteString content = blobs.get(digest);
              checkArgument(content != null, "blob not found: %s", digest);
              checkArgument(compressor == Compressor.Value.IDENTITY);
              return content.substring((int) offset).newInput();
            });
    smallEntryCache.initializeRootDirectory();
    smallEntryCache.setBlockSizeForTesting(TEST_BLOCK_SIZE);

    byte[] data = new byte[1500]; // logical: 1500, on-disk: 4096
    ByteString blob = ByteString.copyFrom(data);
    Digest digest = DIGEST_UTIL.compute(blob);
    blobs.put(digest, blob);
    Path path = smallEntryCache.put(digest, false).path();
    assertThat(Files.exists(path)).isTrue();
    assertThat(smallEntryCache.size()).isEqualTo(estimateSizeOnDisk(1500));
  }

  // Entry state machine and atomic refcount tests live in EntryTest — the sentinel-entry
  // tests below stay here because they need the cache's header sentinel via the test fixture.

  @Test
  public void sentinelEntryRefcountOperationsShouldThrow() {
    // The LRU header is a SentinelEntry; it must never participate in refcount/state
    // transitions. A regression that let the sentinel be acquired/released/evicted would
    // corrupt the LRU's header invariant. Pin each override via the fileCache's header.
    Entry sentinel = fileCache.headerForTesting();
    assertThrows(UnsupportedOperationException.class, sentinel::tryAcquire);
    assertThrows(UnsupportedOperationException.class, sentinel::release);
    assertThrows(UnsupportedOperationException.class, sentinel::tryEvict);
    assertThrows(UnsupportedOperationException.class, sentinel::completeEviction);
    assertThrows(UnsupportedOperationException.class, sentinel::unlink);
    assertThrows(UnsupportedOperationException.class, () -> sentinel.recordAccess(sentinel));
    // isEvictable returns false unconditionally; not an exception, but pin the contract.
    assertThat(sentinel.isEvictable()).isFalse();
  }

  @Test
  public void sentinelEntryShouldNotSatisfyPlaceholderPredicate() {
    // SentinelEntry inherits the parent's no-arg Entry() constructor which sets
    // refCount=-1, so the inherited isPlaceholder() (refCount < 0) would otherwise be
    // true. The header is the LRU anchor, not the STARTING/STOPPED dummy returned by
    // getEntry() — a future change that exposes the header through getEntry() must not
    // have it pass isPlaceholder() and bypass the real-entry checks downstream.
    assertThat(fileCache.headerForTesting().isPlaceholder()).isFalse();
  }

  @Test
  public void refcountedInputStreamShouldReleaseRefcountWhenInnerCloseThrows() throws Exception {
    // The release-on-close discipline must hold even when the wrapped stream's close()
    // throws — a regression that ran releaseRef BEFORE super.close() or that propagated
    // the exception without entering the finally would leak refcounts on every failed
    // close. RefcountedInputStream uses try/finally so this can't happen — pin it.
    AtomicBoolean released = new AtomicBoolean(false);
    InputStream throwingClose =
        new InputStream() {
          @Override
          public int read() {
            return -1;
          }

          @Override
          public void close() throws IOException {
            throw new IOException("close failure");
          }
        };
    RefcountedInputStream wrapper =
        new RefcountedInputStream(throwingClose, () -> released.set(true));
    IOException thrown = null;
    try {
      wrapper.close();
    } catch (IOException ioe) {
      thrown = ioe;
    }
    assertThat(thrown).isNotNull();
    assertThat(thrown).hasMessageThat().contains("close failure");
    // The refcount must have been released regardless of the inner close failure.
    assertThat(released.get()).isTrue();
    // Second close must be a no-op (idempotent release).
    released.set(false);
    try {
      wrapper.close();
    } catch (IOException ignored) {
      // inner close throws again; we only care that releaseRef did not re-fire.
    }
    assertThat(released.get()).isFalse();
  }

  @Test
  public void refcountedInputStreamShouldReleaseRefcountWhenInnerCloseThrowsRuntimeException()
      throws Exception {
    // A wrapped stream's close() may throw a RuntimeException (e.g. JNI unwind from a
    // ZstdCompressingInputStream or any assertion in a guava-style invariant). Before the
    // fix, only IOException was caught — a RuntimeException escaped the try block and
    // skipped the closed.compareAndSet release path, pinning the Entry LIVE permanently.
    // The fix catches Throwable on super.close() so the refcount discipline holds.
    AtomicBoolean released = new AtomicBoolean(false);
    RuntimeException innerThrow = new RuntimeException("native unwind");
    InputStream throwingClose =
        new InputStream() {
          @Override
          public int read() {
            return -1;
          }

          @Override
          public void close() {
            throw innerThrow;
          }
        };
    RefcountedInputStream wrapper =
        new RefcountedInputStream(throwingClose, () -> released.set(true));
    RuntimeException thrown = null;
    try {
      wrapper.close();
    } catch (RuntimeException re) {
      thrown = re;
    }
    assertThat(thrown).isSameInstanceAs(innerThrow);
    // The release ran despite the non-IOException throw.
    assertThat(released.get()).isTrue();
    // Second close is idempotent: releaseRef must not re-fire even though the inner
    // close still throws.
    released.set(false);
    try {
      wrapper.close();
    } catch (RuntimeException ignored) {
      // inner close throws again; we only care that releaseRef did not re-fire.
    }
    assertThat(released.get()).isFalse();
  }

  @Test
  @SuppressWarnings("GuardedBy")
  public void evictionShouldDischargeExactlyOnceWhenDirectoryExpirationFails() throws Exception {
    // Cluster F/G regression pin: dischargeEntry calls discharge() and then rethrows
    // a directory-expiration failure. Without setting discharged=true BEFORE unlinkEntry,
    // the caller's !completed finally would observe discharged=false and decrement
    // totalBytes a second time — recreating the useless-eviction-loop dynamic Phase 1
    // was written to eliminate.
    //
    // Build a custom CFC whose unlinkAndExpireDirectories returns a failed future,
    // then trigger the failure path via referenceIfExists on an entry whose file no
    // longer exists. Assert that totalBytes returns to its pre-eviction value (single
    // discharge), not a negative drift (double discharge).
    ConcurrentMap<String, Entry> failingStorage = Maps.newConcurrentMap();
    Path failingRoot = root.resolveSibling("failing-cache");
    DirectoryEntryCFC failingCache =
        new DirectoryEntryCFC(
            failingRoot,
            /* maxSizeInBytes= */ 24576,
            /* maxEntrySizeInBytes= */ 24576,
            /* hexBucketLevels= */ 1,
            expireService,
            /* accessRecorder= */ directExecutor(),
            failingStorage,
            /* zstdBufferPool= */ null,
            onPut,
            onExpire,
            delegate,
            /* delegateSkipLoad= */ false,
            (compressor, digest, offset) -> {
              throw new NoSuchFileException("not used");
            }) {
          @Override
          protected List<ListenableFuture<Void>> unlinkAndExpireDirectories(
              Entry entry, ExecutorService service) {
            return ImmutableList.of(
                Futures.immediateFailedFuture(new IOException("injected expiration failure")));
          }
        };
    failingCache.initializeRootDirectory();
    failingCache.setBlockSizeForTesting(TEST_BLOCK_SIZE);

    // Synthesize one unreferenced entry: orphan + linkUnreferenced under lruLock, plus
    // totalBytes += entry.size. existsDeadline starts in the past so entryExists() goes
    // straight to Files.exists (which returns false: the file was never created).
    // Use a digest-shaped key so HexBucketEntryPathStrategy's hex-prefix regex passes.
    ByteString syntheticContent = ByteString.copyFromUtf8("synthetic-blob-content");
    Digest syntheticDigest = DIGEST_UTIL.compute(syntheticContent);
    String key = CASFileCache.getKey(syntheticDigest, /* isExecutable= */ false);
    long size = 4096L;
    Entry e = Entry.orphan(key, size, Deadline.after(-1, SECONDS));
    failingStorage.put(key, e);
    failingCache.setSizeInBytesForTesting(size);
    failingCache.lruLockForTesting().lock();
    try {
      failingCache.linkUnreferenced(e);
    } finally {
      failingCache.lruLockForTesting().unlock();
    }
    assertThat(e.isLinked()).isTrue();
    assertThat(failingCache.size()).isEqualTo(size);
    long unreferencedBefore = failingCache.unreferencedEntryCount();

    IOException thrown = null;
    try {
      failingCache.referenceIfExists(key);
    } catch (IOException ex) {
      thrown = ex;
    }

    assertThat(thrown).isNotNull();
    // The expirationException is wrapped as IOException via unlinkEntry's catch.
    assertThat(thrown).hasCauseThat().hasMessageThat().contains("injected expiration failure");
    // Single discharge: totalBytes back to zero, not negative. Without the fix, this
    // would be -size because dischargeEntry's internal discharge AND the finally's
    // discharge would both run.
    assertThat(failingCache.size()).isEqualTo(0L);
    assertThat(failingStorage.containsKey(key)).isFalse();
    assertThat(e.state()).isEqualTo(Entry.State.EVICTED);
    assertThat(e.isLinked()).isFalse();
    assertThat(failingCache.unreferencedEntryCount()).isEqualTo(unreferencedBefore - 1);
  }

  @Test
  @SuppressWarnings("GuardedBy")
  public void linkUnreferencedShouldRefuseReferencedEntry() throws Exception {
    // Race-safety pin: linkUnreferenced must observe refCount() == 0 under lruLock
    // before linking, so a release-then-acquire race (e.release() commits 1->0 outside
    // lruLock, then a concurrent tryAcquire bumps to 1 before the batched
    // linkUnreferenced runs) does not publish a referenced entry onto the LRU tail.
    Entry e = Entry.orphan("k", 7L, Deadline.after(10, SECONDS));
    e.tryAcquire(); // simulate the racing acquirer winning the 0 -> 1 transition.
    assertThat(e.refCount()).isEqualTo(1);

    long unreferencedBefore = fileCache.unreferencedEntryCount();
    fileCache.lruLockForTesting().lock();
    try {
      fileCache.linkUnreferenced(e);
    } finally {
      fileCache.lruLockForTesting().unlock();
    }
    // Must not have linked the referenced entry into the LRU. The unreferencedCount
    // assertion catches a regression that incremented the counter unconditionally
    // while still skipping the addBefore call — isLinked() alone would false-pass.
    assertThat(e.isLinked()).isFalse();
    assertThat(fileCache.unreferencedEntryCount()).isEqualTo(unreferencedBefore);
  }

  @Test
  @SuppressWarnings("GuardedBy")
  public void linkUnreferencedShouldRefuseNonLiveEntry() throws Exception {
    // Pin the state-guard arm: an EVICTING entry has already been chosen as a victim;
    // re-linking it would let waitForLastUnreferencedEntry pick the same Entry twice.
    Entry e = Entry.orphan("k", 7L, Deadline.after(10, SECONDS));
    assertThat(e.tryEvict()).isTrue();
    assertThat(e.state()).isEqualTo(Entry.State.EVICTING);
    assertThat(e.refCount()).isEqualTo(0);

    fileCache.lruLockForTesting().lock();
    try {
      fileCache.linkUnreferenced(e);
    } finally {
      fileCache.lruLockForTesting().unlock();
    }
    assertThat(e.isLinked()).isFalse();
  }

  @Test
  @SuppressWarnings("GuardedBy")
  public void linkUnreferencedShouldRefuseAlreadyLinkedEntry() throws Exception {
    // Pin the linkage-guard arm: a double-link corrupts the doubly-linked list and
    // double-counts unreferencedCount. The first call publishes; the second must be
    // a no-op even with state==LIVE && refCount==0.
    Entry e = Entry.orphan("k", 7L, Deadline.after(10, SECONDS));
    long before = fileCache.unreferencedEntryCount();
    fileCache.lruLockForTesting().lock();
    try {
      fileCache.linkUnreferenced(e);
      assertThat(e.isLinked()).isTrue();
      assertThat(fileCache.unreferencedEntryCount()).isEqualTo(before + 1);

      fileCache.linkUnreferenced(e);
      assertThat(e.isLinked()).isTrue();
      assertThat(fileCache.unreferencedEntryCount()).isEqualTo(before + 1);
    } finally {
      fileCache.lruLockForTesting().unlock();
    }
  }

  @Test
  @SuppressWarnings("GuardedBy")
  public void unlinkReferencedShouldBeNoOpOnUnlinkedEntry() throws Exception {
    // Pin the Cluster E NPE fix: a release-then-acquire race can leave an entry with
    // refCount==0 && !isLinked() (linkUnreferenced's recheck skipped publication
    // because a concurrent tryAcquire bumped refCount back to 1, then released).
    // tryEvict succeeds on such an entry; the subclass unlinkAndExpireDirectories
    // override routes through unlinkReferenced, which MUST gate on isLinked() so
    // entry.unlink() does not NPE on null before/after links.
    Entry e = Entry.orphan("k", 7L, Deadline.after(10, SECONDS));
    assertThat(e.isLinked()).isFalse();
    long unreferencedBefore = fileCache.unreferencedEntryCount();

    fileCache.lruLockForTesting().lock();
    try {
      fileCache.unlinkReferenced(e); // must not throw, must not decrement.
    } finally {
      fileCache.lruLockForTesting().unlock();
    }

    assertThat(e.isLinked()).isFalse();
    assertThat(fileCache.unreferencedEntryCount()).isEqualTo(unreferencedBefore);
  }

  @Test
  public void concurrentAcquireReleaseShouldConvergeToInitialRefCount() throws Exception {
    Entry e = new Entry("k", 7L, Deadline.after(10, SECONDS));
    int threadCount = 16;
    int iterationsPerThread = 5000;
    CyclicBarrier start = new CyclicBarrier(threadCount);
    List<Thread> threads = new ArrayList<>();
    AtomicReference<Throwable> error = new AtomicReference<>();
    // Tracking the sum of previous-refcount values + final 1->0 transitions catches
    // off-by-one drift the final-state check alone can miss.
    LongAdder previousSum = new LongAdder();
    LongAdder acquireCount = new LongAdder();
    LongAdder finalOneTransitions = new LongAdder();
    for (int i = 0; i < threadCount; i++) {
      Thread t =
          new Thread(
              () -> {
                try {
                  start.await();
                  for (int j = 0; j < iterationsPerThread; j++) {
                    int prev = e.tryAcquire();
                    assertThat(prev).isAtLeast(1); // initial refcount is 1, never EVICTING
                    previousSum.add(prev);
                    acquireCount.increment();
                    boolean wasFinal = e.release();
                    if (wasFinal) {
                      finalOneTransitions.increment();
                    }
                  }
                } catch (Throwable th) {
                  error.set(th);
                }
              });
      // daemon so a stuck worker doesn't keep the JVM alive past test failure.
      t.setDaemon(true);
      threads.add(t);
    }
    for (Thread t : threads) {
      t.start();
    }
    for (Thread t : threads) {
      t.join(SECONDS.toMillis(30));
      if (t.isAlive()) {
        // Interrupt every worker before fail so the other surviving threads don't keep
        // hammering the entry through tearDown and cascade noise into subsequent tests.
        for (Thread other : threads) {
          other.interrupt();
        }
        fail("worker thread did not terminate within 30s");
      }
    }
    assertThat(error.get()).isNull();
    // After all pairs settle, only the constructor's initial reference remains.
    assertThat(e.refCount()).isEqualTo(1);
    assertThat(e.state()).isEqualTo(Entry.State.LIVE);
    // Total acquires must be threadCount * iterationsPerThread.
    long totalAcquires = (long) threadCount * iterationsPerThread;
    assertThat(acquireCount.sum()).isEqualTo(totalAcquires);
    // Sum of previous-refcount values must be at least totalAcquires (each acquire
    // observes refcount >= 1 because the constructor reference is held throughout).
    assertThat(previousSum.sum()).isAtLeast(totalAcquires);
    // Final 1 -> 0 transitions should never fire: every release happens while the
    // constructor's reference (refCount = 1) is still held, so the minimum refcount
    // observed by any release is 2 -> 1. A regression that double-released would
    // make this counter positive.
    assertThat(finalOneTransitions.sum()).isEqualTo(0);
  }

  @Test
  public void referenceIfExistsShouldUseAtomicAcquireAndReturnTrueOnSuccess() throws Exception {
    ByteString content = ByteString.copyFromUtf8("ref-check");
    Digest digest = DIGEST_UTIL.compute(content);
    blobs.put(digest, content);
    Path path = fileCache.put(digest, false).path();
    decrementReference(path); // drop to refCount 0 so the entry is on the LRU.
    String key = path.getFileName().toString();
    Entry e = storage.get(key);
    assertThat(e.refCount()).isEqualTo(0);
    assertThat(e.isLinked()).isTrue();

    // Call the protected referenceIfExists directly (same-package access). This pins
    // the atomic 0 -> 1 revive: tryAcquire success + unlinkReferenced under lruLock.
    boolean acquired = fileCache.referenceIfExists(key);
    assertThat(acquired).isTrue();
    assertThat(e.refCount()).isEqualTo(1);
    // After a 0 -> 1 revival the entry must be unlinked from the LRU.
    assertThat(e.isLinked()).isFalse();
    decrementReference(path);
  }

  @Test
  public void referenceIfExistsConcurrentAcquiresShouldUnlinkExactlyOnce() throws Exception {
    // Atomicity pin: many concurrent referenceIfExists calls on a refCount==0 entry
    // must all succeed (the entry is LIVE), but exactly one acquirer wins the 0->1
    // transition and drives the unlink. A regression to non-atomic check-then-increment
    // could let two threads both observe previous==0 and both attempt the unlink,
    // double-decrementing unreferencedCount. The single-threaded happy-path test
    // above cannot distinguish atomic from non-atomic on this dimension.
    ByteString content = ByteString.copyFromUtf8("ref-atomic-concurrent");
    Digest digest = DIGEST_UTIL.compute(content);
    blobs.put(digest, content);
    Path path = fileCache.put(digest, false).path();
    decrementReference(path);
    String key = path.getFileName().toString();
    Entry e = storage.get(key);
    assertThat(e.refCount()).isEqualTo(0);
    assertThat(e.isLinked()).isTrue();
    long unreferencedBefore = fileCache.unreferencedEntryCount();

    int threadCount = 16;
    CyclicBarrier start = new CyclicBarrier(threadCount);
    AtomicInteger successCount = new AtomicInteger();
    AtomicReference<Throwable> error = new AtomicReference<>();
    List<Thread> threads = new ArrayList<>();
    for (int i = 0; i < threadCount; i++) {
      Thread t =
          new Thread(
              () -> {
                try {
                  start.await();
                  if (fileCache.referenceIfExists(key)) {
                    successCount.incrementAndGet();
                  }
                } catch (Throwable th) {
                  error.compareAndSet(null, th);
                }
              });
      t.setDaemon(true);
      threads.add(t);
    }
    for (Thread t : threads) {
      t.start();
    }
    for (Thread t : threads) {
      t.join(SECONDS.toMillis(10));
      if (t.isAlive()) {
        for (Thread other : threads) {
          other.interrupt();
        }
        fail("worker thread did not terminate within 10s");
      }
    }

    assertThat(error.get()).isNull();
    assertThat(successCount.get()).isEqualTo(threadCount);
    assertThat(e.refCount()).isEqualTo(threadCount);
    // The 0 -> 1 winner drove the unlink; all subsequent acquirers found
    // !isLinked. unreferencedCount drops by exactly one — the signature of a single
    // unlink, not multiple competing unlinks.
    assertThat(e.isLinked()).isFalse();
    assertThat(fileCache.unreferencedEntryCount()).isEqualTo(unreferencedBefore - 1);

    // Drain references so the entry is evictable for tearDown.
    for (int i = 0; i < threadCount; i++) {
      decrementReference(path);
    }
  }

  @Test
  public void lockFreeReadersShouldNotBlockEvictionLock() throws Exception {
    // Populate one entry. While a thread holds lruLock for an extended period, the public
    // metric readers (size(), getEvictedCount(), getEvictedSize(), entryCount()) must
    // not enqueue on the lock. A regression that re-introduced synchronized() on a
    // reader would show up as the reader thread queueing on lruLock while the holder
    // sits on it — `lock.getQueueLength() > 0` is the binary check, not wall-clock.
    //
    // The readers run on a separate thread so a regression manifests as a bounded
    // join timeout with a queue-length-observed-non-zero diagnostic rather than the
    // test thread blocking inside size() until bazel's outer timeout (no diagnostic).
    ByteString content = ByteString.copyFromUtf8("metric-read");
    Digest digest = DIGEST_UTIL.compute(content);
    blobs.put(digest, content);
    fileCache.put(digest, false);

    java.util.concurrent.locks.ReentrantLock lock = fileCache.lruLockForTesting();
    CountDownLatch lockHeld = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    CountDownLatch readsDone = new CountDownLatch(1);
    AtomicReference<Throwable> readerError = new AtomicReference<>();
    Thread holder =
        new Thread(
            () -> {
              lock.lock();
              try {
                lockHeld.countDown();
                release.await();
              } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
              } finally {
                lock.unlock();
              }
            },
            "lru-lock-holder");
    Thread reader =
        new Thread(
            () -> {
              try {
                for (int i = 0; i < 200; i++) {
                  fileCache.size();
                  fileCache.entryCount();
                  fileCache.getEvictedCount();
                  fileCache.getEvictedSize();
                }
              } catch (Throwable th) {
                readerError.compareAndSet(null, th);
              } finally {
                readsDone.countDown();
              }
            },
            "lock-free-reader");
    reader.setDaemon(true);
    holder.setDaemon(true);
    holder.start();
    try {
      assertThat(lockHeld.await(5, SECONDS)).isTrue();
      reader.start();
      // Poll the lock's queue length while the reader runs. A regression that made
      // any reader acquire lruLock would park the reader thread on this lock, and
      // getQueueLength would observe it before the await(1ms) returns true.
      int maxQueueLengthObserved = 0;
      long deadlineNanos = System.nanoTime() + SECONDS.toNanos(5);
      while (!readsDone.await(1, MILLISECONDS)) {
        int q = lock.getQueueLength();
        if (q > maxQueueLengthObserved) {
          maxQueueLengthObserved = q;
        }
        if (System.nanoTime() > deadlineNanos) {
          break;
        }
      }
      assertThat(readerError.get()).isNull();
      assertWithMessage("reader did not complete within 5s — likely queued on lruLock")
          .that(readsDone.getCount())
          .isEqualTo(0);
      assertThat(maxQueueLengthObserved).isEqualTo(0);
    } finally {
      release.countDown();
      holder.join(SECONDS.toMillis(5));
      reader.join(SECONDS.toMillis(5));
    }
    assertThat(lock.getQueueLength()).isEqualTo(0);
  }

  @Test
  public void newLocalInputShouldHoldRefcountAcrossOpenLifetime() throws Exception {
    ByteString content = ByteString.copyFromUtf8("read-path-refcount");
    Digest digest = DIGEST_UTIL.compute(content);
    blobs.put(digest, content);
    Path path = fileCache.put(digest, false).path();
    decrementReference(path); // drop initial put refcount so the entry is evictable.

    String key = path.getFileName().toString();
    Entry e = storage.get(key);
    assertThat(e.refCount()).isEqualTo(0);

    InputStream in = fileCache.newInput(Compressor.Value.IDENTITY, digest, 0);
    try {
      // While the stream is open the entry must be held by exactly one reference and
      // therefore unlinked from the LRU.
      assertThat(e.refCount()).isEqualTo(1);
      assertThat(e.isLinked()).isFalse();
      // Read all bytes — the open fd survives even if eviction unlinks the file.
      ByteString actual = ByteString.readFrom(in);
      assertThat(actual).isEqualTo(content);
    } finally {
      in.close();
    }
    // After close the refcount returns to zero and the entry is back on the LRU.
    assertThat(e.refCount()).isEqualTo(0);
    assertThat(e.isLinked()).isTrue();
  }

  @Test
  public void newLocalInputCloseShouldBeIdempotentForRefcountRelease() throws Exception {
    ByteString content = ByteString.copyFromUtf8("close-idempotent");
    Digest digest = DIGEST_UTIL.compute(content);
    blobs.put(digest, content);
    Path path = fileCache.put(digest, false).path();
    decrementReference(path);

    String key = path.getFileName().toString();
    Entry e = storage.get(key);

    InputStream in = fileCache.newInput(Compressor.Value.IDENTITY, digest, 0);
    assertThat(e.refCount()).isEqualTo(1);
    in.close();
    assertThat(e.refCount()).isEqualTo(0);
    in.close(); // second close must not double-release.
    assertThat(e.refCount()).isEqualTo(0);
  }

  @Test
  public void newLocalInputUnsupportedCompressorShouldNotLeakRefcount() throws Exception {
    // Pin the Cluster D validation hoist: an unsupported Compressor.Value must
    // fail with IllegalArgumentException BEFORE any tryAcquire runs. Without the
    // hoist, the inner compressorInputStream's checkArgument would throw past the
    // outer IOException catch, leaving the entry pinned LIVE with no live holder —
    // unevictable and unacquirable forever.
    ByteString content = ByteString.copyFromUtf8("bad-compressor");
    Digest digest = DIGEST_UTIL.compute(content);
    blobs.put(digest, content);
    Path path = fileCache.put(digest, false).path();
    decrementReference(path); // drop initial put refcount so the entry is at 0.

    String key = path.getFileName().toString();
    Entry e = storage.get(key);
    assertThat(e.refCount()).isEqualTo(0);
    assertThat(e.state()).isEqualTo(Entry.State.LIVE);

    try {
      fileCache.newInput(Compressor.Value.DEFLATE, digest, 0);
      fail("expected IllegalArgumentException for unsupported compressor");
    } catch (IllegalArgumentException expected) {
      // expected.
    }

    // Refcount unchanged: the validation rejected the call before tryAcquire ran.
    // A leak here would manifest as refCount == 1, pinning the entry LIVE forever.
    assertThat(e.refCount()).isEqualTo(0);
    assertThat(e.state()).isEqualTo(Entry.State.LIVE);
    assertThat(e.isLinked()).isTrue();
  }

  // Sequential acquirer/evictor outcomes are pinned in EntryTest. The A3 race-handling
  // path (acquirer's state recheck after refCount increment, when the evictor's CAS landed
  // mid-acquire) is not exercised by deterministic tests — it would require either a
  // brute-force racing test or invasive test hooks on Entry's private state.

  @Test
  public void concurrentChargersShouldNotStallUnderSustainedPressure() throws Exception {
    // Pin the 2026-04-13 incident pattern: pre-Phase-1, a thread holding the global
    // synchronized(this) monitor across slow I/O wedged every other charger + reader,
    // producing a 25-minute cluster-wide outage. The regression assertion is a fixed
    // workload that must complete within a generous wall-clock budget: if any monitor
    // wedges the hot path, the latch won't reach zero and we fail with a thread-dump.
    // findDeadlockedThreads() distinguishes a literal deadlock from a slow stall.
    //
    // Pre-populate ALL digests into `blobs` up front so we never mutate the test's
    // HashMap while concurrent fileCache.put threads read it.
    final int chargerThreads = 4;
    final int readerThreads = 2;
    final int chargesPerThread = 200;
    final int readsPerThread = 200;
    final int readableCount = 4;

    List<List<Digest>> chargerDigests = new ArrayList<>();
    for (int t = 0; t < chargerThreads; t++) {
      List<Digest> own = new ArrayList<>();
      for (int s = 0; s < chargesPerThread; s++) {
        // 30-byte content fits in one 4k block, so each put adds 4096 bytes to the
        // 24576-byte cache; eviction kicks in after ~6 puts.
        byte[] bytes = new byte[30];
        bytes[0] = (byte) t;
        bytes[1] = (byte) s;
        bytes[2] = (byte) (s >>> 8);
        ByteString content = ByteString.copyFrom(bytes);
        Digest d = DIGEST_UTIL.compute(content);
        blobs.put(d, content);
        own.add(d);
      }
      chargerDigests.add(own);
    }

    List<Digest> readableDigests = new ArrayList<>();
    for (int i = 0; i < readableCount; i++) {
      byte[] bytes = new byte[10];
      bytes[0] = (byte) (0x80 | i);
      ByteString content = ByteString.copyFrom(bytes);
      Digest d = DIGEST_UTIL.compute(content);
      blobs.put(d, content);
      Path p = fileCache.put(d, false).path();
      decrementReference(p);
      readableDigests.add(d);
    }

    AtomicReference<Throwable> error = new AtomicReference<>();
    CountDownLatch done = new CountDownLatch(chargerThreads + readerThreads);
    // Track successful reads so a regression that made every read silently fall into
    // the NoSuchFileException branch (e.g. a refcount-pin bug that pinned readers'
    // entries past their own decrement and made them evictable mid-read) would not
    // pass this test with zero reads actually exercising the refcount-protected path.
    LongAdder successfulReads = new LongAdder();
    // Starting gate so all workers begin contending simultaneously — sequential
    // Thread.start would let early threads finish their loops before late threads
    // even begin, materially shrinking the contention window the test exists for.
    CyclicBarrier start = new CyclicBarrier(chargerThreads + readerThreads);
    List<Thread> threads = new ArrayList<>();
    for (int t = 0; t < chargerThreads; t++) {
      final List<Digest> myDigests = chargerDigests.get(t);
      threads.add(
          new Thread(
              () -> {
                try {
                  start.await();
                  for (int s = 0; s < chargesPerThread; s++) {
                    Digest d = myDigests.get(s);
                    Path p = fileCache.put(d, false).path();
                    decrementReference(p);
                  }
                } catch (Throwable th) {
                  error.compareAndSet(null, th);
                } finally {
                  done.countDown();
                }
              },
              "charger-" + t));
    }
    for (int t = 0; t < readerThreads; t++) {
      threads.add(
          new Thread(
              () -> {
                try {
                  start.await();
                  for (int s = 0; s < readsPerThread; s++) {
                    Digest d = readableDigests.get(s % readableDigests.size());
                    try (InputStream in = fileCache.newInput(Compressor.Value.IDENTITY, d, 0)) {
                      ByteStreams.exhaust(in);
                      successfulReads.increment();
                    } catch (NoSuchFileException nsfe) {
                      // entry may have been evicted under pressure; expected.
                    }
                  }
                } catch (Throwable th) {
                  error.compareAndSet(null, th);
                } finally {
                  done.countDown();
                }
              },
              "reader-" + t));
    }
    for (Thread t : threads) {
      // Daemon so a stall doesn't keep the JVM alive past the test; the explicit
      // interrupt on fail() below still bounds cleanup time within this test.
      t.setDaemon(true);
      t.start();
    }
    // Generous wall-clock budget: the workload completes in ~1-2s locally; 30s covers
    // slow CI nodes. A timeout here is the deterministic signal that monitor
    // wedging has regressed.
    boolean finished = done.await(30, SECONDS);
    if (!finished) {
      ThreadMXBean tmx = ManagementFactory.getThreadMXBean();
      long[] deadlocked = tmx.findDeadlockedThreads();
      StringBuilder failure =
          new StringBuilder(
              "workload did not complete within 30s — this is the 2026-04-13 stall pattern\n");
      if (deadlocked != null && deadlocked.length > 0) {
        failure.append("findDeadlockedThreads reported ").append(deadlocked.length).append(":\n");
        for (ThreadInfo info : tmx.getThreadInfo(deadlocked, true, true)) {
          failure.append(info).append('\n');
        }
      } else {
        failure.append("no literal deadlock detected; live-lock or held-too-long stall.\n");
        for (Thread t : threads) {
          failure.append(t.getName()).append(' ').append(t.getState()).append('\n');
        }
      }
      // Surface any worker-side throwable that may have caused the stall by leaving
      // other workers parked on the CyclicBarrier — without this, fail() would only
      // report the timeout and hide the real cause.
      Throwable workerError = error.get();
      if (workerError != null) {
        failure.append("worker error captured before stall: ").append(workerError).append('\n');
      }
      // Interrupt before fail() so the threads don't keep hammering fileCache while
      // tearDown() shuts down the put service and wipes the root — subsequent tests
      // would otherwise see cascading errors that obscure this stall.
      for (Thread t : threads) {
        t.interrupt();
      }
      fail(failure.toString());
    }
    for (Thread t : threads) {
      t.join(SECONDS.toMillis(1));
      assertThat(t.isAlive()).isFalse();
    }
    assertThat(error.get()).isNull();
    // At least one read must have succeeded — otherwise a regression that made
    // every read fall to NoSuchFileException (the eviction-during-read failure
    // mode this test's read path exercises) would pass without ever exercising
    // the refcount-protected newLocalInput happy path.
    assertWithMessage("no reads succeeded — regression in read-path refcount discipline?")
        .that(successfulReads.sum())
        .isGreaterThan(0L);
  }

  @Test
  @SuppressWarnings("GuardedBy")
  public void unreferencedCountShouldMatchLruSizeAfterConcurrentPutRelease() throws Exception {
    // Cluster A invariant pin: at quiescence, unreferencedCount must equal the number
    // of entries actually on the LRU. The orphan/acquirer/evictor race had paths
    // where the counter could drift (missed increment when linkUnreferenced refused
    // a referenced entry; missed decrement when unlinkReferenced ran against an
    // already-unlinked entry). Drift is invisible to throughput or stalls but
    // corrupts size() reporting and eventually starves eviction.
    //
    // The workload pre-populates the shared digests, then hammers them through the
    // referenceIfExists -> release -> linkUnreferenced path that Cluster A's race
    // operates on. Without pre-population, a separate race in charge() between
    // the optimistic totalBytes.add and storage.put trips waitForLastUnreferencedEntry
    // before the new entry is published — not the invariant under test here.
    final int workerThreads = 4;
    final int opsPerThread = 200;
    final int sharedKeyCount = 4;
    List<Digest> sharedDigests = new ArrayList<>();
    for (int i = 0; i < sharedKeyCount; i++) {
      byte[] bytes = new byte[16];
      bytes[0] = (byte) (0xA0 | i);
      ByteString content = ByteString.copyFrom(bytes);
      Digest d = DIGEST_UTIL.compute(content);
      blobs.put(d, content);
      sharedDigests.add(d);
      Path p = fileCache.put(d, false).path();
      decrementReference(p);
    }

    AtomicReference<Throwable> error = new AtomicReference<>();
    CyclicBarrier start = new CyclicBarrier(workerThreads);
    CountDownLatch done = new CountDownLatch(workerThreads);
    List<Thread> workers = new ArrayList<>();
    for (int t = 0; t < workerThreads; t++) {
      workers.add(
          new Thread(
              () -> {
                try {
                  start.await();
                  for (int i = 0; i < opsPerThread; i++) {
                    Digest d = sharedDigests.get(i % sharedDigests.size());
                    Path p = fileCache.put(d, false).path();
                    decrementReference(p);
                  }
                } catch (Throwable th) {
                  error.compareAndSet(null, th);
                } finally {
                  done.countDown();
                }
              },
              "uref-worker-" + t));
    }
    for (Thread w : workers) {
      w.setDaemon(true);
      w.start();
    }
    boolean finished = done.await(30, SECONDS);
    if (!finished) {
      // Interrupt before fail() so workers don't keep hammering fileCache through
      // tearDown() and cascade errors into subsequent tests — matching the
      // Cluster F/G discipline established for the sibling stall-regression tests.
      for (Thread w : workers) {
        w.interrupt();
      }
      Throwable workerError = error.get();
      fail(
          "workload did not complete within 30s"
              + (workerError != null ? "; worker error: " + workerError : ""));
    }
    for (Thread w : workers) {
      w.join(SECONDS.toMillis(1));
      assertThat(w.isAlive()).isFalse();
    }
    assertThat(error.get()).isNull();

    // All decrementReference calls have completed → no held refcounts → every entry
    // in storage must be on the LRU and unreferencedCount must agree.
    fileCache.lruLockForTesting().lock();
    try {
      long onLru = 0;
      Entry header = fileCache.headerForTesting();
      for (Entry e = header.after; e != header; e = e.after) {
        onLru++;
        // Defense in depth: every LRU entry must be unreferenced and LIVE.
        assertThat(e.refCount()).isEqualTo(0);
        assertThat(e.state()).isEqualTo(Entry.State.LIVE);
      }
      assertThat(fileCache.unreferencedEntryCount()).isEqualTo(onLru);
    } finally {
      fileCache.lruLockForTesting().unlock();
    }
  }

  static class ConcurrentWriteStreamObserver {
    Write write;
    FeedbackOutputStream out;

    ConcurrentWriteStreamObserver(Write write) {
      this.write = write;
    }

    void registerCallback() {
      Futures.addCallback(
          write.getFuture(),
          new FutureCallback<Long>() {
            @Override
            public void onSuccess(Long committedSize) {
              commit();
            }

            @Override
            public void onFailure(Throwable t) {
              // do nothing
            }
          },
          directExecutor());
    }

    synchronized void ownStream() throws Exception {
      this.out = write.getOutput(10, MILLISECONDS, () -> {});
    }

    /**
     * Request 1 may invoke this method for request 2 or vice-versa via callback on
     * write.getFuture(). Synchronization is necessary to prevent conflicts when this method is
     * called simultaneously by different threads.
     */
    synchronized void commit() {
      // critical section
    }

    void write(ByteString data) throws IOException {
      data.writeTo(out);
    }

    void close() throws IOException {
      out.close();
    }
  }

  @RunWith(JUnit4.class)
  @SuppressWarnings("PMD.TestClassWithoutTestCases")
  public static class NativeFileDirsIndexInMemoryCASFileCacheTest extends CASFileCacheTest {
    public NativeFileDirsIndexInMemoryCASFileCacheTest() throws IOException {
      super(createTempDirectory(), /* storeFileDirsIndexInMemory= */ true);
    }

    private static Path createTempDirectory() throws IOException {
      if (Thread.interrupted()) {
        throw new RuntimeException(new InterruptedException());
      }
      return Files.createTempDirectory("native-cas-test");
    }
  }

  @RunWith(JUnit4.class)
  @SuppressWarnings("PMD.TestClassWithoutTestCases")
  public static class NativeFileDirsIndexInSqliteCASFileCacheTest extends CASFileCacheTest {
    public NativeFileDirsIndexInSqliteCASFileCacheTest() throws IOException {
      super(createTempDirectory(), /* storeFileDirsIndexInMemory= */ false);
    }

    private static Path createTempDirectory() throws IOException {
      if (Thread.interrupted()) {
        throw new RuntimeException(new InterruptedException());
      }
      return Files.createTempDirectory("native-cas-test");
    }
  }

  @RunWith(JUnit4.class)
  @SuppressWarnings("PMD.TestClassWithoutTestCases")
  public static class OsXFileDirsIndexInMemoryCASFileCacheTest extends CASFileCacheTest {
    public OsXFileDirsIndexInMemoryCASFileCacheTest() {
      super(
          Iterables.getFirst(
              Jimfs.newFileSystem(
                      Configuration.osX().toBuilder()
                          .setAttributeViews("basic", "owner", "posix", "unix")
                          .build())
                  .getRootDirectories(),
              null),
          /* storeFileDirsIndexInMemory= */ true);
    }
  }

  @RunWith(JUnit4.class)
  @SuppressWarnings("PMD.TestClassWithoutTestCases")
  public static class OsXFileDirsIndexInSqliteCASFileCacheTest extends CASFileCacheTest {
    public OsXFileDirsIndexInSqliteCASFileCacheTest() {
      super(
          Iterables.getFirst(
              Jimfs.newFileSystem(
                      Configuration.osX().toBuilder()
                          .setAttributeViews("basic", "owner", "posix", "unix")
                          .build())
                  .getRootDirectories(),
              null),
          /* storeFileDirsIndexInMemory= */ false);
    }
  }

  @RunWith(JUnit4.class)
  @SuppressWarnings("PMD.TestClassWithoutTestCases")
  public static class UnixFileDirsIndexInMemoryCASFileCacheTest extends CASFileCacheTest {
    public UnixFileDirsIndexInMemoryCASFileCacheTest() {
      super(
          Iterables.getFirst(
              Jimfs.newFileSystem(
                      Configuration.unix().toBuilder()
                          .setAttributeViews("basic", "owner", "posix", "unix")
                          .build())
                  .getRootDirectories(),
              null),
          /* storeFileDirsIndexInMemory= */ true);
    }
  }

  @RunWith(JUnit4.class)
  @SuppressWarnings("PMD.TestClassWithoutTestCases")
  public static class UnixFileDirsIndexInSqliteCASFileCacheTest extends CASFileCacheTest {
    public UnixFileDirsIndexInSqliteCASFileCacheTest() {
      super(
          Iterables.getFirst(
              Jimfs.newFileSystem(
                      Configuration.unix().toBuilder()
                          .setAttributeViews("basic", "owner", "posix", "unix")
                          .build())
                  .getRootDirectories(),
              null),
          /* storeFileDirsIndexInMemory= */ false);
    }
  }

  @RunWith(JUnit4.class)
  @SuppressWarnings("PMD.TestClassWithoutTestCases")
  public static class WindowsFileDirsIndexInMemoryCASFileCacheTest extends CASFileCacheTest {
    public WindowsFileDirsIndexInMemoryCASFileCacheTest() {
      super(
          Iterables.getFirst(
              Jimfs.newFileSystem(
                      Configuration.windows().toBuilder()
                          .setAttributeViews("basic", "owner", "dos", "acl", "posix", "user")
                          .build())
                  .getRootDirectories(),
              null),
          /* storeFileDirsIndexInMemory= */ true);
    }
  }

  @RunWith(JUnit4.class)
  @SuppressWarnings("PMD.TestClassWithoutTestCases")
  public static class WindowsFileDirsIndexInSqliteCASFileCacheTest extends CASFileCacheTest {
    public WindowsFileDirsIndexInSqliteCASFileCacheTest() {
      super(
          Iterables.getFirst(
              Jimfs.newFileSystem(
                      Configuration.windows().toBuilder()
                          .setAttributeViews("basic", "owner", "dos", "acl", "posix", "user")
                          .build())
                  .getRootDirectories(),
              null),
          /* storeFileDirsIndexInMemory= */ false);
    }
  }
}
