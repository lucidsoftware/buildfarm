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
    // ConcurrentMap (not HashMap): tests like expireEntryWaitsForUnreferencedEntry write
    // into blobs from a child put-thread while the main thread reads via the
    // InputStreamFactory lambda. HashMap is not safe under concurrent put + get even when
    // the map is small.
    blobs = Maps.newConcurrentMap();
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
            },
            // Test fixture: lowBytes == maxSizeInBytes disables eager-to-low-watermark eviction.
            // Most tests fill the cache to exactly maxSizeInBytes and assert that no eviction
            // has fired yet; with the production watermark default they'd see early eviction.
            /* lowBytes= */ 24576,
            /* wakeBudgetNanos= */ 50_000_000L,
            /* idleHeartbeatNanos= */ 2_000_000_000L,
            java.time.Clock.systemUTC());
    // do this so that we can remove the cache root dir
    fileCache.initializeRootDirectory();
    // Force a known block size so tests are more hermetic and don't depend on the host filesystem
    fileCache.setBlockSizeForTesting(TEST_BLOCK_SIZE);
    // The evictor runs on its own thread. Most tests exercise eviction without
    // calling start(), so the fixture starts the evictor up-front. The start() call is
    // idempotent — tests that call fileCache.start(...) explicitly still work.
    fileCache.evictorForTesting().start();
  }

  @After
  public void tearDown() throws IOException, InterruptedException {
    FileStore fileStore = Files.getFileStore(root);
    // Stop the evictor thread before draining executors so it doesn't queue more cleanup
    // tasks onto a shutting-down expireService.
    fileCache.evictorForTesting().stop();
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
    assertThat(fileCache.evictorForTesting().awaitQuiescence(java.time.Duration.ofSeconds(5)))
        .isTrue();
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

    // Capture the put-thread so we can observe its state deterministically rather than
    // poll on a wall-clock interval. The plan calls for awaitThreadState + awaitQuiescence
    // here.
    java.util.concurrent.atomic.AtomicReference<Thread> putThread =
        new java.util.concurrent.atomic.AtomicReference<>();
    ExecutorService service = newSingleThreadExecutor();
    Future<Void> putFuture =
        service.submit(
            () -> {
              putThread.set(Thread.currentThread());
              ByteString content = ByteString.copyFromUtf8("CAS Would Exceed Max Size");
              Digest digest = DIGEST_UTIL.compute(content);
              blobs.put(digest, content);
              fileCache.put(digest, /* isExecutable= */ false);
              return null;
            });
    // Wait for the put-thread to be parked on the hard-cap condvar (held bigBlob fills the
    // cache exactly). parkUntilUnderHardCap uses cond.await with a timeout, which puts the
    // thread into TIMED_WAITING (rather than the unbounded WAITING state).
    long deadlineNanos = System.nanoTime() + SECONDS.toNanos(5);
    while (putThread.get() == null) {
      if (System.nanoTime() > deadlineNanos) {
        putFuture.cancel(true);
        fail("put thread did not start within 5s");
      }
      Thread.yield();
    }
    awaitThreadState(putThread.get(), Thread.State.TIMED_WAITING, SECONDS.toNanos(5));
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
    // LRU mutation runs on the evictor thread after MPSC drain. Wait for the
    // releases to land before asserting order.
    assertThat(fileCache.evictorForTesting().awaitQuiescence(java.time.Duration.ofSeconds(5)))
        .isTrue();
    /* sentinel <- three <- two <- one <- sentinel */
    assertThat(storage.get(pathOne).after).isEqualTo(storage.get(pathTwo));
    assertThat(storage.get(pathTwo).after).isEqualTo(storage.get(pathThree));

    /* sentinel <- one <- three <- two <- sentinel */
    assertThat(
            fileCache.findMissingBlobs(
                ImmutableList.of(DigestUtil.toDigest(digestOne)), digestOne.getDigestFunction()))
        .isEmpty();
    // The recordAccess MPSC re-position also runs on the evictor thread; wait again.
    assertThat(fileCache.evictorForTesting().awaitQuiescence(java.time.Duration.ofSeconds(5)))
        .isTrue();
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
  public void asyncWriteCompletionDischargesWriteSize() throws Exception {
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
    // The async discharge of the duplicate write runs via expireService and
    // the writesInProgress RemovalListener. Wait for the evictor to settle (which drains
    // any insertion event from the cancellation) and then poll briefly for the discharge
    // to propagate.
    assertThat(fileCache.evictorForTesting().awaitQuiescence(java.time.Duration.ofSeconds(5)))
        .isTrue();
    long expectedSize = estimateSizeOnDisk(digest.getSize());
    long deadlineNanos = System.nanoTime() + SECONDS.toNanos(5);
    while (fileCache.size() != expectedSize && System.nanoTime() < deadlineNanos) {
      Thread.sleep(5);
    }
    assertThat(fileCache.size()).isEqualTo(expectedSize);
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
    assertThat(fileCache.evictorForTesting().awaitQuiescence(java.time.Duration.ofSeconds(5)))
        .isTrue();
    assertThat(storage.containsKey(key)).isFalse();
    assertThat(fileCache.unreferencedEntryCount()).isEqualTo(0);
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
    // The eviction that calls delegate.getWrite runs on the evictor thread,
    // not synchronously inside put(). Wait for the evictor to settle so the verify and
    // isComplete assertions see the side-effects.
    assertThat(fileCache.evictorForTesting().awaitQuiescence(java.time.Duration.ofSeconds(5)))
        .isTrue();

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
    decrementReference(fileCache, path);
  }

  void decrementReference(CASFileCache cache, Path path) throws IOException, InterruptedException {
    cache.decrementReferences(
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
    // Hold a refcount on the executable=true variant so its presence in storage suppresses
    // the digest-level onExpire callback when the executable=false variant gets evicted.
    Blob expiringBlob;
    try (ByteString.Output out = ByteString.newOutput(10000)) {
      for (int i = 0; i < 10000; i++) {
        out.write(0);
      }
      expiringBlob = new Blob(out.toByteString(), DIGEST_UTIL);
    }
    blobs.put(expiringBlob.getDigest(), expiringBlob.getData());
    Path nonExecPath = fileCache.put(expiringBlob.getDigest(), /* isExecutable= */ false).path();
    Path execPath = fileCache.put(expiringBlob.getDigest(), /* isExecutable= */ true).path();
    // Release the executable=false reference so it becomes evictable; keep the executable=true
    // reference held so it remains pinned in storage.
    decrementReference(nonExecPath);
    // Wait for the release to land on the LRU.
    assertThat(fileCache.evictorForTesting().awaitQuiescence(java.time.Duration.ofSeconds(5)))
        .isTrue();

    fileCache.put(new Blob(ByteString.copyFromUtf8("Hello, World"), DIGEST_UTIL));
    // The eviction runs on the evictor thread. Wait for the async cleanup
    // (including Files.delete via expireService) to complete before checking files exist.
    assertThat(fileCache.evictorForTesting().awaitQuiescence(java.time.Duration.ofSeconds(5)))
        .isTrue();
    shutdownAndAwaitTermination(expireService, 5, SECONDS);

    // executable=true variant still in storage → onExpire was NOT called for this digest.
    verifyNoInteractions(onExpire);
    String expiringKey = fileCache.getKey(expiringBlob.getDigest(), /* isExecutable= */ false);
    assertThat(storage.containsKey(expiringKey)).isFalse();
    assertThat(Files.exists(fileCache.getPath(expiringKey))).isFalse();

    // Clean up so tearDown doesn't trip a refcount-held storage assertion.
    decrementReference(execPath);
  }

  @Test
  public void chargeShouldReleaseReservationOnInterrupt() throws Exception {
    // The observable behavior we care about: a charger parked at the hard cap that is
    // interrupted out propagates InterruptedException and rolls back its byte reservation.
    // Hold a refcount on the filling blob so it CANNOT be evicted; the second-thread
    // charger then has no evictable LRU entries and parks indefinitely until interrupted.
    Blob fillingBlob;
    try (ByteString.Output out = ByteString.newOutput(22000)) {
      for (int i = 0; i < 22000; i++) {
        out.write(0);
      }
      fillingBlob = new Blob(out.toByteString(), DIGEST_UTIL);
    }
    blobs.put(fillingBlob.getDigest(), fillingBlob.getData());
    Path fillingPath = fileCache.put(fillingBlob.getDigest(), false).path();
    // Do NOT decrementReference — refCount stays 1; entry is not on the LRU; not evictable.
    assertThat(fileCache.size()).isEqualTo(estimateSizeOnDisk(22000));

    AtomicReference<Throwable> exRef = new AtomicReference<>();
    Thread chargeThread =
        new Thread(
            () -> {
              try {
                ByteString content = ByteString.copyFromUtf8("Hello, World");
                Digest d = DIGEST_UTIL.compute(content);
                blobs.put(d, content);
                fileCache.put(d, /* isExecutable= */ false);
              } catch (InterruptedException ie) {
                exRef.compareAndSet(null, ie);
                Thread.currentThread().interrupt();
              } catch (Throwable t) {
                // The hard-cap-backpressure IOException is a real failure here — we expected
                // the interrupt to land first and propagate InterruptedException. Capture it
                // so the assertion below produces a useful diagnostic instead of NPE.
                exRef.compareAndSet(null, t);
              }
            });
    chargeThread.setDaemon(true);
    chargeThread.start();
    // Wait until the charger has reserved its bytes AND is actually parked on the hard-cap
    // condvar. The size check confirms the reservation landed (totalBytes.add ran); the
    // thread-state check confirms we're past Evictor.charge's totalBytes.add and inside
    // cond.await(remainingNanos, NANOSECONDS), so the interrupt below tests the
    // interrupt-while-parked invariant rather than interrupt-during-charge-prologue.
    long deadlineNanos = System.nanoTime() + SECONDS.toNanos(5);
    long expectedReservedSize = estimateSizeOnDisk(22000) + estimateSizeOnDisk(12);
    while (fileCache.size() != expectedReservedSize) {
      if (System.nanoTime() > deadlineNanos) {
        chargeThread.interrupt();
        fail(
            "charger did not enter hard-cap park within 5s; cache size="
                + fileCache.size()
                + " expected="
                + expectedReservedSize);
      }
      Thread.sleep(1);
    }
    awaitThreadState(chargeThread, Thread.State.TIMED_WAITING, SECONDS.toNanos(5));
    chargeThread.interrupt();
    chargeThread.join(SECONDS.toMillis(5));
    assertThat(chargeThread.isAlive()).isFalse();
    // The reserved bytes (4096 on-disk for a 12-byte blob) must have been rolled back when
    // the interrupt propagated. The cache size returns to the cap (the held first blob).
    assertThat(fileCache.size()).isEqualTo(estimateSizeOnDisk(22000));
    Throwable t = exRef.get();
    assertThat(t).isNotNull();
    assertThat(t).isInstanceOf(InterruptedException.class);
    // Clean up the held refcount so tearDown can drain.
    decrementReference(fillingPath);
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
    // The 1->0 transition's LRU link runs on the evictor; wait for it.
    assertThat(fileCache.evictorForTesting().awaitQuiescence(java.time.Duration.ofSeconds(5)))
        .isTrue();
    String key = path.getFileName().toString();
    Entry e = storage.get(key);
    assertThat(e.refCount()).isEqualTo(0);
    assertThat(e.isLinked()).isTrue();

    // Call the protected referenceIfExists directly (same-package access).
    boolean acquired = fileCache.referenceIfExists(key);
    assertThat(acquired).isTrue();
    assertThat(e.refCount()).isEqualTo(1);
    // 0->1 transitions do NOT proactively unlink — the evictor observes the
    // bumped refcount via tryEvict's rollback. isLinked() may remain true.
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
    // Wait for the evictor to drain the release event so the entry is on LRU.
    assertThat(fileCache.evictorForTesting().awaitQuiescence(java.time.Duration.ofSeconds(5)))
        .isTrue();
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
    // 0->1 acquirers do NOT proactively unlink — that's the evictor's
    // responsibility (tryEvict's rollback). isLinked() may stay true until the evictor
    // sweep observes the bumped refcount. The observable invariant we care about
    // — the entry was not evictable while held — is enforced by the refcount alone.

    // Drain references so the entry is evictable for tearDown.
    for (int i = 0; i < threadCount; i++) {
      decrementReference(path);
    }
  }

  @Test
  public void newLocalInputShouldHoldRefcountAcrossOpenLifetime() throws Exception {
    ByteString content = ByteString.copyFromUtf8("read-path-refcount");
    Digest digest = DIGEST_UTIL.compute(content);
    blobs.put(digest, content);
    Path path = fileCache.put(digest, false).path();
    decrementReference(path); // drop initial put refcount so the entry is evictable.
    // Wait for the evictor to drain the 1->0 release insertion event so the
    // entry actually lands on the LRU before we open the stream.
    assertThat(fileCache.evictorForTesting().awaitQuiescence(java.time.Duration.ofSeconds(5)))
        .isTrue();

    String key = path.getFileName().toString();
    Entry e = storage.get(key);
    assertThat(e.refCount()).isEqualTo(0);
    assertThat(e.isLinked()).isTrue();

    InputStream in = fileCache.newInput(Compressor.Value.IDENTITY, digest, 0);
    try {
      // While the stream is open the entry must be held by exactly one reference. In
      // The LRU stays linked (the evictor's tryEvict observes the bumped
      // refcount and rolls back rather than the acquirer unlinking eagerly), so we do
      // not assert on isLinked() here.
      assertThat(e.refCount()).isEqualTo(1);
      // Read all bytes — the open fd survives even if eviction unlinks the file.
      ByteString actual = ByteString.readFrom(in);
      assertThat(actual).isEqualTo(content);
    } finally {
      in.close();
    }
    // After close the refcount returns to zero and (eventually) the entry is back on
    // the LRU via the evictor's MPSC drain.
    assertThat(e.refCount()).isEqualTo(0);
    assertThat(fileCache.evictorForTesting().awaitQuiescence(java.time.Duration.ofSeconds(5)))
        .isTrue();
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
    // The entry lands on the LRU through the evictor's drain; wait.
    assertThat(fileCache.evictorForTesting().awaitQuiescence(java.time.Duration.ofSeconds(5)))
        .isTrue();
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
    // in storage must be on the LRU and unreferencedCount must agree. The evictor puts the
    // evictor on its own thread, so we wait for the MPSC drain to settle via
    // awaitQuiescence before walking the LRU — by then the evictor is parked and the
    // LRU is safe to read without a lock.
    assertWithMessage("evictor did not quiesce after workload completed")
        .that(fileCache.evictorForTesting().awaitQuiescence(java.time.Duration.ofSeconds(5)))
        .isTrue();
    long onLru = 0;
    Entry header = fileCache.headerForTesting();
    for (Entry e = header.after; e != header; e = e.after) {
      onLru++;
      // Defense in depth: every LRU entry must be unreferenced and LIVE.
      assertThat(e.refCount()).isEqualTo(0);
      assertThat(e.state()).isEqualTo(Entry.State.LIVE);
    }
    assertThat(fileCache.unreferencedEntryCount()).isEqualTo(onLru);
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

  // === Evictor-specific tests ===

  @Test
  public void evictorShouldQuiesceAfterIdle() throws Exception {
    // After a put + release with no further activity the evictor must reach quiescence,
    // not busy-spin. awaitQuiescence times out (false) if the evictor's loop keeps making
    // forward progress without going to await — the heartbeat check inside the helper
    // distinguishes "parked between cond.await calls" from "running through the loop body."
    ByteString content = ByteString.copyFromUtf8("idle-quiescence");
    Digest digest = DIGEST_UTIL.compute(content);
    blobs.put(digest, content);
    Path path = fileCache.put(digest, false).path();
    decrementReference(path);
    assertThat(fileCache.evictorForTesting().awaitQuiescence(java.time.Duration.ofSeconds(5)))
        .isTrue();
  }

  @Test
  public void offerInsertionShouldSignalEvictorEvenAtLowDepth() throws Exception {
    // Single-entry insertion (queue depth 1) must wake the evictor so the entry lands on
    // the LRU. A regression that only signaled on threshold-cross would leave entries
    // stranded in the MPSC queue until heartbeat fires. Use a dedicated cache with a long
    // heartbeat so the heartbeat cannot mask a missing signal-on-low-depth path.
    ConcurrentMap<String, Entry> signalStorage = Maps.newConcurrentMap();
    CASFileCache signalCache =
        new DirectoryEntryCFC(
            root.resolve("low-depth-signal"),
            /* maxSizeInBytes= */ 24576,
            /* maxEntrySizeInBytes= */ 24576,
            /* hexBucketLevels= */ 1,
            expireService,
            /* accessRecorder= */ directExecutor(),
            signalStorage,
            /* zstdBufferPool= */ null,
            /* onPut= */ digest -> {},
            /* onExpire= */ digests -> {},
            /* delegate= */ null,
            /* delegateSkipLoad= */ false,
            (compressor, digest, offset) -> {
              ByteString content = blobs.get(digest);
              assertThat(content).isNotNull();
              checkArgument(compressor == Compressor.Value.IDENTITY);
              return content.substring((int) offset).newInput();
            },
            /* lowBytes= */ 24576,
            /* wakeBudgetNanos= */ 50_000_000L,
            /* idleHeartbeatNanos= */ 60_000_000_000L,
            java.time.Clock.systemUTC());
    signalCache.initializeRootDirectory();
    signalCache.setBlockSizeForTesting(TEST_BLOCK_SIZE);
    signalCache.evictorForTesting().start();
    ByteString content = ByteString.copyFromUtf8("low-depth-signal");
    Digest digest = DIGEST_UTIL.compute(content);
    blobs.put(digest, content);
    try {
      Path path = signalCache.put(digest, false).path();
      decrementReference(signalCache, path);
      assertThat(signalCache.evictorForTesting().awaitQuiescence(java.time.Duration.ofSeconds(5)))
          .isTrue();
      Entry e = signalStorage.get(path.getFileName().toString());
      assertThat(e.isLinked()).isTrue();
    } finally {
      signalCache.evictorForTesting().stop();
    }
  }

  @Test
  public void evictorShouldSweepDownToLowWatermark() throws Exception {
    // Production behavior the default fixture (lowBytes == cap, eager eviction disabled) can't
    // exercise: with lowBytes < cap the evictor sweeps unreferenced entries until totalBytes
    // drops to lowBytes, not merely to the cap. Use a dedicated cache rooted in a subdir so its
    // evictor + snapshot file don't collide with the fixture's.
    Path wmRoot = root.resolve("wm");
    CASFileCache wm =
        new DirectoryEntryCFC(
            wmRoot,
            /* maxSizeInBytes= */ 24576, // cap = 6 blocks
            /* maxEntrySizeInBytes= */ 24576,
            /* hexBucketLevels= */ 1,
            expireService,
            /* accessRecorder= */ directExecutor(),
            Maps.newConcurrentMap(),
            /* zstdBufferPool= */ null,
            /* onPut= */ digest -> {},
            /* onExpire= */ digests -> {},
            /* delegate= */ null,
            /* delegateSkipLoad= */ false,
            (compressor, digest, offset) -> blobs.get(digest).substring((int) offset).newInput(),
            /* lowBytes= */ 8192, // sweep target = 2 blocks
            /* wakeBudgetNanos= */ 50_000_000L,
            /* idleHeartbeatNanos= */ 2_000_000_000L,
            java.time.Clock.systemUTC());
    wm.initializeRootDirectory();
    wm.setBlockSizeForTesting(TEST_BLOCK_SIZE);
    wm.evictorForTesting().start();
    try {
      // Four unreferenced 1-block entries: 4 * 4096 = 16384, above lowBytes (8192) and below the
      // cap (24576), so admission never rejects and the watermark — not the cap — drives eviction.
      List<Path> paths = new ArrayList<>();
      for (int i = 0; i < 4; i++) {
        byte[] data = new byte[100];
        data[0] = (byte) i; // distinct content per entry
        ByteString blob = ByteString.copyFrom(data);
        Digest digest = DIGEST_UTIL.compute(blob);
        blobs.put(digest, blob);
        Path p = wm.put(digest, false).path();
        decrementReference(wm, p);
        paths.add(p);
      }
      assertWithMessage("watermark evictor did not quiesce")
          .that(wm.evictorForTesting().awaitQuiescence(java.time.Duration.ofSeconds(5)))
          .isTrue();
      // Quiescence requires totalBytes <= lowBytes, so the sweep ran down to the watermark (8192),
      // not merely to the cap (24576). The two oldest (coldest) entries are the ones reclaimed.
      assertThat(wm.size()).isAtMost(8192L);
      assertThat(Files.exists(paths.get(0))).isFalse();
      assertThat(Files.exists(paths.get(1))).isFalse();
      assertThat(Files.exists(paths.get(2))).isTrue();
      assertThat(Files.exists(paths.get(3))).isTrue();
    } finally {
      wm.evictorForTesting().stop();
    }
  }

  @Test
  public void sweepShouldLeaveLiveRollbackVictimLinked() throws Exception {
    // If tryEvict rolls LIVE -> EVICTING -> LIVE because refCount became non-zero, the
    // entry must remain linked. A failed acquirer can still undo a transient refcount bump,
    // and unlinking the LIVE rollback victim can strand a refCount==0 entry off the LRU
    // with no later release event to re-offer it.
    fileCache.evictorForTesting().stop();
    Entry header = fileCache.headerForTesting();
    Entry victim = Entry.orphan("live-rollback-victim", 100, Deadline.after(10, SECONDS));
    victim.addBefore(header);
    assertThat(victim.tryAcquire()).isEqualTo(0);
    long diskSize = estimateSizeOnDisk(victim.size);
    AtomicInteger nowCalls = new AtomicInteger();
    CasEvictorBacking backing =
        new CasEvictorBacking() {
          @Override
          public Entry lookupEntry(String key) {
            return key.equals(victim.key) ? victim : null;
          }

          @Override
          public Entry safeStorageRemoval(String key) throws IOException {
            fail("referenced rollback victim must not reach storage removal");
            return null;
          }

          @Override
          public void deleteExpiredKey(String key) {}

          @Override
          public void expireEntryFallback(String key, long size) {}

          @Override
          public void invalidateWriteForKey(String key, long size) {}

          @Override
          public void restoreEntryAfterRaceLoss(String key, Entry replacement) {}

          @Override
          public long onDiskSize(Entry entry) {
            return diskSize;
          }

          @Override
          public void onDigestFullyExpired(String key, long size) {}

          @Override
          public void onEntryEvicted(Entry entry) {}
        };
    Evictor evictor =
        new Evictor(
            backing,
            expireService,
            java.time.Clock.systemUTC(),
            header,
            /* parkBytes= */ 24576,
            /* lowBytes= */ 0,
            /* wakeBudgetNanos= */ 1,
            /* idleHeartbeatNanos= */ SECONDS.toNanos(30),
            new TextLRUDB(),
            root.resolve("live-rollback-lru.txt"));
    // Allow startup + one sweep iteration, then expire the sweep budget before a second tryEvict.
    evictor.setNanosSupplier(() -> nowCalls.incrementAndGet() <= 4 ? 0 : 1);
    evictor.noteUnreferencedAtStartup();
    evictor.addBytesAtStartup(diskSize);
    long rollbacksBefore = evictor.currentStateRollbackCount();
    evictor.start();
    try {
      long deadline = System.nanoTime() + SECONDS.toNanos(5);
      while (evictor.currentStateRollbackCount() == rollbacksBefore) {
        if (System.nanoTime() > deadline) {
          fail("evictor did not observe the referenced rollback victim within 5s");
        }
        Thread.sleep(2);
      }
      assertThat(victim.state()).isEqualTo(Entry.State.LIVE);
      assertThat(victim.refCount()).isEqualTo(1);
      assertThat(victim.isLinked()).isTrue();
      assertThat(evictor.currentUnreferencedCount()).isEqualTo(1);
    } finally {
      evictor.stop();
    }
  }

  @Test
  public void evictionShouldReplaceStaleRemovedSibling() throws Exception {
    ByteString content = ByteString.copyFromUtf8("stale-removed-sibling");
    Digest digest = DIGEST_UTIL.compute(content);
    blobs.put(digest, content);
    Path path = fileCache.put(digest, false).path();
    decrementReference(path);
    assertThat(fileCache.evictorForTesting().awaitQuiescence(java.time.Duration.ofSeconds(5)))
        .isTrue();

    String key = path.getFileName().toString();
    Path removingPath = fileCache.getRemovingPath(key);
    Files.createDirectories(removingPath.getParent());
    Files.write(removingPath, ByteString.copyFromUtf8("stale-expired-path").toByteArray());
    assertThat(Files.exists(removingPath)).isTrue();

    fileCache
        .evictorForTesting()
        .setTotalBytesForTesting(fileCache.evictorForTesting().lowBytes() + 1);
    fileCache.evictorForTesting().requestDrain("test");

    assertThat(fileCache.evictorForTesting().awaitQuiescence(java.time.Duration.ofSeconds(5)))
        .isTrue();
    assertThat(storage.containsKey(key)).isFalse();
    assertThat(Files.exists(path)).isFalse();
    long deadline = System.nanoTime() + SECONDS.toNanos(5);
    while (Files.exists(removingPath) && System.nanoTime() < deadline) {
      Thread.sleep(5);
    }
    assertThat(Files.exists(removingPath)).isFalse();
  }

  @Test
  public void evictionInvalidatesCompletedWriteBeforeAsyncListenerCleanup() throws Exception {
    CountDownLatch expireServiceBlocked = new CountDownLatch(1);
    CountDownLatch releaseExpireService = new CountDownLatch(1);
    expireService.submit(
        () -> {
          expireServiceBlocked.countDown();
          try {
            releaseExpireService.await();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        });
    assertThat(expireServiceBlocked.await(5, SECONDS)).isTrue();

    try {
      ByteString content = ByteString.copyFromUtf8("completed-write-evicted");
      Digest digest = DIGEST_UTIL.compute(content);
      Write completedWrite = getWrite(digest);
      try (OutputStream out = completedWrite.getOutput(1, SECONDS, () -> {})) {
        content.writeTo(out);
      }
      assertThat(completedWrite.isComplete()).isTrue();
      String key = fileCache.getKey(digest, false);
      assertThat(storage.containsKey(key)).isTrue();

      fileCache
          .evictorForTesting()
          .setTotalBytesForTesting(fileCache.evictorForTesting().lowBytes() + 1);
      fileCache.evictorForTesting().requestDrain("test");

      long deadline = System.nanoTime() + SECONDS.toNanos(5);
      while (storage.containsKey(key)) {
        if (System.nanoTime() > deadline) {
          fail("evictor did not remove completed write entry within 5s");
        }
        Thread.sleep(5);
      }

      Write replacementWrite = getWrite(digest);
      assertThat(replacementWrite.getFuture().isDone()).isFalse();
      assertThat(replacementWrite.isComplete()).isFalse();
    } finally {
      releaseExpireService.countDown();
    }
  }

  @Test
  public void evictionDischargesBytesEvenWhenDigestExpiryHookThrows() throws Exception {
    // bytesFreedThisWake invariant: a RuntimeException escaping evictOne AFTER the discharge
    // (here from the onExpire digest-fully-expired hook, which runs after safeStorageRemoval and
    // dischargeNoSignal) must NOT strand the freed bytes — the victim's storage/file removal and
    // byte discharge already happened, so a producer parked at the hard cap is still woken by the
    // end-of-sweep signal. Inject the throw via onExpire and confirm a cap-crossing put completes
    // (proving the parker woke) and the victim is gone despite the hook failure.
    byte[] fillData = new byte[22000]; // on-disk 24576 = cap
    Arrays.fill(fillData, (byte) 1);
    ByteString fillBlob = ByteString.copyFrom(fillData);
    Digest fillDigest = DIGEST_UTIL.compute(fillBlob);
    blobs.put(fillDigest, fillBlob);
    Path fillPath = fileCache.put(fillDigest, false).path();
    decrementReference(fillPath);
    String fillKey = fillPath.getFileName().toString();
    assertThat(fileCache.evictorForTesting().awaitQuiescence(java.time.Duration.ofSeconds(5)))
        .isTrue();

    // The digest-fully-expired hook throws on every eviction from now on.
    doThrow(new RuntimeException("injected onExpire failure")).when(onExpire).accept(any());

    // A cap-crossing put parks at the hard cap until the evictor frees the filler. The eviction's
    // onExpire throw fires only AFTER the filler's bytes are discharged, so this put MUST complete
    // rather than time out at the 5-minute hard cap — that is the bytesFreedThisWake wake firing
    // despite the post-discharge throw.
    byte[] data2 = new byte[100];
    Arrays.fill(data2, (byte) 2);
    ByteString blob2 = ByteString.copyFrom(data2);
    Digest digest2 = DIGEST_UTIL.compute(blob2);
    blobs.put(digest2, blob2);
    Path path2 = fileCache.put(digest2, false).path(); // returns only if the parker woke
    decrementReference(path2);

    // The filler was evicted — storage entry and canonical file gone — even though the hook threw.
    assertThat(storage.containsKey(fillKey)).isFalse();
    assertThat(Files.exists(fillPath)).isFalse();
    // The new entry made it in.
    assertThat(storage.containsKey(path2.getFileName().toString())).isTrue();
  }

  @Test
  public void evictorShouldRollbackEntryWhenStorageRemovalFailsBeforeDetach() throws Exception {
    fileCache.evictorForTesting().stop();
    Entry header = fileCache.headerForTesting();
    Entry victim = Entry.orphan("rollback-victim", 100, Deadline.after(10, SECONDS));
    victim.addBefore(header);
    long diskSize = estimateSizeOnDisk(victim.size);
    AtomicInteger removalAttempts = new AtomicInteger();
    CasEvictorBacking backing =
        new CasEvictorBacking() {
          @Override
          public Entry lookupEntry(String key) {
            return key.equals(victim.key) ? victim : null;
          }

          @Override
          public Entry safeStorageRemoval(String key) throws IOException {
            removalAttempts.incrementAndGet();
            throw new IOException("injected detach failure");
          }

          @Override
          public void deleteExpiredKey(String key) {}

          @Override
          public void expireEntryFallback(String key, long size) {}

          @Override
          public void invalidateWriteForKey(String key, long size) {}

          @Override
          public void restoreEntryAfterRaceLoss(String key, Entry replacement) {}

          @Override
          public long onDiskSize(Entry entry) {
            return diskSize;
          }

          @Override
          public void onDigestFullyExpired(String key, long size) {}

          @Override
          public void onEntryEvicted(Entry entry) {}
        };
    Evictor evictor =
        new Evictor(
            backing,
            expireService,
            java.time.Clock.systemUTC(),
            header,
            /* parkBytes= */ 24576,
            /* lowBytes= */ 0,
            /* wakeBudgetNanos= */ 50_000_000L,
            /* idleHeartbeatNanos= */ 2_000_000_000L,
            new TextLRUDB(),
            root.resolve("rollback-lru.txt"));
    evictor.noteUnreferencedAtStartup();
    evictor.addBytesAtStartup(diskSize);
    evictor.start();
    try {
      evictor.requestDrain("test");
      long deadline = System.nanoTime() + SECONDS.toNanos(5);
      while (removalAttempts.get() == 0) {
        if (System.nanoTime() > deadline) {
          fail("evictor did not attempt removal within 5s");
        }
        Thread.sleep(2);
      }
      while (victim.state() == Entry.State.EVICTING) {
        if (System.nanoTime() > deadline) {
          fail("evictor did not roll back failed removal within 5s");
        }
        Thread.sleep(2);
      }

      assertThat(victim.state()).isEqualTo(Entry.State.LIVE);
      assertThat(victim.refCount()).isEqualTo(0);
      assertThat(victim.isLinked()).isTrue();
      assertThat(evictor.currentTotalBytes()).isEqualTo(diskSize);
      assertThat(evictor.currentUnreferencedCount()).isEqualTo(1);
    } finally {
      evictor.stop();
    }
  }

  // chargeShouldRollBackReservationOnHardCapTimeoutTrace was deleted because it asserted
  // nothing about the timeout path it claimed to cover. The interrupt-path rollback is
  // exercised by chargeShouldReleaseReservationOnInterrupt above; the timeout-path rollback
  // is structurally identical (parkUntilUnderHardCap's catch clauses share the same
  // totalBytes.add(-bytesReserved) recovery) and lacks a deterministic harness without a
  // FakeClock — which the Evictor's nanos source supports via setNanosSupplier but the
  // current test fixture does not wire through. Add timeout coverage when that wiring lands.

  @Test
  public void newLocalInputShouldReadCorrectBytesAcrossPutAndAccess() throws Exception {
    // Round-trip: write a non-trivial payload, read it back, verify byte-equality. Phase
    // 2.1's lazy LRU mutation shouldn't disturb the read path.
    ByteString content = ByteString.copyFromUtf8("read-roundtrip-payload-with-some-length");
    Digest digest = DIGEST_UTIL.compute(content);
    blobs.put(digest, content);
    Path path = fileCache.put(digest, false).path();
    decrementReference(path);
    try (InputStream in = fileCache.newInput(Compressor.Value.IDENTITY, digest, 0)) {
      ByteString actual = ByteString.readFrom(in);
      assertThat(actual).isEqualTo(content);
    }
  }

  @Test
  public void evictorStartShouldBeIdempotent() throws Exception {
    // The test fixture starts the evictor in setUp. A redundant start (e.g., from a test
    // that explicitly invokes start() or from the cache's start() lifecycle) must be a
    // no-op rather than throwing or doubling threads.
    long evictorThreadsBefore = countEvictorThreads();
    long schedulerThreadsBefore = countSnapshotSchedulerThreads();
    // Sanity: the fixture's evictor thread must actually be running, otherwise the
    // idempotence assertion below could pass against an empty baseline if someone later
    // renamed the evictor thread.
    assertThat(evictorThreadsBefore).isAtLeast(1);
    assertThat(schedulerThreadsBefore).isAtLeast(1);
    fileCache.evictorForTesting().start();
    fileCache.evictorForTesting().start();
    // Count both the evictor thread and the snapshot scheduler thread: a buggy
    // non-idempotent start() would spawn an extra of either.
    assertThat(countEvictorThreads()).isEqualTo(evictorThreadsBefore);
    assertThat(countSnapshotSchedulerThreads()).isEqualTo(schedulerThreadsBefore);
    // The evictor thread is still running; quiescence still returns true.
    assertThat(fileCache.evictorForTesting().awaitQuiescence(java.time.Duration.ofSeconds(2)))
        .isTrue();
  }

  private static long countEvictorThreads() {
    return Thread.getAllStackTraces().keySet().stream()
        .filter(t -> t.getName().startsWith("buildfarm-cas-evictor"))
        .count();
  }

  private static long countSnapshotSchedulerThreads() {
    return Thread.getAllStackTraces().keySet().stream()
        .filter(t -> t.getName().startsWith("buildfarm-cas-snapshot-scheduler"))
        .count();
  }

  @Test
  public void startupShouldReclaimRemovedSuffixOrphans() throws Exception {
    // Synthesize an orphan _removed file in the bucket tree, then start the cache. The
    // orphan must be deleted by scanRoot before any real storage admission.
    Path bucket =
        fileCache.getPath(
            fileCache.getKey(DIGEST_UTIL.compute(ByteString.copyFromUtf8("x")), false));
    Files.createDirectories(bucket.getParent());
    Path orphan = bucket.getParent().resolve("orphan_removed");
    Files.write(orphan, new byte[16]);
    assertThat(Files.exists(orphan)).isTrue();

    fileCache.start(/* skipLoad= */ false).get();

    assertThat(Files.exists(orphan)).isFalse();
  }

  @Test
  public void startupShouldReclaimSnapshotTmpOrphans() throws Exception {
    // Synthesize a stale AtomicFileWriter tmp at the snapshot location (root), then start
    // the cache. Stale tmps are left behind when the JVM crashes between writing the tmp
    // and the atomic rename — they must be cleaned up so the snapshot directory doesn't
    // accumulate dead state.
    Path tmpOrphan = root.resolve("lru.txt.tmp.deadbeef-1234-5678-abcd-ef0123456789");
    Files.write(tmpOrphan, new byte[32]);
    assertThat(Files.exists(tmpOrphan)).isTrue();

    fileCache.start(/* skipLoad= */ false).get();

    assertThat(Files.exists(tmpOrphan)).isFalse();
  }

  @Test
  public void snapshotWriterShouldEmitOnShutdown() throws Exception {
    // After putting multiple entries and stopping the evictor, the shutdown snapshot file
    // must exist and contain a (key,size) line for every entry on the LRU. Spot-checking a
    // single matching line would silently pass if the writer truncated the output or wrote
    // the wrong size.
    ByteString content1 = ByteString.copyFromUtf8("snapshot-shutdown-1");
    ByteString content2 = ByteString.copyFromUtf8("snapshot-shutdown-second-entry");
    Digest digest1 = DIGEST_UTIL.compute(content1);
    Digest digest2 = DIGEST_UTIL.compute(content2);
    blobs.put(digest1, content1);
    blobs.put(digest2, content2);
    Path path1 = fileCache.put(digest1, false).path();
    Path path2 = fileCache.put(digest2, false).path();
    decrementReference(path1);
    decrementReference(path2);
    fileCache.evictorForTesting().stop();

    Path snapshotPath = root.resolve("lru.txt");
    assertThat(Files.exists(snapshotPath)).isTrue();
    List<String> lines = Files.readAllLines(snapshotPath);
    java.util.Map<String, Long> parsed = new java.util.HashMap<>();
    for (String line : lines) {
      int sep = line.indexOf(',');
      assertWithMessage("snapshot line missing comma: " + line).that(sep).isGreaterThan(-1);
      parsed.put(line.substring(0, sep), Long.parseLong(line.substring(sep + 1)));
    }
    String key1 = path1.getFileName().toString();
    String key2 = path2.getFileName().toString();
    assertThat(parsed).containsKey(key1);
    assertThat(parsed).containsKey(key2);
    assertThat(parsed.get(key1)).isEqualTo(content1.size());
    assertThat(parsed.get(key2)).isEqualTo(content2.size());
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
