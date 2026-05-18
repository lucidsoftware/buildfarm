// Copyright 2017 The Buildfarm Authors. All rights reserved.
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

import static build.buildfarm.common.DigestUtil.OMITTED_DIGEST_FUNCTIONS;
import static build.buildfarm.common.io.EvenMoreFiles.setReadOnlyPerms;
import static build.buildfarm.common.io.Utils.getOrIOException;
import static build.buildfarm.common.io.Utils.listDir;
import static build.buildfarm.common.io.Utils.stat;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.io.ByteStreams.nullOutputStream;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.Futures.transformAsync;
import static com.google.common.util.concurrent.Futures.whenAllComplete;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static java.lang.String.format;
import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

import build.bazel.remote.execution.v2.BatchReadBlobsResponse.Response;
import build.bazel.remote.execution.v2.Compressor;
import build.bazel.remote.execution.v2.DigestFunction;
import build.bazel.remote.execution.v2.Directory;
import build.bazel.remote.execution.v2.DirectoryNode;
import build.bazel.remote.execution.v2.FileNode;
import build.bazel.remote.execution.v2.RequestMetadata;
import build.bazel.remote.execution.v2.SymlinkNode;
import build.buildfarm.cas.ContentAddressableStorage;
import build.buildfarm.cas.DigestMismatchException;
import build.buildfarm.cas.cfc.LRUDB.SizeEntry;
import build.buildfarm.common.BuildfarmExecutors;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.HashFunction;
import build.buildfarm.common.EmptyInputStreamFactory;
import build.buildfarm.common.EntryLimitException;
import build.buildfarm.common.FailoverInputStreamFactory;
import build.buildfarm.common.InputStreamFactory;
import build.buildfarm.common.Time;
import build.buildfarm.common.Write;
import build.buildfarm.common.Write.CompleteWrite;
import build.buildfarm.common.WritesHelper;
import build.buildfarm.common.ZstdCompressingInputStream;
import build.buildfarm.common.ZstdDecompressingOutputStream;
import build.buildfarm.common.ZstdDecompressingOutputStream.FixedBufferPool;
import build.buildfarm.common.grpc.Retrier;
import build.buildfarm.common.grpc.Retrier.Backoff;
import build.buildfarm.common.io.CountingOutputStream;
import build.buildfarm.common.io.Directories;
import build.buildfarm.common.io.FeedbackOutputStream;
import build.buildfarm.common.io.FileStatus;
import build.buildfarm.v1test.BlobWriteKey;
import build.buildfarm.v1test.Digest;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.hash.HashingOutputStream;
import com.google.common.io.ByteStreams;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.gson.Gson;
import com.google.protobuf.ByteString;
import io.grpc.Deadline;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ServerCallStreamObserver;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;
import java.io.BufferedReader;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.logging.Level;
import javax.annotation.concurrent.GuardedBy;
import lombok.Getter;
import lombok.extern.java.Log;
import org.jspecify.annotations.Nullable;

@Log
public abstract class CASFileCache implements ContentAddressableStorage {
  // Prometheus metrics
  private static final Counter expiredKeyCounter =
      Counter.build().name("expired_key").help("Number of key expirations.").register();
  private static final Gauge casSizeMetric =
      Gauge.build().name("cas_size").help("CAS size.").register();
  private static final Gauge casEntryCountMetric =
      Gauge.build().name("cas_entry_count").help("Number of entries in the CAS.").register();
  private static Histogram casTtl =
      Histogram.build()
          .name("cas_ttl_s")
          .buckets(
              3600, // 1 hour
              21600, // 6 hours
              86400, // 1 day
              345600, // 4 days
              604800, // 1 week
              1210000 // 2 weeks
              )
          .help("The amount of time CAS entries live on L1 storage before expiration (seconds)")
          .register();

  private static final Counter readIOErrors =
      Counter.build().name("read_io_errors").help("Number of IO errors on read.").register();

  @Getter private final Path root;
  protected final EntryPathStrategy entryPathStrategy;
  protected final long maxSizeInBytes;
  protected final long maxEntrySizeInBytes;
  protected final ConcurrentMap<String, Entry> storage;
  private final Consumer<Digest> onPut;
  private final Consumer<Iterable<Digest>> onExpire;
  private final Executor accessRecorder;
  private final ExecutorService expireService;
  private final LRUDB db = new TextLRUDB();
  private volatile Deadline saveLRUAfter = Deadline.after(10, MINUTES);
  private final Path lru;

  private final FixedBufferPool zstdBufferPool;
  @Nullable private final ContentAddressableStorage delegate;
  private final boolean delegateSkipLoad;
  private final InputStreamFactory inputStreamFactory;
  private final LoadingCache<String, Lock> keyLocks =
      CacheBuilder.newBuilder()
          .expireAfterAccess(
              1, MINUTES) // hopefully long enough for any of our file ops to take place and prevent
          // collision
          .build(
              new CacheLoader<>() {
                @Override
                public Lock load(String key) {
                  // should be sufficient for what we're doing
                  return new ReentrantLock();
                }
              });
  private final LoadingCache<BlobWriteKey, Write> writes;

  private final LoadingCache<Digest, SettableFuture<Long>> writesInProgress =
      CacheBuilder.newBuilder()
          .expireAfterAccess(1, HOURS)
          .removalListener(
              (RemovalListener<Digest, SettableFuture<Long>>)
                  notification -> {
                    // no effect if already done
                    notification.getValue().setException(new IOException("write cancelled"));
                  })
          .build(
              new CacheLoader<>() {
                @SuppressWarnings("NullableProblems")
                @Override
                public SettableFuture<Long> load(Digest digest) {
                  SettableFuture<Long> future = SettableFuture.create();
                  if (containsLocal(digest, /* result= */ null, (key) -> {})) {
                    future.set(digest.getSize());
                  }
                  return future;
                }
              });

  private static final long DEFAULT_BLOCK_SIZE = 4096;
  private static final long ESTIMATED_BYTES_PER_DIRECTORY_ENTRY = 32;

  protected FileStore fileStore; // bound to root
  protected long blockSize = DEFAULT_BLOCK_SIZE;
  // Eventually consistent; the eviction loop re-reads after each successful expiration.
  protected final transient LongAdder totalBytes = new LongAdder();
  protected final transient Entry header = new SentinelEntry();
  protected final transient LongAdder unreferencedCount = new LongAdder();

  private State state = new State();

  private final transient LongAdder removedBytes = new LongAdder();
  private final transient LongAdder removedCount = new LongAdder();

  protected final ReentrantLock lruLock = new ReentrantLock();
  protected final Condition lruCondition = lruLock.newCondition();

  private Thread prometheusMetricsThread;

  public long size() {
    return totalBytes.sum();
  }

  public long maxSize() {
    return maxSizeInBytes;
  }

  public long entryCount() {
    return storage.size();
  }

  public long unreferencedEntryCount() {
    return unreferencedCount.sum();
  }

  public long directoryStorageCount() {
    return 0;
  }

  public int getEvictedCount() {
    return (int) removedCount.sumThenReset();
  }

  public long getEvictedSize() {
    return removedBytes.sumThenReset();
  }

  @VisibleForTesting
  ReentrantLock lruLockForTesting() {
    return lruLock;
  }

  @VisibleForTesting
  Condition lruConditionForTesting() {
    return lruCondition;
  }

  @VisibleForTesting
  Entry headerForTesting() {
    return header;
  }

  public record CacheScanResults(
      List<Path> computeDirs, List<Path> deleteFiles, Map<Object, Entry> fileKeys) {}

  public record CacheLoadResults(
      boolean loadSkipped, CacheScanResults scan, List<Path> invalidDirectories) {}

  public record StartupCacheResults(
      Path cacheDirectory, CacheLoadResults load, Duration startupTime) {}

  public static class IncompleteBlobException extends IOException {
    IncompleteBlobException(Path writePath, String key, long committed, long expected) {
      super(
          format("blob %s => %s: committed %d, expected %d", writePath, key, committed, expected));
    }
  }

  public CASFileCache(
      Path root,
      long maxSizeInBytes,
      long maxEntrySizeInBytes,
      int hexBucketLevels,
      ExecutorService expireService,
      Executor accessRecorder,
      ConcurrentMap<String, Entry> storage,
      FixedBufferPool zstdBufferPool,
      Consumer<Digest> onPut,
      Consumer<Iterable<Digest>> onExpire,
      @Nullable ContentAddressableStorage delegate,
      boolean delegateSkipLoad,
      InputStreamFactory externalInputStreamFactory) {
    this.root = root;
    this.maxSizeInBytes = maxSizeInBytes;
    this.maxEntrySizeInBytes = maxEntrySizeInBytes;
    this.expireService = expireService;
    this.accessRecorder = accessRecorder;
    this.storage = storage;
    this.onPut = onPut;
    this.onExpire = onExpire;
    this.delegate = delegate;
    this.delegateSkipLoad = delegateSkipLoad;
    this.inputStreamFactory =
        new EmptyInputStreamFactory(
            new FailoverInputStreamFactory(this::newTransparentInput, externalInputStreamFactory));
    this.zstdBufferPool = zstdBufferPool;

    lru = root.resolve("lru.txt");

    writes =
        CacheBuilder.newBuilder()
            .expireAfterAccess(1, HOURS)
            .removalListener(
                (RemovalListener<BlobWriteKey, Write>)
                    notification -> expireService.execute(notification.getValue()::reset))
            .build(
                new CacheLoader<>() {
                  @SuppressWarnings("NullableProblems")
                  @Override
                  public Write load(BlobWriteKey key) {
                    return newWrite(key, CASFileCache.this.getFuture(key.getDigest()));
                  }
                });

    entryPathStrategy = new HexBucketEntryPathStrategy(root, hexBucketLevels);

    header.before = header.after = header;
  }

  protected static Digest keyToDigest(String key, long size, DigestUtil digestUtil)
      throws NumberFormatException {
    String[] components = key.split("_");

    int hashIndex = 0;

    if (!OMITTED_DIGEST_FUNCTIONS.contains(digestUtil.getDigestFunction())) {
      hashIndex++;
    }

    String hashComponent = components[hashIndex];

    return digestUtil.build(hashComponent, size);
  }

  protected static @Nullable DigestUtil parseDirectoryDigestUtil(String fileName) {
    String[] components = fileName.split("_");
    if ((components.length != 2 && components.length != 3)
        || !components[components.length - 1].equals("dir")) {
      return null;
    }

    if (components.length == 2) {
      // contains hash and "dir"
      return DigestUtil.parseHash(components[0]);
    }
    // contains expected digest function, hash, and "dir"
    return DigestUtil.forHash(components[0]);
  }

  /** Parses the given fileName into a FileEntryKey or null if parsing failed */
  private static @Nullable FileEntryKey parseFileEntryKey(String fileName, long size) {
    String[] components = fileName.split("_");

    if (components.length > 3) {
      return null;
    }

    // executable if last component of plural is "exec"
    boolean isExecutable =
        components.length > 1 && components[components.length - 1].equals("exec");
    if (!isExecutable && components.length > 2) {
      return null;
    }

    boolean hasDigestFunction = components.length == 3 || (!isExecutable && components.length == 2);
    DigestUtil digestUtil;
    if (hasDigestFunction) {
      digestUtil = DigestUtil.forHash(components[0]);
    } else {
      digestUtil = DigestUtil.parseHash(components[0]);
    }
    if (digestUtil == null) {
      return null;
    }

    try {
      Digest digest = digestUtil.build(components[hasDigestFunction ? 1 : 0], size);
      return new FileEntryKey(getKey(digest, isExecutable), size, isExecutable, digest);
    } catch (NumberFormatException e) {
      return null;
    }
  }

  private boolean contains(
      Digest digest,
      boolean isExecutable,
      build.bazel.remote.execution.v2.Digest.@Nullable Builder result,
      Consumer<String> onContains) {
    String key = getKey(digest, isExecutable);
    Entry entry = getEntry(key);
    if (entry == null) {
      return false;
    }
    // getEntry returns a placeholder when storage misses and we're not in a running
    // state — recover by reading the on-disk size directly.
    long size = entry.size;
    if (entry.isPlaceholder()) {
      try {
        size = Files.size(getPath(key));
      } catch (IOException e) {
        return false;
      }
    }
    if (digest.getSize() < 0 || digest.getSize() == size) {
      if (result != null) {
        result.mergeFrom(DigestUtil.toDigest(digest)).setSizeBytes(size);
      }
      onContains.accept(key);
      return true;
    }
    return false;
  }

  private void saveLRU() {
    List<SizeEntry> list;
    lruLock.lock();
    try {
      list = lruSizeEntryList();
    } finally {
      lruLock.unlock();
    }
    try {
      // synchronized(lru) serializes save() with db's commit ordering. Lock order is
      // lruLock -> lru; the lruLock block above is closed before we acquire lru here.
      synchronized (lru) {
        db.save(list.iterator(), lru);
      }
    } catch (Exception e) {
      log.log(Level.SEVERE, "error saving lru state", e);
    }
  }

  /** Not reentrant, not thread safe */
  private void maybeSaveLRU() {
    if (saveLRUAfter == null || !saveLRUAfter.isExpired()) {
      return;
    }
    saveLRUAfter = null;
    expireService.execute(
        () -> {
          try {
            saveLRU();
          } finally {
            saveLRUAfter = Deadline.after(10, MINUTES);
          }
        });
  }

  private void accessed(Iterable<String> keys) {
    /* could also bucket these */
    try {
      accessRecorder.execute(
          () -> {
            recordAccess(keys);
            maybeSaveLRU();
          });
    } catch (RejectedExecutionException e) {
      log.log(Level.SEVERE, format("could not record access for %d keys", Iterables.size(keys)), e);
    }
  }

  private void recordAccess(Iterable<String> keys) {
    // Only entries currently on the LRU (refCount == 0) need to be re-positioned;
    // referenced entries are a no-op. Filter outside the lock so a workload that
    // accesses mostly-held tool digests skips the eviction lock entirely.
    List<Entry> unreferenced = null;
    for (String key : keys) {
      Entry e = storage.get(key);
      if (e != null && e.refCount() == 0) {
        if (unreferenced == null) {
          unreferenced = new ArrayList<>();
        }
        unreferenced.add(e);
      }
    }
    if (unreferenced == null) {
      return;
    }
    lruLock.lock();
    try {
      for (Entry e : unreferenced) {
        if (e.recordAccess(header)) {
          unreferencedCount.decrement();
        }
      }
    } finally {
      lruLock.unlock();
    }
  }

  private boolean entryExists(Entry e) {
    if (!e.existsDeadline.isExpired()) {
      return true;
    }

    if (Files.exists(getPath(e.key))) {
      e.existsDeadline = Deadline.after(10, SECONDS);
      return true;
    }
    return false;
  }

  boolean containsLocal(
      Digest digest,
      build.bazel.remote.execution.v2.Digest.@Nullable Builder result,
      Consumer<String> onContains) {
    /* maybe swap the order here if we're higher in ratio on one side */
    return contains(digest, false, result, onContains)
        || contains(digest, true, result, onContains);
  }

  @Override
  public Iterable<build.bazel.remote.execution.v2.Digest> findMissingBlobs(
      Iterable<build.bazel.remote.execution.v2.Digest> digests, DigestFunction.Value digestFunction)
      throws InterruptedException {
    ImmutableList.Builder<build.bazel.remote.execution.v2.Digest> builder = ImmutableList.builder();
    ImmutableList.Builder<String> found = ImmutableList.builder();
    build.bazel.remote.execution.v2.Digest.Builder result =
        build.bazel.remote.execution.v2.Digest.newBuilder();
    for (build.bazel.remote.execution.v2.Digest digest : digests) {
      if (digest.getSizeBytes() != 0
          && !containsLocal(DigestUtil.fromDigest(digest, digestFunction), result, found::add)) {
        builder.add(digest);
      } else if (digest.getSizeBytes() == -1) {
        // may misbehave with delegate
        builder.add(result.build());
      }
    }
    if (state.shouldRecordAccess()) {
      List<String> foundDigests = found.build();
      if (!foundDigests.isEmpty()) {
        accessed(foundDigests);
      }
    }
    ImmutableList<build.bazel.remote.execution.v2.Digest> missingDigests = builder.build();
    return CasFallbackDelegate.findMissingBlobs(delegate, missingDigests, digestFunction);
  }

  @Override
  public boolean contains(Digest digest, build.bazel.remote.execution.v2.Digest.Builder result) {
    return containsLocal(digest, result, (key) -> accessed(ImmutableList.of(key)))
        || CasFallbackDelegate.contains(delegate, digest, result);
  }

  @Override
  public ListenableFuture<List<Response>> getAllFuture(
      Iterable<build.bazel.remote.execution.v2.Digest> digests,
      DigestFunction.Value digestFunction) {
    throw new UnsupportedOperationException();
  }

  protected InputStream newTransparentInput(Compressor.Value compressor, Digest digest, long offset)
      throws IOException {
    try {
      return newLocalInput(compressor, digest, offset);
    } catch (NoSuchFileException e) {
      return CasFallbackDelegate.newInput(delegate, e, compressor, digest, offset);
    }
  }

  private InputStream compressorInputStream(Compressor.Value compressor, InputStream identity)
      throws IOException {
    if (compressor == Compressor.Value.IDENTITY) {
      return identity;
    }
    checkArgument(compressor == Compressor.Value.ZSTD);
    return new ZstdCompressingInputStream(identity);
  }

  private Entry getEntry(String key) {
    Entry entry = storage.get(key);
    if (entry != null) {
      return entry;
    }
    if (!state.shouldRecordAccess()) {
      return new Entry();
    }
    return null;
  }

  @SuppressWarnings({"ResultOfMethodCallIgnored", "PMD.CompareObjectsWithEquals"})
  InputStream newLocalInput(Compressor.Value compressor, Digest digest, long offset)
      throws IOException {
    // Validate compressor before taking any refcount. compressorInputStream's checkArgument
    // would otherwise throw IAE from inside the inner try, escaping the outer IOException
    // catch and leaking the refcount taken by e.tryAcquire() in the loop body.
    checkArgument(
        compressor == Compressor.Value.IDENTITY || compressor == Compressor.Value.ZSTD,
        "unsupported compressor %s",
        compressor);
    // branch here or above for STARTING
    log.log(Level.FINER, format("getting input stream for %s", DigestUtil.toString(digest)));
    boolean isExecutable = false;
    do {
      String key = getKey(digest, isExecutable);
      Entry e = getEntry(key);
      // Sentinel entries bypass the refcount discipline.
      boolean haveRefcount = false;
      boolean placeholder = e != null && e.isPlaceholder();
      if (e != null && !(e instanceof SentinelEntry) && !placeholder) {
        int previous = e.tryAcquire();
        if (previous < 0) {
          e = null;
        } else {
          haveRefcount = true;
          if (previous == 0) {
            lruLock.lock();
            try {
              unlinkReferenced(e);
            } finally {
              lruLock.unlock();
            }
          }
        }
      }
      if (e != null) {
        InputStream input = null;
        try {
          // Capture the FIS so the wrap can't leak it: if compressorInputStream's ctor
          // throws (e.g. Zstd pipe setup), Files.newInputStream's FD would dangle until GC.
          InputStream fis = Files.newInputStream(getPath(key));
          try {
            input = compressorInputStream(compressor, fis);
            // input.skip is inside the same Throwable handler as the wrap: a non-IOException
            // throw from skip (e.g. a RuntimeException from a compressor wrapper's
            // skip implementation) would otherwise escape the IOException-only outer
            // catch with refcount + stream still held, pinning the entry LIVE forever.
            input.skip(offset);
          } catch (Exception t) {
            try {
              if (input != null) {
                // close the wrapper if compressorInputStream succeeded; the wrapper
                // owns and will close the underlying fis. Null out so the outer
                // IOException catch's input-close block does not double-close.
                input.close();
                input = null;
              } else {
                fis.close();
              }
            } catch (Exception closeEx) {
              t.addSuppressed(closeEx);
            }
            // The outer catch is IOException-only; a non-IOException RuntimeException (e.g.
            // from ZstdCompressingInputStream's JNI ctor or from input.skip) would escape
            // with the refcount taken by tryAcquire still held, pinning the entry LIVE and
            // unevictable. Release before rethrow so the only invariant breach is the
            // unexpected exception itself. An Error (OutOfMemoryError, LinkageError, ...) is
            // deliberately NOT caught here: it propagates immediately without the refcount
            // release or stream close — we make no attempt to clean up after an unrecoverable
            // error, since the JVM state is undefined.
            if (!(t instanceof IOException) && haveRefcount) {
              releaseAndRelink(e);
              haveRefcount = false;
            }
            throw t;
          }
        } catch (IOException ioEx) {
          if (!(ioEx instanceof NoSuchFileException)) {
            readIOErrors.inc();
            log.log(
                Level.WARNING,
                format("error opening %s at %d", DigestUtil.toString(digest), offset),
                ioEx);
          }

          if (haveRefcount) {
            // safeStorageRemoval needs refCount == 0 to discharge the entry.
            releaseAndRelink(e);
            haveRefcount = false;
          }

          if (!(e instanceof SentinelEntry) && !placeholder) {
            if (e.tryEvict()) {
              // safeStorageRemoval / unlinkEntry can throw IOException; without the
              // finally, a throw between tryEvict and completeEviction would strand
              // the entry in EVICTING permanently. The !completed path also unlinks
              // and discharges so a disk-pressure IOException does not leak bytes
              // from totalBytes.
              boolean discharged = false;
              boolean completed = false;
              try {
                boolean removed = false;
                lruLock.lock();
                try {
                  invalidateWrite(digest);
                  Entry removedEntry = safeStorageRemoval(key);
                  if (removedEntry == e) { // Intentional reference comparison
                    // Set discharged before unlinkEntry: dischargeEntry's finally
                    // guarantees discharge() always runs, but unlinkEntry can still
                    // rethrow a directory-expiration failure past us. If we only set
                    // discharged after the call, the outer finally would double-decrement
                    // totalBytes on that path.
                    discharged = true;
                    unlinkEntry(removedEntry);
                    removed = true;
                  } else if (removedEntry != null) {
                    log.severe(
                        format(
                            "nonexistent entry %s did not match last unreferenced entry,"
                                + " restoring it",
                            key));
                    storage.put(key, removedEntry);
                    // e was on the LRU and its bytes were charged, but storage now holds
                    // a different Entry for this key. Without unlinking + discharging, e
                    // strands on the LRU as EVICTED with its bytes permanently in
                    // totalBytes — the resurrection branch in expireEntry would later
                    // unlink but never discharge.
                    unlinkReferenced(e);
                    dischargeLocked(e.key, e.size);
                    discharged = true;
                  } else {
                    // Defensive: per the protocol the tryEvict winner is the unique remover,
                    // so safeStorageRemoval should always find e in storage. If it doesn't,
                    // e's bytes remain in totalBytes and e is still on the LRU — discharge
                    // here so accounting does not slow-leak under a crash-recovery or
                    // corruption scenario.
                    log.severe(
                        format(
                            "entry %s was already removed from storage before this evictor"
                                + " could remove it, discharging",
                            key));
                    unlinkReferenced(e);
                    dischargeLocked(e.key, e.size);
                    discharged = true;
                  }
                } finally {
                  lruLock.unlock();
                }
                e.completeEviction();
                completed = true;
                if (removed && isExecutable) {
                  onExpire.accept(ImmutableList.of(digest));
                }
              } catch (IOException secondaryEx) {
                // The eviction body's !completed finally already restored the entry's
                // protocol state (unlinked, discharged, EVICTED). A fresh IOException
                // here is typically a Files.move/createLink/delete failure under disk
                // pressure. Letting it escape would replace the original "error opening"
                // ioEx as the caller-visible exception and skip the isExecutable retry
                // below. Suppress onto ioEx instead so the original diagnostic survives
                // and the do-while loop still falls through to the alternative key.
                ioEx.addSuppressed(secondaryEx);
              } finally {
                if (!completed) {
                  lruLock.lock();
                  try {
                    unlinkReferenced(e);
                    if (!discharged) {
                      dischargeLocked(e.key, e.size);
                    }
                  } finally {
                    lruLock.unlock();
                  }
                  e.completeEviction();
                }
              }
            }
            e = null;
          }
          // input.skip may have left a partially-read stream that we no longer hold a
          // refcount on; the caller would receive bare data without the eviction
          // discipline that the RefcountedInputStream wrapper provides.
          if (input != null) {
            try {
              input.close();
            } catch (IOException closeEx) {
              ioEx.addSuppressed(closeEx);
            }
            input = null;
          }
        }
        if (e != null && !(e instanceof SentinelEntry) && !placeholder) {
          accessed(ImmutableList.of(key));
        }
        if (input != null) {
          if (haveRefcount) {
            final Entry refEntry = e;
            return new RefcountedInputStream(input, () -> releaseAndRelink(refEntry));
          }
          return input;
        }
      }
      if (haveRefcount) {
        releaseAndRelink(e);
      }
      isExecutable = !isExecutable;
    } while (isExecutable);
    throw new NoSuchFileException(DigestUtil.toString(digest));
  }

  /**
   * Append a now-unreferenced entry to the LRU tail. Skips when the entry is already linked, has
   * been picked up by a concurrent evictor (non-LIVE), or has been re-acquired between the caller's
   * refcount transition and this lock acquisition (refCount != 0). The refCount recheck under
   * {@code lruLock} closes the release-then-acquire race where the caller's {@link Entry#release}
   * hit 0 but a concurrent {@link Entry#tryAcquire} bumped the count back to 1 before this method
   * ran — without the recheck we would publish a referenced entry onto the LRU tail.
   */
  @GuardedBy("lruLock")
  protected void linkUnreferenced(Entry e) {
    if (!e.isLinked() && e.state() == Entry.State.LIVE && e.refCount() == 0) {
      e.addBefore(header);
      unreferencedCount.increment();
    }
  }

  /**
   * Detach a now-referenced entry from the LRU list. The decrement is conditional on actually
   * performing the unlink: when the entry is not on the LRU (e.g. publication raced with this
   * acquire, or the inline evictor's resurrection branch already pulled it), {@code
   * unreferencedCount} was never incremented for it and decrementing here would drive the counter
   * negative.
   */
  @GuardedBy("lruLock")
  protected void unlinkReferenced(Entry e) {
    if (e.isLinked()) {
      e.unlink();
      unreferencedCount.decrement();
    }
  }

  /**
   * Drop one refcount on {@code e}; if the release transitions 1 -> 0, append the entry to the LRU
   * tail under {@link #lruLock}. The caller must hold a real reference to {@code e} — looking up
   * storage.get(key) at release time can return a different Entry inserted after the caller's
   * tryAcquire, causing underflow on the replacement or a leak on the original.
   */
  private void releaseAndRelink(Entry e) {
    if (e.release()) {
      lruLock.lock();
      try {
        linkUnreferenced(e);
        // signalAll because multiple chargers can be parked in waitForLastUnreferencedEntry
        // simultaneously (await releases lruLock so they queue at park, not at lock). A
        // single signal would wake only one waiter; the rest would stay parked until the
        // next release, producing latency tails under fill pressure.
        lruCondition.signalAll();
      } finally {
        lruLock.unlock();
      }
    }
  }

  @Override
  public InputStream newInput(Compressor.Value compressor, Digest digest, long offset)
      throws IOException {
    try {
      return newLocalInput(compressor, digest, offset);
    } catch (NoSuchFileException e) {
      if (delegate == null) {
        throw e;
      }
    }
    return newInputFallback(compressor, digest, offset);
  }

  @Override
  public Blob get(Digest digest) {
    try (InputStream in = newInput(Compressor.Value.IDENTITY, digest, /* offset= */ 0)) {
      return new Blob(ByteString.readFrom(in), digest);
    } catch (NoSuchFileException e) {
      return null;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static final int CHUNK_SIZE = 128 * 1024;

  private static boolean shouldReadThrough(RequestMetadata requestMetadata) {
    try {
      URI uri = new URI(requestMetadata.getCorrelatedInvocationsId());
      QueryStringDecoder decoder = new QueryStringDecoder(uri);
      return decoder
          .parameters()
          .getOrDefault("THROUGH", ImmutableList.of("false"))
          .getFirst()
          .equals("true");
    } catch (URISyntaxException e) {
      return false;
    }
  }

  @Override
  public void get(
      Compressor.Value compressor,
      Digest digest,
      long offset,
      long count,
      ServerCallStreamObserver<ByteString> blobObserver,
      RequestMetadata requestMetadata) {
    boolean readThrough = shouldReadThrough(requestMetadata);
    InputStream in;
    try {
      if (readThrough && !contains(digest, /* result= */ null)) {
        // really need to be able to reuse/restart the same write over
        // multiple requests - if we get successive read throughs for a single
        // digest, we should pick up from where we were last time
        // Also servers should affinitize
        // And share data, so that they can pick the same worker to pull from
        // if possible.
        Write write = getWrite(compressor, digest, UUID.randomUUID(), requestMetadata);
        blobObserver.setOnCancelHandler(write::reset);
        in =
            new ReadThroughInputStream(
                newExternalInput(compressor, digest, 0),
                localOffset -> newTransparentInput(compressor, digest, localOffset),
                digest.getSize(),
                offset,
                write);
      } else {
        in = newInput(compressor, digest, offset);
      }
    } catch (IOException e) {
      blobObserver.onError(e);
      return;
    }

    blobObserver.setOnCancelHandler(
        () -> {
          try {
            in.close();
          } catch (IOException e) {
            log.log(Level.SEVERE, "error closing input stream on cancel", e);
          }
        });
    byte[] buffer = new byte[CHUNK_SIZE];
    int initialLength;
    try {
      initialLength = in.read(buffer);
    } catch (IOException e) {
      try {
        in.close();
      } catch (IOException ioEx) {
        log.log(Level.SEVERE, "error closing input stream on error", ioEx);
      }
      blobObserver.onError(e);
      return;
    }
    final class ReadOnReadyHandler implements Runnable {
      private boolean wasReady = false;

      private int len = initialLength;

      @Override
      public void run() {
        if (blobObserver.isReady() && !wasReady) {
          wasReady = true;
          try {
            sendBuffer();
          } catch (IOException e) {
            log.log(Level.SEVERE, "error reading from input stream", e);
            try {
              in.close();
            } catch (IOException ioEx) {
              log.log(Level.SEVERE, "error closing input stream on error", ioEx);
            }
            blobObserver.onError(e);
          }
        }
      }

      void sendBuffer() throws IOException {
        while (len >= 0 && wasReady) {
          if (len != 0) {
            blobObserver.onNext(ByteString.copyFrom(buffer, 0, len));
          }
          len = in.read(buffer);
          if (!blobObserver.isReady()) {
            wasReady = false;
          }
        }
        if (len < 0) {
          in.close();
          blobObserver.onCompleted();
        }
      }
    }
    blobObserver.setOnReadyHandler(new ReadOnReadyHandler());
  }

  boolean completeWrite(Digest digest) {
    // this should be traded for an event emission
    try {
      onPut.accept(digest);
    } catch (RuntimeException e) {
      log.log(
          Level.SEVERE,
          "error during write completion onPut for " + DigestUtil.toString(digest),
          e);
      /* ignore error, writes must complete */
    }
    try {
      return getFuture(digest).set(digest.getSize());
    } catch (Exception e) {
      log.log(
          Level.SEVERE,
          "error getting write in progress future for " + DigestUtil.toString(digest),
          e);
      return false;
    }
  }

  void invalidateWrite(Digest digest) {
    writesInProgress.invalidate(digest);
  }

  // TODO stop ignoring onExpiration
  @Override
  public void put(Blob blob, Runnable onExpiration) throws InterruptedException {
    Digest digest = blob.getDigest();
    String key = getKey(digest, false);
    try {
      log.log(Level.FINER, format("put: %s", key));
      OutputStream out =
          putImpl(
              key,
              digest.getDigestFunction(),
              UUID.randomUUID(),
              () -> completeWrite(digest),
              digest.getSize(),
              /* isExecutable= */ false,
              () -> invalidateWrite(digest),
              /* isReset= */ true);
      boolean referenced = out == null;
      try {
        if (out != null) {
          try {
            blob.getData().writeTo(out);
          } finally {
            out.close();
            referenced = true;
          }
        }
      } finally {
        if (referenced) {
          decrementReference(key);
        }
      }
    } catch (IOException e) {
      log.log(Level.SEVERE, "error putting " + DigestUtil.toString(digest), e);
    }
  }

  @Override
  public boolean isReadOnly() {
    return !state.isWritable();
  }

  public boolean setReadOnly(boolean value) {
    return state.setReadOnly(value);
  }

  @Override
  public void waitForWritable(Duration timeout) throws InterruptedException {
    synchronized (state) {
      state.wait(timeout.toMillis(), (int) (timeout.toNanos() % 1000000));
    }
  }

  @Override
  public Write getWrite(
      Compressor.Value compressor, Digest digest, UUID uuid, RequestMetadata requestMetadata)
      throws EntryLimitException {
    if (digest.getSize() == 0) {
      return new CompleteWrite(0);
    }
    if (digest.getSize() > maxEntrySizeInBytes) {
      throw new EntryLimitException(digest.getSize(), maxEntrySizeInBytes);
    }
    try {
      return writes.get(
          BlobWriteKey.newBuilder()
              .setDigest(digest)
              .setIdentifier(uuid.toString())
              .setCompressor(compressor)
              .build());
    } catch (ExecutionException e) {
      String compression = "";
      if (compressor == Compressor.Value.ZSTD) {
        compression = "zstd compressed ";
      }
      log.log(
          Level.SEVERE,
          "error getting " + compression + "write for " + DigestUtil.toString(digest) + ":" + uuid,
          e);
      throw new IllegalStateException("write create must not fail", e.getCause());
    }
  }

  SettableFuture<Long> getFuture(Digest digest) {
    try {
      return writesInProgress.get(digest);
    } catch (ExecutionException e) {
      Throwables.throwIfUnchecked(e.getCause());
      throw new UncheckedExecutionException(e.getCause());
    }
  }

  private static class UniqueWriteOutputStream extends CancellableOutputStream {
    private final CancellableOutputStream out;
    private final Consumer<Boolean> onClosed;
    private final long size;
    private boolean closed = false;

    UniqueWriteOutputStream(CancellableOutputStream out, Consumer<Boolean> onClosed, long size) {
      super(out);
      this.out = out;
      this.onClosed = onClosed;
      this.size = size;
    }

    // available to refer to replicable stream
    CancellableOutputStream delegate() {
      return out;
    }

    @Override
    public void write(int b) throws IOException {
      if (closed) {
        throw new IOException("write output stream is closed");
      }
      super.write(b);
    }

    @Override
    public void write(byte[] b) throws IOException {
      write(b, 0, b.length);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      if (closed) {
        throw new IOException("write output stream is closed");
      }
      super.write(b, off, len);
    }

    @Override
    public void close() throws IOException {
      // disallow any further writes
      closed = true;
      try {
        // we ignore closes below the complete size
        if (out.getWrittenForClose() >= size) {
          super.close();
        }
      } finally {
        onClosed.accept(/* cancelled= */ false);
      }
    }

    @Override
    public void cancel() throws IOException {
      try {
        out.cancel();
      } finally {
        onClosed.accept(/* cancelled= */ true);
      }
    }
  }

  Write newWrite(BlobWriteKey key, SettableFuture<Long> future) {
    Write write =
        new Write() {
          CancellableOutputStream out = null;

          @GuardedBy("this")
          boolean isReset = false;

          @GuardedBy("this")
          SettableFuture<Void> closedFuture = null;

          @GuardedBy("this")
          long fileCommittedSize = -1;

          @Override
          public synchronized void reset() {
            try {
              if (out != null) {
                out.cancel();
                out = null;
              }
            } catch (IOException e) {
              log.log(
                  Level.SEVERE,
                  "could not reset write "
                      + DigestUtil.toString(key.getDigest())
                      + ":"
                      + key.getIdentifier(),
                  e);
            } finally {
              if (closedFuture != null) {
                closedFuture.set(null);
              }
              isReset = true;
            }
          }

          @Override
          public synchronized long getCommittedSize() {
            long committedSize = getCommittedSizeFromOutOrDisk();
            if (committedSize == 0 && out == null) {
              isReset = true;
            }
            return committedSize;
          }

          long getCommittedSizeFromOutOrDisk() {
            if (isComplete()) {
              return key.getDigest().getSize();
            }
            return getCommittedSizeFromOut();
          }

          synchronized long getCommittedSizeFromOut() {
            if (out == null) {
              if (fileCommittedSize < 0) {
                // we need to cache this from disk until an out stream is acquired
                String blobKey = getKey(key.getDigest(), false);
                Path blobKeyPath = getPath(blobKey);
                try {
                  fileCommittedSize =
                      Files.size(blobKeyPath.resolveSibling(blobKey + "." + key.getIdentifier()));
                } catch (IOException e) {
                  fileCommittedSize = 0;
                }
              }
              return fileCommittedSize;
            }
            return out.getWritten();
          }

          @Override
          public synchronized boolean isComplete() {
            return getFuture().isDone()
                || ((closedFuture == null || closedFuture.isDone())
                    && containsLocal(key.getDigest(), /* result= */ null, (key) -> {}));
          }

          @Override
          public synchronized ListenableFuture<FeedbackOutputStream> getOutputFuture(
              long offset,
              long deadlineAfter,
              TimeUnit deadlineAfterUnits,
              Runnable onReadyHandler) {
            if (closedFuture == null || closedFuture.isDone()) {
              try {
                // this isn't great, and will block when there are multiple requesters
                return immediateFuture(
                    getOutput(offset, deadlineAfter, deadlineAfterUnits, onReadyHandler));
              } catch (IOException e) {
                return immediateFailedFuture(e);
              }
            }
            return transformAsync(
                closedFuture,
                result ->
                    getOutputFuture(offset, deadlineAfter, deadlineAfterUnits, onReadyHandler),
                directExecutor());
          }

          private synchronized void syncCancelled() {
            out = null;
            isReset = true;
          }

          @Override
          public synchronized FeedbackOutputStream getOutput(
              long offset, long deadlineAfter, TimeUnit deadlineAfterUnits, Runnable onReadyHandler)
              throws IOException {
            // caller will be the exclusive owner of this write stream. all other requests
            // will block until it is returned via a close.
            if (closedFuture != null) {
              try {
                while (!closedFuture.isDone()) {
                  wait();
                }
                closedFuture.get();
              } catch (ExecutionException e) {
                throw new IOException(e.getCause());
              } catch (InterruptedException e) {
                throw new IOException(e);
              }
            }
            if (offset == 0) {
              reset();
            }
            if (isComplete()) {
              if (!future.isDone()) {
                log.log(Level.WARNING, format("%s isComplete but has not completed future", key));
                future.set(key.getDigest().getSize());
              }
              throw new WriteCompleteException();
            }
            checkState(
                getCommittedSize() == offset,
                format(
                    "cannot position stream to %d, committed_size is %d",
                    offset, getCommittedSize()));
            SettableFuture<Void> outClosedFuture = SettableFuture.create();
            Digest digest = key.getDigest();
            UniqueWriteOutputStream uniqueOut =
                createUniqueWriteOutput(
                    out,
                    digest,
                    UUID.fromString(key.getIdentifier()),
                    cancelled -> {
                      if (cancelled) {
                        syncCancelled();
                      }
                      outClosedFuture.set(null);
                    },
                    this::isComplete,
                    isReset);
            if (uniqueOut.getPath() == null) {
              // this is a duplicate output stream and the write is complete
              future.set(key.getDigest().getSize());
              return uniqueOut;
            } else {
              commitOpenState(uniqueOut.delegate(), outClosedFuture);
              switch (key.getCompressor()) {
                case IDENTITY:
                  return uniqueOut;
                case ZSTD:
                  return new ZstdDecompressingOutputStream(uniqueOut, zstdBufferPool);
                default:
                  throw new UnsupportedOperationException(
                      "Unsupported compressor " + key.getCompressor());
              }
            }
          }

          private synchronized void syncNotify() {
            notify();
          }

          private synchronized void commitOpenState(
              CancellableOutputStream out, SettableFuture<Void> closedFuture) {
            // transition the Write to an open state, and modify all internal state required
            // atomically
            // this function must. not. throw.

            this.out = out;
            this.closedFuture = closedFuture;
            closedFuture.addListener(this::syncNotify, directExecutor());
            // they will likely write to this, so we can no longer assume isReset.
            // might want to subscribe to a write event on the stream
            isReset = false;
            // our cached file committed size is now invalid
            fileCommittedSize = -1;
          }

          @Override
          public ListenableFuture<Long> getFuture() {
            return future;
          }
        };
    write.getFuture().addListener(write::reset, expireService);
    return write;
  }

  UniqueWriteOutputStream createUniqueWriteOutput(
      CancellableOutputStream out,
      Digest digest,
      UUID uuid,
      Consumer<Boolean> onClosed,
      BooleanSupplier isComplete,
      boolean isReset)
      throws IOException {
    if (out == null) {
      out = newOutput(digest, uuid, isComplete, isReset);
    }
    if (out == null) {
      // duplicate output stream
      out =
          new CancellableOutputStream(nullOutputStream()) {
            @Override
            public long getWritten() {
              return digest.getSize();
            }

            @Override
            public void cancel() {}

            @Override
            public Path getPath() {
              return null;
            }
          };
    }

    // this stream is uniquely assigned to the consumer, can be closed,
    // and will properly reject any subsequent write activity with an
    // exception. It will not close the underlying stream unless we have
    // reached our digest point (or beyond).
    return new UniqueWriteOutputStream(out, onClosed, digest.getSize());
  }

  CancellableOutputStream newOutput(
      Digest digest, UUID uuid, BooleanSupplier isComplete, boolean isReset) throws IOException {
    String key = getKey(digest, false);
    final CancellableOutputStream cancellableOut;
    try {
      log.log(Level.FINER, format("getWrite: %s", key));
      cancellableOut =
          putImpl(
              key,
              digest.getDigestFunction(),
              uuid,
              () -> completeWrite(digest),
              digest.getSize(),
              /* isExecutable= */ false,
              () -> invalidateWrite(digest),
              isReset);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException(e);
    }
    if (cancellableOut == null) {
      decrementReference(key);
      return null;
    }
    return new CancellableOutputStream(cancellableOut) {
      final AtomicBoolean closed = new AtomicBoolean(false);

      @Override
      public void write(int b) throws IOException {
        try {
          super.write(b);
        } catch (ClosedChannelException e) {
          if (!isComplete.getAsBoolean()) {
            throw e;
          }
        }
      }

      @Override
      public void write(byte[] b) throws IOException {
        try {
          super.write(b);
        } catch (ClosedChannelException e) {
          if (!isComplete.getAsBoolean()) {
            throw e;
          }
        }
      }

      @Override
      public void write(byte[] b, int off, int len) throws IOException {
        try {
          super.write(b, off, len);
        } catch (ClosedChannelException e) {
          if (!isComplete.getAsBoolean()) {
            throw e;
          }
        }
      }

      @Override
      public void cancel() throws IOException {
        if (closed.compareAndSet(/* expected= */ false, /* update= */ true)) {
          cancellableOut.cancel();
        }
      }

      @Override
      public void close() throws IOException {
        if (closed.compareAndSet(/* expected= */ false, /* update= */ true)) {
          try {
            out.close();
            decrementReference(key);
          } catch (IncompleteBlobException e) {
            // ignore
          }
        }
      }

      @Override
      public long getWrittenForClose() {
        return cancellableOut.getWrittenForClose();
      }
    };
  }

  @Override
  public void put(Blob blob) throws InterruptedException {
    put(blob, /* onExpiration= */ null);
  }

  @Override
  public long maxEntrySize() {
    return maxEntrySizeInBytes;
  }

  private record FileEntryKey(String key, long size, boolean isExecutable, Digest digest) {}

  public void initializeRootDirectory() throws IOException {
    for (Path dir : entryPathStrategy) {
      Files.createDirectories(dir);
    }
    fileStore = Files.getFileStore(root);
    try {
      blockSize = fileStore.getBlockSize();
    } catch (UnsupportedOperationException | IOException e) {
      // blockSize retains its initial value of DEFAULT_BLOCK_SIZE
    }
  }

  @VisibleForTesting
  void setBlockSizeForTesting(long blockSize) {
    this.blockSize = blockSize;
  }

  @VisibleForTesting
  void setSizeInBytesForTesting(long sizeInBytes) {
    totalBytes.reset();
    totalBytes.add(sizeInBytes);
  }

  @SuppressWarnings({"PMD.CompareObjectsWithEquals"})
  @GuardedBy("lruLock")
  private List<SizeEntry> lruSizeEntryList() {
    /**
     * Steps the entries in order from oldest to newest access The order is used here to insert into
     * the lru on load
     */
    List<SizeEntry> list = new ArrayList<>(storage.size());
    for (Entry current = header.after; current != header; current = checkNotNull(current.after)) {
      list.add(new SizeEntry(current.key, current.size));
    }
    return list;
  }

  public void stop() throws IOException, InterruptedException {
    if (prometheusMetricsThread != null) {
      prometheusMetricsThread.interrupt();
      prometheusMetricsThread.join();
    }
    saveLRU();
    state.stop();
  }

  public ListenableFuture<Void> start(boolean skipLoad) {
    return start(newDirectExecutorService(), skipLoad, /* writable= */ true);
  }

  public ListenableFuture<Void> start(
      ExecutorService removeDirectoryService, boolean skipLoad, boolean writable) {
    return start(onPut, removeDirectoryService, skipLoad, writable);
  }

  public ListenableFuture<Void> start(
      Consumer<Digest> onStartPut,
      ExecutorService removeDirectoryService,
      boolean skipLoad,
      boolean writable) {
    state.start();
    CasFallbackDelegate.start(
        delegate, onStartPut, removeDirectoryService, delegateSkipLoad, writable);

    return listeningDecorator(expireService)
        .submit(
            () -> {
              startRoutine(onStartPut, removeDirectoryService, skipLoad);
              synchronized (this) {
                saveLRUAfter = Deadline.after(10, MINUTES);
                checkState(state.setReadOnly(!writable));
              }
              return null;
            });
  }

  /**
   * initialize the cache for persistent storage and inject any consistent entries which already
   * exist under the root into the storage map. This call will create the root if it does not exist,
   * and will scale in cost with the number of files already present.
   */
  private void startRoutine(
      Consumer<Digest> onStartPut, ExecutorService removeDirectoryService, boolean skipLoad)
      throws IOException, InterruptedException {
    log.log(Level.INFO, "Initializing cache at: " + root);
    Instant startTime = Instant.now();

    // Load the cache
    if (!skipLoad) {
      initializeRootDirectory();
      loadCache(onStartPut, removeDirectoryService);
    } else {
      // Skip loading the cache and ensure it is empty
      fileStore = Files.getFileStore(root);
      Directories.remove(root, fileStore, removeDirectoryService);
      initializeRootDirectory();
    }

    // Calculate Startup time
    Instant endTime = Instant.now();
    Duration startupTime = Duration.between(startTime, endTime);
    log.log(Level.INFO, "Startup Time: " + startupTime.getSeconds() + "s");

    // Start metrics collection thread
    prometheusMetricsThread =
        new Thread(
            () -> {
              while (!Thread.currentThread().isInterrupted()) {
                try {
                  casSizeMetric.set(size());
                  casEntryCountMetric.set(entryCount());
                  MINUTES.sleep(5);
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  break;
                } catch (Exception e) {
                  log.log(Level.SEVERE, "Could not update CasFileCache metrics", e);
                }
              }
            },
            "Prometheus CAS Metrics Collector");
    prometheusMetricsThread.start();
  }

  protected CacheLoadResults loadCache(
      Consumer<Digest> onStartPut, ExecutorService removeDirectoryService)
      throws IOException, InterruptedException {
    // Phase 1: Scan
    // build scan cache results by analyzing each file on the root.
    CacheScanResults scan = scanRoot(onStartPut);
    logCacheScanResults(scan);
    deleteInvalidFileContent(scan.deleteFiles, removeDirectoryService);

    // Phase 2: Compute
    // recursively construct all directory structures.
    List<Path> invalidDirectories = computeDirectories(scan);
    logComputeDirectoriesResults(invalidDirectories);
    deleteInvalidFileContent(invalidDirectories, removeDirectoryService);

    return new CacheLoadResults(false, scan, invalidDirectories);
  }

  private void deleteInvalidFileContent(List<Path> files, ExecutorService removeDirectoryService) {
    for (Path path : files) {
      try {
        if (Files.isDirectory(path)) {
          Directories.remove(path, fileStore, removeDirectoryService);
        } else {
          Files.delete(path);
        }
      } catch (Exception e) {
        log.log(Level.SEVERE, "failure to delete CAS content: ", e);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private void logCacheScanResults(CacheScanResults cacheScanResults) {
    Map<String, Integer> map =
        Map.of(
            "dirs", cacheScanResults.computeDirs.size(),
            "delete", cacheScanResults.deleteFiles.size());
    log.log(Level.INFO, new Gson().toJson(map));
  }

  @SuppressWarnings("unchecked")
  private void logComputeDirectoriesResults(List<Path> invalidDirectories) {
    Map<String, Integer> map = Map.of("invalid dirs", invalidDirectories.size());
    log.log(Level.INFO, new Gson().toJson(map));
  }

  protected boolean shouldDeleteBranchFile(Path branchDir, String name) {
    return !name.matches("[0-9a-f]{2}");
  }

  private CacheScanResults scanRoot(Consumer<Digest> onStartPut)
      throws IOException, InterruptedException {
    // create thread pool
    ExecutorService pool = BuildfarmExecutors.getScanCachePool();

    // collect keys from cache root.
    ImmutableList.Builder<Path> computeDirsBuilder = new ImmutableList.Builder<>();
    ImmutableList.Builder<Path> deleteFilesBuilder = new ImmutableList.Builder<>();

    // TODO invalidate mismatched hash prefix
    Set<Path> files = new HashSet<>();
    for (Path path : entryPathStrategy) {
      files.addAll(listDir(path));
    }

    for (Path branchDir : entryPathStrategy.branchDirectories()) {
      for (Path file : listDir(branchDir)) {
        // allow migration for digest-y names
        String name = file.getFileName().toString();
        if (shouldDeleteBranchFile(branchDir, name)) {
          deleteFilesBuilder.add(file);
        }
      }
    }

    // TODO test for hex bins
    try (BufferedReader br = Files.newBufferedReader(lru)) {
      for (SizeEntry entry : db.entries(br)) {
        // ignore files in the lru that are not present in the directories
        Path path = entryPathStrategy.getPath(entry.key());
        if (files.remove(path)) {
          processRootFile(onStartPut, path, entry, computeDirsBuilder, deleteFilesBuilder);
        }
      }
      // prevent the lru db from being processed -> removed in the purge below
      files.remove(lru);
    } catch (NoSuchFileException e) {
      // ignore - LRU file doesn't exist, will scan all files
    } catch (Exception e) {
      // Handle corrupted LRU file - delete it and fall back to full scan
      log.log(
          Level.WARNING,
          "LRU file is corrupted and cannot be parsed. Deleting corrupted LRU file and falling back"
              + " to full cache scan.",
          e);
      try {
        Files.deleteIfExists(lru);
        log.log(Level.INFO, "Deleted corrupted LRU file: " + lru);
      } catch (IOException deleteEx) {
        log.log(Level.SEVERE, "Failed to delete corrupted LRU file: " + lru, deleteEx);
      }
      // Continue with full scan - all files will be processed in the loop below
    }
    for (Path file : files) {
      String basename = file.getFileName().toString();
      pool.execute(
          () -> {
            try {
              FileStatus stat = stat(file, false, fileStore);
              processRootFile(
                  onStartPut,
                  file,
                  new SizeEntry(basename, stat.getSize()),
                  computeDirsBuilder,
                  deleteFilesBuilder);
            } catch (Exception e) {
              log.log(Level.SEVERE, "error reading file " + file.toString(), e);
            }
          });
    }

    joinThreads(pool, "Scanning Cache Root...");

    // log information from scanning cache root.
    return new CacheScanResults(computeDirsBuilder.build(), deleteFilesBuilder.build(), null);
  }

  @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
  private void processRootFile(
      Consumer<Digest> onStartPut,
      Path path,
      SizeEntry entry,
      ImmutableList.Builder<Path> computeDirs,
      ImmutableList.Builder<Path> deleteFiles)
      throws IOException {
    String basename = entry.key();

    // mark directory for later key compute
    if (basename.endsWith("_dir")) {
      synchronized (computeDirs) {
        computeDirs.add(path);
      }
    } else {
      // if cas is full or entry is oversized or empty, mark file for later deletion.
      long size = entry.size();
      if (totalBytes.sum() + estimateSizeOnDisk(size, blockSize, /* isHardlink= */ false)
              > maxSizeInBytes
          || size > maxEntrySizeInBytes
          || size == 0) {
        synchronized (deleteFiles) {
          deleteFiles.add(path);
        }
      } else {
        // get the key entry from the file name.
        FileEntryKey fileEntryKey = parseFileEntryKey(basename, size);

        // if key entry file name cannot be parsed, mark file for later deletion.
        if (fileEntryKey == null) {
          synchronized (deleteFiles) {
            deleteFiles.add(path);
          }
        } else {
          String key = fileEntryKey.key();
          // populate key if it is not currently stored.
          Entry e = Entry.orphan(key, size, Deadline.after(10, SECONDS));
          checkState(storage.put(e.key, e) == null, key);
          onStartPut.accept(fileEntryKey.digest());
          totalBytes.add(estimateSizeOnDisk(size, blockSize, /* isHardlink= */ false));
          lruLock.lock();
          try {
            linkUnreferenced(e);
          } finally {
            lruLock.unlock();
          }
        }
      }
    }
  }

  @SuppressWarnings("ConstantConditions")
  protected abstract List<Path> computeDirectories(CacheScanResults cacheScanResults)
      throws InterruptedException;

  @SuppressWarnings("ResultOfMethodCallIgnored")
  protected static void joinThreads(ExecutorService pool, String message)
      throws InterruptedException {
    pool.shutdown();
    while (!pool.isTerminated()) {
      log.log(Level.INFO, message);
      pool.awaitTermination(1, MINUTES);
    }
  }

  static String digestFilename(Digest digest) {
    return optionalDigestFunction(digest.getDigestFunction()) + digest.getHash();
  }

  private static String optionalDigestFunction(DigestFunction.Value digestFunction) {
    if (OMITTED_DIGEST_FUNCTIONS.contains(digestFunction)) {
      return "";
    }
    return digestFunction.toString().toLowerCase() + "_";
  }

  public static String getKey(Digest digest, boolean isExecutable) {
    return digestFilename(digest) + (isExecutable ? "_exec" : "");
  }

  public void decrementReference(String inputFile) throws IOException {
    decrementInputReferences(ImmutableList.of(inputFile));
  }

  public abstract void decrementReferences(
      Iterable<String> inputFiles,
      Iterable<build.bazel.remote.execution.v2.Digest> inputDirectories,
      DigestFunction.Value digestFunction)
      throws IOException, InterruptedException;

  protected int decrementInputReferences(Iterable<String> inputFiles) {
    // Collect entries that hit 1 -> 0 so a single lruLock acquisition appends them all
    // and signals once at the end. An action releasing N inputs would otherwise take N
    // independent lock acquisitions on the eviction lock.
    List<Entry> toLink = null;
    for (String input : inputFiles) {
      checkNotNull(input);
      Entry e = storage.get(input);
      if (e == null) {
        throw new IllegalStateException(input + " has been removed with references");
      }
      if (!e.key.equals(input)) {
        throw new RuntimeException("ERROR: entry retrieved: " + e.key + " != " + input);
      }
      if (e.release()) {
        if (toLink == null) {
          toLink = new ArrayList<>();
        }
        toLink.add(e);
      }
    }
    if (toLink == null) {
      return 0;
    }
    lruLock.lock();
    try {
      for (Entry e : toLink) {
        linkUnreferenced(e);
      }
      // signalAll because we may have linked N entries; one signal would wake only
      // one parked charger and could leave others stuck if linkUnreferenced is the
      // only signal source until the next batched release.
      lruCondition.signalAll();
    } finally {
      lruLock.unlock();
    }
    return toLink.size();
  }

  public Path getPath(String filename) {
    return entryPathStrategy.getPath(filename);
  }

  protected Path getRemovingPath(String filename) {
    return entryPathStrategy.getPath(filename + "_removed");
  }

  private void dischargeAndNotify(String key, long size) {
    lruLock.lock();
    try {
      dischargeLocked(key, size);
    } finally {
      lruLock.unlock();
    }
  }

  /**
   * Discharge variant for callers already holding {@code lruLock}. The signal is required — a bare
   * {@link #discharge} that drops totalBytes below maxSizeInBytes leaves chargers parked in {@link
   * #waitForLastUnreferencedEntry}'s {@code await()} with no edge to wake them on. Every
   * evictor-path discharge routes through this helper or {@link #dischargeAndNotify}.
   */
  @GuardedBy("lruLock")
  private void dischargeLocked(String key, long size) {
    discharge(key, size);
    // signalAll because a discharge can drop totalBytes below maxSizeInBytes and unblock
    // every waiter in waitForLastUnreferencedEntry's totalBytes <= max exit branch.
    lruCondition.signalAll();
  }

  protected void discharge(String key, long size) {
    long diskSize = estimateSizeOnDisk(size, blockSize, /* isHardlink= */ false);
    totalBytes.add(-diskSize);
    removedCount.increment();
    removedBytes.add(diskSize);
  }

  @GuardedBy("lruLock")
  private void unlinkEntry(Entry entry) throws IOException {
    try {
      dischargeEntry(entry, expireService);
    } catch (Exception e) {
      throw new IOException(e);
    }
    // technically we should attempt to remove the file here,
    // but we're only called in contexts where it doesn't exist...
  }

  protected String getDirectoryKey(Digest digest) {
    return digestFilename(digest) + "_dir";
  }

  @VisibleForTesting
  public Path getDirectoryPath(Digest digest) {
    return getPath(getDirectoryKey(digest));
  }

  @VisibleForTesting
  @GuardedBy("lruLock")
  @SuppressWarnings("PMD.CompareObjectsWithEquals")
  Entry waitForLastUnreferencedEntry(long blobSizeInBytes) throws InterruptedException {
    while (header.after == header) { // Intentional reference comparison
      if (storage.isEmpty()) {
        throw new IllegalStateException(
            "CASFileCache::expireEntry("
                + blobSizeInBytes
                + ") there are no keys to wait for expiration on");
      }
      // The detailed walk is O(N) over storage and is only used for diagnostics. At
      // hundreds-of-millions of entries it dominates each wait cycle while the
      // eviction lock is held. Gate it behind FINE; emit a lightweight INFO summary
      // on the production path.
      if (log.isLoggable(Level.FINE)) {
        logDetailedExpireStats(blobSizeInBytes);
      } else {
        log.log(
            Level.INFO,
            format(
                "CASFileCache::expireEntry(%d) waiting: %d bytes, %d keys",
                blobSizeInBytes, totalBytes.sum(), storage.size()));
      }
      lruCondition.await();
      if (totalBytes.sum() <= maxSizeInBytes) {
        return null;
      }
    }
    return header.after;
  }

  @GuardedBy("lruLock")
  @SuppressWarnings("PMD.CompareObjectsWithEquals")
  private void logDetailedExpireStats(long blobSizeInBytes) {
    int references = 0;
    int keys = 0;
    int min = -1;
    int max = 0;
    String minkey = null;
    String maxkey = null;
    log.log(
        Level.FINE,
        format(
            "CASFileCache::expireEntry(%d) header(%s): { after: %s, before: %s }",
            blobSizeInBytes, header.hashCode(), header.after.hashCode(), header.before.hashCode()));
    for (Map.Entry<String, Entry> pe : storage.entrySet()) {
      String key = pe.getKey();
      Entry e = pe.getValue();
      int rc = e.refCount();
      if (rc > max) {
        max = rc;
        maxkey = key;
      }
      if (min == -1 || rc < min) {
        min = rc;
        minkey = key;
      }
      if (rc == 0) {
        log.log(
            Level.FINE,
            format(
                "CASFileCache::expireEntry(%d) unreferenced entry(%s): { after: %s, before: %s }",
                blobSizeInBytes,
                e.hashCode(),
                e.after == null ? null : e.after.hashCode(),
                e.before == null ? null : e.before.hashCode()));
      }
      references += rc;
      keys++;
    }
    log.log(
        Level.FINE,
        format(
            "CASFileCache::expireEntry(%d) unreferenced list is empty, %d bytes, %d keys with %d"
                + " references, min(%d, %s), max(%d, %s)",
            blobSizeInBytes, totalBytes.sum(), keys, references, min, minkey, max, maxkey));
  }

  protected abstract List<ListenableFuture<Void>> unlinkAndExpireDirectories(
      Entry entry, ExecutorService service);

  @GuardedBy("lruLock")
  protected ListenableFuture<Entry> dischargeEntryFuture(Entry entry, ExecutorService service) {
    // Mirror dischargeEntry's discharge-in-finally so callers can hoist `discharged = true`
    // BEFORE this call: by the time this method returns or throws synchronously, discharge()
    // has run exactly once. Without the finally, a throw from unlinkAndExpireDirectories
    // (subclass overrides catch internally today, but the contract doesn't forbid a throw)
    // or from whenAllComplete().call() construction would leave the caller's outer finally
    // looking at discharged=false and double-decrement totalBytes.
    List<ListenableFuture<Void>> directoryExpirationFutures;
    try {
      directoryExpirationFutures = unlinkAndExpireDirectories(entry, service);
    } finally {
      dischargeLocked(entry.key, entry.size);
    }
    return whenAllComplete(directoryExpirationFutures)
        .call(
            () -> {
              Exception expirationException = null;
              for (ListenableFuture<Void> directoryExpirationFuture : directoryExpirationFutures) {
                try {
                  directoryExpirationFuture.get();
                } catch (ExecutionException e) {
                  Throwable cause = e.getCause();
                  if (cause instanceof Exception) {
                    expirationException = (Exception) cause;
                  } else {
                    log.log(
                        Level.SEVERE,
                        "undeferrable exception during discharge of " + entry.key,
                        cause);
                    // errors and the like, avoid any deferrals
                    Throwables.throwIfUnchecked(cause);
                    throw new RuntimeException(cause);
                  }
                } catch (InterruptedException e) {
                  // unlikely, all futures must be complete
                }
              }
              if (expirationException != null) {
                throw expirationException;
              }
              return entry;
            },
            service);
  }

  @GuardedBy("lruLock")
  private void dischargeEntry(Entry entry, ExecutorService service) throws Exception {
    Exception expirationException = null;
    // discharge in a finally so totalBytes accounting is reconciled even when a
    // directory-expiration future failure rethrows past us — callers rely on the
    // contract "by the time dischargeEntry returns or throws, discharge() has run".
    // Without it, the catch path in newLocalInput / referenceIfExists would see
    // discharged=false and double-decrement totalBytes after dischargeEntry's own
    // discharge already ran.
    try {
      for (ListenableFuture<Void> directoryExpirationFuture :
          unlinkAndExpireDirectories(entry, service)) {
        do {
          try {
            directoryExpirationFuture.get();
          } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Exception) {
              expirationException = (Exception) cause;
            } else {
              log.log(
                  Level.SEVERE, "undeferrable exception during discharge of " + entry.key, cause);
              // errors and the like, avoid any deferrals
              Throwables.throwIfUnchecked(cause);
              throw new RuntimeException(cause);
            }
          } catch (InterruptedException e) {
            // FIXME add some suppression
            expirationException = e;
          }
        } while (!directoryExpirationFuture.isDone());
      }
    } finally {
      // only discharge after all the directories are gone, or their removal failed
      dischargeLocked(entry.key, entry.size);
    }
    if (expirationException != null) {
      throw expirationException;
    }
  }

  // clears the interrupted status
  private static boolean causedByInterrupted(Exception e) {
    return Thread.interrupted()
        || e.getCause() instanceof InterruptedException
        || e instanceof ClosedByInterruptException;
  }

  protected Entry safeStorageInsertion(String key, Entry entry) {
    Lock lock;
    try {
      lock = keyLocks.get(key);
    } catch (ExecutionException e) {
      // impossible without exception instantiating lock
      throw new RuntimeException(e.getCause());
    }

    lock.lock();
    try {
      return storage.putIfAbsent(key, entry);
    } finally {
      lock.unlock();
    }
  }

  private Entry safeStorageRemoval(String key) throws IOException {
    Path path = getPath(key);
    Path expiredPath = getRemovingPath(key);
    boolean deleteExpiredPath = false;

    Lock lock;
    try {
      lock = keyLocks.get(key);
    } catch (ExecutionException e) {
      // impossible without exception instantiating lock
      throw new IOException(e);
    }

    Entry entry;
    lock.lock();
    // why are we splitting IO between this and dischargeEntryFuture?
    try {
      if (key.endsWith("_dir")) {
        Files.move(path, expiredPath, ATOMIC_MOVE, REPLACE_EXISTING);
      } else {
        Files.createLink(expiredPath, path);
        deleteExpiredPath = true;
        Files.delete(path);
        deleteExpiredPath = false;
      }
    } catch (NoSuchFileException e) {
      // ignore, already removed
    } finally {
      entry = storage.remove(key);
      if (deleteExpiredPath) {
        // only valid for files with 2-step
        try {
          Files.delete(expiredPath);
        } catch (IOException e) {
          log.log(Level.SEVERE, "error cleaning up after failed safeStorageRemoval", e);
        }
      }
      lock.unlock();
    }
    // the directory remains, guess we should get the service in here??
    return entry;
  }

  @SuppressWarnings({"PMD.CompareObjectsWithEquals"})
  @GuardedBy("lruLock")
  private ListenableFuture<Entry> expireEntry(long blobSizeInBytes, ExecutorService service)
      throws IOException, InterruptedException {
    for (Entry e = waitForLastUnreferencedEntry(blobSizeInBytes);
        e != null;
        e = waitForLastUnreferencedEntry(blobSizeInBytes)) {
      if (!e.tryEvict()) {
        // Resurrected by a concurrent acquirer; drop from the LRU so we don't pick it
        // again. The acquirer is parked on lruLock (which we hold) and will observe
        // !isLinked() in unlinkReferenced — meaning we own the decrement here.
        unlinkReferenced(e);
        continue;
      }
      // expireEntryFallback / safeStorageRemoval / dischargeEntryFuture can throw; without
      // this finally, a throw between tryEvict and completeEviction would strand the entry
      // in EVICTING permanently. The !completed path also unlinks and discharges so a
      // disk-pressure IOException (Files.createLink/delete) does not leak bytes from
      // totalBytes.
      boolean discharged = false;
      boolean completed = false;
      try {
        if (!e.key.endsWith("_dir")) {
          FileEntryKey fileEntryKey = parseFileEntryKey(e.key, e.size);
          if (fileEntryKey == null) {
            log.log(Level.SEVERE, format("error parsing expired key %s", e.key));
          } else {
            // The fallback copy is best-effort: a synchronous throw from
            // delegate.getWrite() / performCopy() that escapes expireEntryFallback's
            // EntryLimitException catch would skip safeStorageRemoval below and strand
            // e in storage (state=EVICTED, file on disk, unevictable + unacquirable).
            // The outer !completed finally restores the LRU/accounting/state but cannot
            // see e in storage. Log-and-continue so storage cleanup runs unconditionally.
            try {
              expireEntryFallback(fileEntryKey);
              invalidateWrite(fileEntryKey.digest());
            } catch (RuntimeException re) {
              log.log(
                  Level.SEVERE,
                  format("expireEntryFallback failed for %s; continuing with removal", e.key),
                  re);
            }
          }
        }
        Entry removedEntry = safeStorageRemoval(e.key);
        // reference compare on purpose
        if (removedEntry == e) {
          // Hoist discharged=true BEFORE dischargeEntryFuture so an early synchronous throw
          // (unlinkAndExpireDirectories or whenAllComplete construction) does not leave
          // the outer finally double-discharging — dischargeEntryFuture's try/finally
          // guarantees discharge() runs on every exit path.
          discharged = true;
          ListenableFuture<Entry> result = dischargeEntryFuture(e, service);
          e.completeEviction();
          completed = true;
          return result;
        }
        if (removedEntry == null) {
          // Defensive: per the protocol (only the tryEvict winner can call safeStorageRemoval,
          // and we are that winner), storage.remove should always find e. If it doesn't, e's
          // bytes are still charged to totalBytes and e is still on the LRU — without
          // discharging here, the bytes leak permanently and the resurrection branch would
          // later unlink without discharging.
          log.log(Level.SEVERE, format("entry %s was already removed during expiration", e.key));
          if (e.isLinked()) {
            log.log(Level.SEVERE, format("removing spuriously non-existent entry %s", e.key));
            unlinkReferenced(e);
          } else {
            log.log(
                Level.SEVERE,
                format(
                    "spuriously non-existent entry %s was somehow unlinked, should not appear"
                        + " again",
                    e.key));
          }
          dischargeLocked(e.key, e.size);
          discharged = true;
          e.completeEviction();
          completed = true;
        } else {
          log.log(
              Level.SEVERE,
              "removed entry %s did not match last unreferenced entry, restoring it",
              e.key);
          storage.put(e.key, removedEntry);
          // e was on the LRU (header.after) and its bytes were charged to totalBytes,
          // but storage now holds a different Entry for this key. Without unlinking +
          // discharging, e strands on the LRU as EVICTED with its bytes permanently
          // in totalBytes — the resurrection branch above would later unlink it but
          // would not discharge.
          unlinkReferenced(e);
          dischargeLocked(e.key, e.size);
          discharged = true;
          // Mark this Entry instance terminal; the restored Entry is a different
          // identity with its own state.
          e.completeEviction();
          completed = true;
        }
      } finally {
        if (!completed) {
          unlinkReferenced(e);
          if (!discharged) {
            dischargeLocked(e.key, e.size);
          }
          e.completeEviction();
        }
      }
      // possibly delegated, but no removal, if we're interrupted, abort loop
      if (Thread.currentThread().isInterrupted()) {
        throw new InterruptedException();
      }
    }
    return null;
  }

  interface FileContent {
    void call(Path filePath, Path cacheFilePath, long size, boolean isExecutable) throws Exception;
  }

  @SuppressWarnings("ConstantConditions")
  private void putDirectoryFiles(
      DigestFunction.Value digestFunction,
      Iterable<FileNode> files,
      Iterable<SymlinkNode> symlinks,
      Path path,
      FileContent onFileContent,
      ImmutableList.Builder<ListenableFuture<Path>> putFutures,
      ExecutorService service) {
    for (FileNode fileNode : files) {
      boolean isExecutable = fileNode.getIsExecutable();
      Path filePath = path.resolve(fileNode.getName());
      final ListenableFuture<Path> putFuture;
      if (fileNode.getDigest().getSizeBytes() != 0) {
        Digest digest = DigestUtil.fromDigest(fileNode.getDigest(), digestFunction);
        putFuture =
            transformAsync(
                put(digest, isExecutable, service),
                cacheFilePath -> {
                  try {
                    onFileContent.call(
                        filePath, cacheFilePath.path(), digest.getSize(), isExecutable);
                  } catch (Exception e) {
                    return immediateFailedFuture(e);
                  }
                  return immediateFuture(filePath);
                },
                service);
      } else {
        putFuture =
            listeningDecorator(service)
                .submit(
                    () -> {
                      Files.createFile(filePath);
                      setReadOnlyPerms(filePath, isExecutable, fileStore);
                      return filePath;
                    });
      }
      putFutures.add(putFuture);
    }
    for (SymlinkNode symlinkNode : symlinks) {
      Path symlinkPath = path.resolve(symlinkNode.getName());
      putFutures.add(
          listeningDecorator(service)
              .submit(
                  () -> {
                    Path relativeTargetPath = root.getFileSystem().getPath(symlinkNode.getTarget());
                    checkState(!relativeTargetPath.isAbsolute());
                    Files.createSymbolicLink(symlinkPath, relativeTargetPath);
                    return symlinkPath;
                  }));
    }
  }

  /**
   * Estimates the physical on-disk size of a file by rounding up to the nearest filesystem block
   * boundary.
   *
   * <p>For many files and filesystems this will be accurate. However, for some files on some
   * filesystems this will overestimate the physical size by up to blockSize bytes. To clarify, this
   * likely overestimates for smaller files on filesystems that use inlining, block suballocation,
   * variable block sizes, etc. It can also overestimate for compressed filesystems proportional to
   * the compression ratio.
   *
   * <p>When {@code isHardlink} is {@code true}, the caller is creating (or has verified) a hardlink
   * to an already-resident inode rather than writing a fresh inode; the physical cost is then just
   * a directory entry (~64 bytes on typical filesystems, rounding to 0 under block alignment), so
   * this method returns 0. Callers must only pass {@code true} when a hardlink to an
   * already-accounted inode is being made.
   */
  @VisibleForTesting
  static long estimateSizeOnDisk(long logicalSize, long blockSize, boolean isHardlink) {
    checkArgument(blockSize > 0, "blockSize (%s) must be positive", blockSize);
    checkArgument(logicalSize >= 0, "logicalSize (%s) must be non-negative", logicalSize);
    if (isHardlink) {
      // Hardlinks reuse an existing inode's blocks and add only a directory entry (~64 bytes on
      // typical filesystems, which rounds to 0 under block alignment).
      return 0;
    }
    if (logicalSize == 0) {
      return 0;
    }
    // Use long division to get the ceiling in order to round up to the next largest blocksize.
    // This should return correct answers for sizes that are block aligned as well as those that
    // are not.
    return ((logicalSize + blockSize - 1) / blockSize) * blockSize;
  }

  /**
   * Estimates the on-disk size of a directory's entry table based on its contents. On typical Linux
   * filesystems (ext4, XFS), each directory entry is approximately 8 bytes of header plus the
   * filename length, aligned to 4 bytes. Each directory occupies at least one filesystem block.
   */
  @VisibleForTesting
  static long estimateDirectorySizeOnDisk(Directory directory, long blockSize) {
    checkArgument(blockSize > 0, "blockSize (%s) must be positive", blockSize);
    long entryCount =
        directory.getFilesCount() + directory.getSymlinksCount() + directory.getDirectoriesCount();
    // ~32 bytes per entry is a reasonable average for typical filename lengths
    // on ext4/XFS (8-byte header + ~20-char name + alignment padding)
    long estimatedBytes = entryCount * ESTIMATED_BYTES_PER_DIRECTORY_ENTRY;
    long blocks = Math.max(1, Math.ceilDiv(estimatedBytes, blockSize));
    return blocks * blockSize;
  }

  protected long fetchDirectory(
      Path rootPath,
      Digest digest,
      Map<build.bazel.remote.execution.v2.Digest, Directory> directoriesIndex,
      FileContent onFileContent,
      ImmutableList.Builder<ListenableFuture<Path>> putFutures,
      ExecutorService service)
      throws IOException, InterruptedException {
    long directoryOverhead = 0;
    Stack<Map.Entry<Path, Directory>> stack = new Stack<>();
    stack.push(
        new AbstractMap.SimpleEntry<>(
            rootPath, getDirectoryFromDigest(directoriesIndex, rootPath, digest)));
    while (!stack.isEmpty()) {
      Map.Entry<Path, Directory> pathDirectoryPair = stack.pop();
      Path path = pathDirectoryPair.getKey();
      Directory directory = pathDirectoryPair.getValue();

      removeFilePath(path);
      Files.createDirectory(path);
      directoryOverhead += estimateDirectorySizeOnDisk(directory, blockSize);
      putDirectoryFiles(
          digest.getDigestFunction(),
          directory.getFilesList(),
          directory.getSymlinksList(),
          path,
          onFileContent,
          putFutures,
          service);
      for (DirectoryNode directoryNode : directory.getDirectoriesList()) {
        Path subPath = path.resolve(directoryNode.getName());
        stack.push(
            new AbstractMap.SimpleEntry<>(
                subPath,
                getDirectoryFromDigest(
                    directoriesIndex,
                    subPath,
                    DigestUtil.fromDigest(directoryNode.getDigest(), digest.getDigestFunction()))));
      }
    }
    return directoryOverhead;
  }

  private void removeFilePath(Path path) throws IOException {
    if (Files.exists(path)) {
      if (Files.isDirectory(path)) {
        log.log(Level.FINER, "removing existing directory " + path + " for fetch");
        Directories.remove(path, fileStore);
      } else {
        Files.delete(path);
      }
    }
  }

  private Directory getDirectoryFromDigest(
      Map<build.bazel.remote.execution.v2.Digest, Directory> directoriesIndex,
      Path path,
      Digest digest)
      throws IOException {
    Directory directory;
    if (digest.getSize() == 0) {
      directory = Directory.getDefaultInstance();
    } else {
      directory = directoriesIndex.get(DigestUtil.toDigest(digest));
    }
    if (directory == null) {
      throw new IOException(
          format("directory not found for %s(%s)", path, DigestUtil.toString(digest)));
    }
    return directory;
  }

  public record PathResult(Path path, boolean isMissed) {}

  public abstract ListenableFuture<PathResult> putDirectory(
      Digest digest,
      Map<build.bazel.remote.execution.v2.Digest, Directory> directoriesIndex,
      ExecutorService service);

  @VisibleForTesting
  public PathResult put(Digest digest, boolean isExecutable)
      throws IOException, InterruptedException {
    checkState(digest.getSize() > 0, "file entries may not be empty");

    return putAndCopy(digest, isExecutable);
  }

  // This can result in deadlock if called with a direct executor. I'm unsure how to guard
  // against it, until we can get to using a current-download future
  public ListenableFuture<PathResult> put(Digest digest, boolean isExecutable, Executor executor) {
    checkState(digest.getSize() > 0, "file entries may not be empty");

    return transformAsync(
        immediateFuture(null),
        (result) -> immediateFuture(putAndCopy(digest, isExecutable)),
        executor);
  }

  @SuppressWarnings("ThrowFromFinallyBlock")
  PathResult putAndCopy(Digest digest, boolean isExecutable)
      throws IOException, InterruptedException {
    String key = getKey(digest, isExecutable);
    boolean downloadComplete = false;
    CancellableOutputStream out =
        putImpl(
            key,
            digest.getDigestFunction(),
            UUID.randomUUID(),
            () -> completeWrite(digest),
            digest.getSize(),
            isExecutable,
            () -> invalidateWrite(digest),
            /* isReset= */ true);
    if (out != null) {
      try {
        copyExternalInput(digest, out);
        downloadComplete = true;
      } finally {
        try {
          log.log(Level.FINER, format("closing output stream for %s", DigestUtil.toString(digest)));
          if (downloadComplete) {
            out.close();
          } else {
            out.cancel();
          }
          log.log(Level.FINER, format("output stream closed for %s", DigestUtil.toString(digest)));
        } catch (IOException e) {
          if (Thread.interrupted()) {
            log.log(
                Level.SEVERE,
                format("could not close stream for %s", DigestUtil.toString(digest)),
                e);
            //noinspection deprecation
            Throwables.propagateIfInstanceOf(e.getCause(), InterruptedException.class);
            throw new InterruptedException();
          } else {
            log.log(
                Level.FINER,
                format("failed output stream close for %s", DigestUtil.toString(digest)),
                e);
          }
          throw e;
        }
      }
    }
    return new PathResult(getPath(key), downloadComplete);
  }

  private void copyExternalInputProgressive(Digest digest, CancellableOutputStream out)
      throws IOException, InterruptedException {
    try (InputStream in = newExternalInput(Compressor.Value.IDENTITY, digest, out.getWritten())) {
      ByteStreams.copy(in, out);
    }
  }

  private static Exception extractStatusException(IOException e) {
    for (Throwable cause = e.getCause(); cause != null; cause = cause.getCause()) {
      if (cause instanceof StatusException statusException) {
        return statusException;
      } else if (cause instanceof StatusRuntimeException statusRuntimeException) {
        return statusRuntimeException;
      }
    }
    return e;
  }

  private void copyExternalInput(Digest digest, CancellableOutputStream out)
      throws IOException, InterruptedException {
    Retrier retrier = new Retrier(Backoff.sequential(5), Retrier.DEFAULT_IS_RETRIABLE);
    log.log(Level.FINER, format("downloading %s", DigestUtil.toString(digest)));
    try {
      retrier.execute(
          () -> {
            while (out.getWritten() < digest.getSize()) {
              try {
                copyExternalInputProgressive(digest, out);
              } catch (IOException e) {
                throw extractStatusException(e);
              }
            }
            return null;
          });
    } catch (IOException e) {
      out.cancel();
      log.log(
          Level.WARNING,
          format(
              "error downloading %s: %s",
              DigestUtil.toString(digest),
              e.getMessage())); // prevent burial by early end of stream during close
      throw e;
    }
    log.log(Level.FINER, format("download of %s complete", DigestUtil.toString(digest)));
  }

  @FunctionalInterface
  private interface IORunnable {
    void run() throws IOException;
  }

  @VisibleForTesting
  abstract static class CancellableOutputStream extends WriteOutputStream {
    CancellableOutputStream(OutputStream out) {
      super(out);
    }

    CancellableOutputStream(WriteOutputStream out) {
      super(out);
    }

    abstract void cancel() throws IOException;

    long getWrittenForClose() {
      return getWritten();
    }
  }

  private static final CancellableOutputStream DUPLICATE_OUTPUT_STREAM =
      new CancellableOutputStream(nullOutputStream()) {
        @Override
        void cancel() {}
      };

  private CancellableOutputStream putImpl(
      String key,
      DigestFunction.Value digestFunction,
      UUID writeId,
      Supplier<Boolean> writeWinner,
      long blobSizeInBytes,
      boolean isExecutable,
      Runnable onInsert,
      boolean isReset)
      throws IOException, InterruptedException {
    CancellableOutputStream out =
        putOrReference(
            key,
            digestFunction,
            writeId,
            writeWinner,
            blobSizeInBytes,
            isExecutable,
            onInsert,
            isReset);
    if (out == DUPLICATE_OUTPUT_STREAM) {
      return null;
    }
    log.log(Level.FINER, format("entry %s is missing, downloading and populating", key));
    return newCancellableOutputStream(out);
  }

  private CancellableOutputStream newCancellableOutputStream(
      CancellableOutputStream cancellableOut) {
    return new CancellableOutputStream(cancellableOut) {
      boolean terminated = false;

      @Override
      public void cancel() throws IOException {
        withSingleTermination(cancellableOut::cancel);
      }

      @Override
      public void close() throws IOException {
        withSingleTermination(cancellableOut::close);
      }

      private void withSingleTermination(IORunnable runnable) throws IOException {
        if (!terminated) {
          try {
            runnable.run();
          } finally {
            terminated = true;
          }
        }
      }

      @Override
      public long getWrittenForClose() {
        return cancellableOut.getWrittenForClose();
      }
    };
  }

  private static final class SkipOutputStream extends FilterOutputStream {
    private long skip;

    SkipOutputStream(OutputStream out, long skip) {
      super(out);
      this.skip = skip;
    }

    @Override
    public void write(int b) throws IOException {
      if (skip > 0) {
        skip--;
      } else {
        super.write(b);
      }
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      if (skip > 0) {
        int skipLen = (int) Math.min(skip, len);
        skip -= skipLen;
        len -= skipLen;
        off += skipLen;
      }
      if (len > 0) {
        super.write(b, off, len);
      }
    }

    boolean isSkipped() {
      return skip == 0;
    }
  }

  protected boolean referenceIfExists(String key) throws IOException {
    Entry e = storage.get(key);
    if (e == null) {
      return false;
    }

    if (!entryExists(e)) {
      if (e.tryEvict()) {
        // unlinkEntry can throw IOException via dischargeEntry; without the finally
        // a throw between tryEvict and completeEviction would strand the entry in
        // EVICTING permanently. The catch path also unlinks and discharges so a
        // disk-pressure IOException does not leak bytes from totalBytes.
        boolean discharged = false;
        boolean completed = false;
        try {
          // reference compare on purpose — mirror newLocalInput's three-arm structure.
          Entry removedEntry = storage.remove(key);
          if (removedEntry == e) {
            lruLock.lock();
            try {
              // Set discharged before unlinkEntry: dischargeEntry's finally guarantees
              // discharge() always runs, but unlinkEntry can still rethrow a directory-
              // expiration failure past us. If we only set discharged after the call,
              // the outer finally would double-decrement totalBytes on that path.
              discharged = true;
              unlinkEntry(removedEntry);
            } finally {
              lruLock.unlock();
            }
          } else if (removedEntry != null) {
            // Defensive: a different Entry now sits under this key. Per the tryEvict-
            // owns-removal protocol this should not happen, but if a future code path
            // breaks the invariant we restore the impostor so its own discipline
            // continues and discharge e ourselves — without this, e's bytes leak
            // permanently into totalBytes and the resurrection branch in expireEntry
            // would later unlink e but never discharge.
            log.severe(
                format(
                    "nonexistent entry %s did not match last unreferenced entry," + " restoring it",
                    key));
            storage.put(key, removedEntry);
            lruLock.lock();
            try {
              unlinkReferenced(e);
              dischargeLocked(e.key, e.size);
            } finally {
              lruLock.unlock();
            }
            discharged = true;
          } else {
            // Defensive: per the protocol the tryEvict winner is the unique remover, so
            // storage.remove should always find e here. If it doesn't, e's bytes remain in
            // totalBytes and e is still on the LRU — discharge here so accounting does not
            // slow-leak under a crash-recovery or corruption scenario.
            log.severe(
                format(
                    "entry %s was already removed from storage before this evictor could"
                        + " remove it, discharging",
                    key));
            lruLock.lock();
            try {
              unlinkReferenced(e);
              dischargeLocked(e.key, e.size);
            } finally {
              lruLock.unlock();
            }
            discharged = true;
          }
          e.completeEviction();
          completed = true;
        } finally {
          if (!completed) {
            lruLock.lock();
            try {
              unlinkReferenced(e);
              if (!discharged) {
                dischargeLocked(e.key, e.size);
              }
            } finally {
              lruLock.unlock();
            }
            e.completeEviction();
          }
        }
      }
      return false;
    }

    int previous = e.tryAcquire();
    if (previous < 0) {
      return false;
    }
    if (previous == 0) {
      lruLock.lock();
      try {
        unlinkReferenced(e);
      } finally {
        lruLock.unlock();
      }
    }
    return true;
  }

  private CancellableOutputStream putOrReference(
      String key,
      DigestFunction.Value digestFunction,
      UUID writeId,
      Supplier<Boolean> writeWinner,
      long blobSizeInBytes,
      boolean isExecutable,
      Runnable onInsert,
      boolean isReset)
      throws IOException, InterruptedException {
    AtomicBoolean requiresDischarge = new AtomicBoolean(false);
    try {
      CancellableOutputStream out =
          putOrReferenceGuarded(
              key,
              digestFunction,
              writeId,
              writeWinner,
              blobSizeInBytes,
              isExecutable,
              onInsert,
              requiresDischarge,
              isReset);
      requiresDischarge.set(false); // stream now owns discharge
      return out;
    } finally {
      if (requiresDischarge.get()) {
        dischargeAndNotify("put:" + key, blobSizeInBytes);
      }
    }
  }

  protected void deleteExpiredKey(String key) throws IOException {
    Path path = getRemovingPath(key);
    long createdTimeMs = Files.getLastModifiedTime(path).to(MILLISECONDS);

    Files.delete(path);

    publishExpirationMetric(createdTimeMs);
  }

  private void publishExpirationMetric(long createdTimeMs) {
    // TODO introduce ttl clock
    long currentTimeMs = new Date().getTime();
    long ttlMs = currentTimeMs - createdTimeMs;
    casTtl.observe(Time.millisecondsToSeconds(ttlMs));
  }

  @SuppressWarnings({"ConstantConditions", "ResultOfMethodCallIgnored"})
  protected boolean charge(String key, long blobSizeInBytes, AtomicBoolean requiresDischarge)
      throws IOException, InterruptedException {
    boolean interrupted = false;
    Iterable<ListenableFuture<Digest>> expiredDigestsFutures;
    if (referenceIfExists(key)) {
      return false;
    }
    totalBytes.add(estimateSizeOnDisk(blobSizeInBytes, blockSize, /* isHardlink= */ false));
    requiresDischarge.set(true);
    lruLock.lock();
    try {
      ImmutableList.Builder<ListenableFuture<Digest>> builder = ImmutableList.builder();
      try {
        while (!interrupted && totalBytes.sum() > maxSizeInBytes) {
          ListenableFuture<Entry> expiredFuture = expireEntry(blobSizeInBytes, expireService);
          interrupted = Thread.interrupted();
          if (expiredFuture != null) {
            builder.add(
                transformAsync(
                    expiredFuture,
                    (expiredEntry) -> {
                      String expiredKey = expiredEntry.key;
                      try {
                        deleteExpiredKey(expiredKey);
                      } catch (NoSuchFileException eNoEnt) {
                        log.log(
                            Level.SEVERE,
                            format(
                                "CASFileCache::putImpl: expired key %s did not exist to delete",
                                expiredKey),
                            eNoEnt);
                      }
                      FileEntryKey fileEntryKey = parseFileEntryKey(expiredKey, expiredEntry.size);
                      if (fileEntryKey != null
                          && storage.containsKey(
                              getKey(fileEntryKey.digest(), !fileEntryKey.isExecutable()))) {
                        return immediateFuture(null);
                      }
                      expiredKeyCounter.inc();
                      return immediateFuture(fileEntryKey != null ? fileEntryKey.digest() : null);
                    },
                    expireService));
          }
        }
      } catch (InterruptedException e) {
        // clear interrupted flag
        Thread.interrupted();
        interrupted = true;
      }
      expiredDigestsFutures = builder.build();
    } finally {
      lruLock.unlock();
    }

    ImmutableSet.Builder<Digest> builder = ImmutableSet.builder();
    for (ListenableFuture<Digest> expiredDigestFuture : expiredDigestsFutures) {
      Digest digest = getOrIOException(expiredDigestFuture);
      if (Thread.interrupted()) {
        interrupted = true;
      }
      if (digest != null) {
        builder.add(digest);
      }
    }
    Set<Digest> expiredDigests = builder.build();
    if (!expiredDigests.isEmpty()) {
      onExpire.accept(expiredDigests);
    }
    if (interrupted || Thread.currentThread().isInterrupted()) {
      throw new InterruptedException();
    }
    return true;
  }

  private CancellableOutputStream putOrReferenceGuarded(
      String key,
      DigestFunction.Value digestFunction,
      UUID writeId,
      Supplier<Boolean> writeWinner,
      long blobSizeInBytes,
      boolean isExecutable,
      Runnable onInsert,
      AtomicBoolean requiresDischarge,
      boolean isReset)
      throws IOException, InterruptedException {
    if (blobSizeInBytes > maxEntrySizeInBytes) {
      throw new EntryLimitException(blobSizeInBytes, maxEntrySizeInBytes);
    }

    if (!charge(key, blobSizeInBytes, requiresDischarge)) {
      return DUPLICATE_OUTPUT_STREAM;
    }

    DigestUtil digestUtil = new DigestUtil(HashFunction.get(digestFunction));
    String writeKey = key + "." + writeId;
    Path writePath = getPath(key).resolveSibling(writeKey);
    final long committedSize;
    HashingOutputStream hashOut;
    if (!isReset && Files.exists(writePath)) {
      committedSize = Files.size(writePath);
      try (InputStream in = Files.newInputStream(writePath)) {
        // TODO this might not be completely safe - best to maybe avoid opening the
        // file for write before we're ready to write to it, could do it with a lazy
        // open
        SkipOutputStream skipStream =
            new SkipOutputStream(Files.newOutputStream(writePath, APPEND), committedSize);
        hashOut = digestUtil.newHashingOutputStream(skipStream);
        ByteStreams.copy(in, hashOut);
        in.close();
        checkState(skipStream.isSkipped());
      }
    } else {
      committedSize = 0;
      hashOut = digestUtil.newHashingOutputStream(Files.newOutputStream(writePath, CREATE));
    }
    Supplier<String> hashSupplier = () -> hashOut.hash().toString();
    CountingOutputStream countingOut = new CountingOutputStream(committedSize, hashOut);
    return new CancellableOutputStream(countingOut) {
      long written = committedSize;
      final Digest expectedDigest = keyToDigest(key, blobSizeInBytes, digestUtil);

      @Override
      public long getWritten() {
        return countingOut.written();
      }

      // must report a size that can be considered closeable
      @Override
      public long getWrittenForClose() {
        try {
          out.flush();
        } catch (IOException e) {
          // technically no harm no foul
        }
        return getWritten();
      }

      @Override
      public Path getPath() {
        return writePath;
      }

      @Override
      public void cancel() throws IOException {
        try {
          out.close();
          Files.delete(writePath);
        } finally {
          dischargeAndNotify(writeKey, blobSizeInBytes);
        }
      }

      @Override
      public void write(int b) throws IOException {
        if (getWritten() >= blobSizeInBytes) {
          throw new IOException(
              format("attempted overwrite at %d by 1 byte for %s", getWritten(), writeKey));
        }
        out.write(b);
      }

      @Override
      public void write(byte[] b) throws IOException {
        write(b, 0, b.length);
      }

      @Override
      public void write(byte[] b, int off, int len) throws IOException {
        long written = getWritten();
        if (written + len > blobSizeInBytes) {
          throw new IOException(
              format("attempted overwrite at %d by %d bytes for %s", written, len, writeKey));
        }
        out.write(b, off, len);
        if (getWritten() > blobSizeInBytes) {
          throw new IOException(
              format("overwrite at %d by %d bytes for %s", written, len, writeKey));
        }
      }

      @Override
      public void close() throws IOException {
        out.flush();
        long size = countingOut.written();
        // has some trouble with multiple closes, fortunately we have something above to handle this
        out.close(); // should probably discharge here as well

        if (size > blobSizeInBytes) {
          String hash = hashSupplier.get();
          try {
            Files.delete(writePath);
          } finally {
            dischargeAndNotify(writeKey, blobSizeInBytes);
          }
          Digest actual = digestUtil.build(hash, size);
          throw new DigestMismatchException(actual, expectedDigest);
        }

        if (size != blobSizeInBytes) {
          throw new IncompleteBlobException(writePath, key, size, blobSizeInBytes);
        }

        commit();
      }

      void commit() throws IOException {
        String hash = hashSupplier.get();
        String fileName = writePath.getFileName().toString();
        Digest actual = digestUtil.build(hash, countingOut.written());
        if (!fileName.equals(getKey(actual, isExecutable) + "." + writeId)) {
          dischargeAndNotify(writeKey, blobSizeInBytes);
          throw new DigestMismatchException(actual, expectedDigest);
        }
        try {
          setReadOnlyPerms(writePath, isExecutable, fileStore);
        } catch (IOException e) {
          dischargeAndNotify(writeKey, blobSizeInBytes);
          throw e;
        }

        Entry entry = new Entry(key, blobSizeInBytes, Deadline.after(10, SECONDS));

        Entry existingEntry = null;
        boolean inserted = false;
        try {
          // acquire the key lock
          Files.createLink(CASFileCache.this.getPath(key), writePath);
          existingEntry = safeStorageInsertion(key, entry);
          inserted = existingEntry == null;
        } catch (FileAlreadyExistsException e) {
          log.log(Level.FINER, "file already exists for " + key + ", nonexistent entry will fail");
        } finally {
          Files.delete(writePath);
          if (!inserted) {
            dischargeAndNotify(writeKey, blobSizeInBytes);
          }
        }

        int attempts = 10;
        if (!inserted) {
          while (existingEntry == null && attempts-- != 0) {
            existingEntry = storage.get(key);
            try {
              MILLISECONDS.sleep(10);
            } catch (InterruptedException intEx) {
              throw new IOException(intEx);
            }
          }

          if (existingEntry == null) {
            throw new IOException("existing entry did not appear for " + key);
          }
        }

        if (existingEntry != null) {
          log.log(Level.FINER, "lost the race to insert " + key);
          if (!referenceIfExists(key)) {
            // we would lose our accountability and have a presumed reference if we returned
            throw new IllegalStateException("storage conflict with existing key for " + key);
          }
        } else if (writeWinner.get()) {
          log.log(Level.FINER, "won the race to insert " + key);
          try {
            onInsert.run();
          } catch (RuntimeException e) {
            throw new IOException(e);
          }
        } else {
          log.log(Level.FINER, "did not win the race to insert " + key);
        }
      }
    };
  }

  @VisibleForTesting
  public static class Entry {
    /**
     * Lifecycle: LIVE -> EVICTING -> EVICTED with the one legal rollback EVICTING -> LIVE (when an
     * evictor's refcount-recheck observes a resurrecting acquire).
     */
    public enum State {
      LIVE,
      EVICTING,
      EVICTED
    }

    private static final VarHandle REFCOUNT;
    private static final VarHandle STATE;

    static {
      try {
        MethodHandles.Lookup lookup = MethodHandles.lookup();
        REFCOUNT = lookup.findVarHandle(Entry.class, "refCount", int.class);
        STATE = lookup.findVarHandle(Entry.class, "state", State.class);
      } catch (ReflectiveOperationException e) {
        throw new ExceptionInInitializerError(e);
      }
    }

    Entry before;
    Entry after;
    final String key;
    final long size;

    @SuppressWarnings("unused")
    volatile int refCount;

    @SuppressWarnings("unused")
    volatile State state;

    volatile Deadline existsDeadline;

    private Entry(String key, long size, Deadline existsDeadline, int initialRefCount) {
      this.key = key;
      this.size = size;
      this.refCount = initialRefCount;
      this.state = State.LIVE;
      this.existsDeadline = existsDeadline;
    }

    private Entry() {
      this(null, -1, null, -1);
    }

    /**
     * Runtime constructor: caller holds the initial reference. Use {@link #orphan} for startup-scan
     * entries that are not yet referenced.
     */
    public Entry(String key, long size, Deadline existsDeadline) {
      this(key, size, existsDeadline, 1);
    }

    /**
     * Produces an entry in the unreferenced state with {@code before/after = null} so {@link
     * #isLinked()} returns false until the caller publishes it via {@link
     * CASFileCache#linkUnreferenced} under {@code lruLock}. Self-linking here would race {@link
     * CASFileCache#unlinkReferenced}'s {@code isLinked()} check on a concurrent acquirer and
     * corrupt {@code unreferencedCount}.
     */
    public static Entry orphan(String key, long size, Deadline existsDeadline) {
      return new Entry(key, size, existsDeadline, 0);
    }

    public int refCount() {
      return (int) REFCOUNT.getVolatile(this);
    }

    public State state() {
      return (State) STATE.getVolatile(this);
    }

    /**
     * True for the no-arg-constructed dummy returned by {@link CASFileCache#getEntry} when the
     * cache is not running (STARTING / STOPPED) and the digest is not in {@code storage}. Real
     * entries never carry a negative refCount — {@link #release} throws on underflow rather than
     * publishing a transient negative.
     */
    public boolean isPlaceholder() {
      return refCount() < 0;
    }

    @VisibleForTesting
    public boolean isEvictable() {
      return state() == State.LIVE && refCount() == 0;
    }

    public boolean isLinked() {
      return before != null && after != null;
    }

    public void unlink() {
      before.after = after;
      after.before = before;
      before = null;
      after = null;
    }

    protected void addBefore(Entry existingEntry) {
      after = existingEntry;
      before = existingEntry.before;
      before.after = this;
      after.before = this;
    }

    /**
     * Atomically increments refCount when the entry is LIVE.
     *
     * @return the previous refcount on success (0 indicates the caller drove the 0 -> 1 revival and
     *     must unlink the entry from the LRU under lruLock); -1 when the entry is no longer LIVE.
     */
    public int tryAcquire() {
      if (((State) STATE.getVolatile(this)) != State.LIVE) {
        return -1;
      }
      int previous = (int) REFCOUNT.getAndAdd(this, 1);
      if (((State) STATE.getVolatile(this)) != State.LIVE) {
        REFCOUNT.getAndAdd(this, -1);
        return -1;
      }
      return previous;
    }

    /**
     * Atomically decrements refCount; throws IllegalStateException on underflow rather than
     * publishing a transient negative.
     *
     * @return true on the final 1 -> 0 transition (caller must append to LRU under lruLock).
     */
    public boolean release() {
      for (; ; ) {
        int current = (int) REFCOUNT.getVolatile(this);
        if (current <= 0) {
          throw new IllegalStateException("entry " + key + " release with refCount=" + current);
        }
        if (REFCOUNT.compareAndSet(this, current, current - 1)) {
          return current == 1;
        }
      }
    }

    /**
     * CAS state LIVE -> EVICTING and re-check refCount. Returns true if the caller now owns the
     * eviction; false otherwise (entry resurrected via a concurrent acquire — the state is rolled
     * back to LIVE in that case).
     */
    public boolean tryEvict() {
      if (!STATE.compareAndSet(this, State.LIVE, State.EVICTING)) {
        return false;
      }
      if ((int) REFCOUNT.getVolatile(this) != 0) {
        // CAS rather than setVolatile so a future EVICTING -> X transition added elsewhere
        // surfaces as a failed checkState rather than silently overwriting state.
        // checkState (not assert) so the invariant holds in production where -ea is off.
        checkState(
            STATE.compareAndSet(this, State.EVICTING, State.LIVE),
            "tryEvict rollback observed unexpected state for %s",
            key);
        return false;
      }
      return true;
    }

    /** Terminal transition EVICTING -> EVICTED after the file delete has succeeded. */
    public void completeEviction() {
      // Mirror tryEvict's rollback CAS: a caller that invokes completeEviction without
      // first owning EVICTING (e.g., on a LIVE entry) surfaces a checkState failure
      // rather than silently turning a LIVE entry into EVICTED. checkState (not assert)
      // so the invariant holds in production where -ea is off.
      checkState(
          STATE.compareAndSet(this, State.EVICTING, State.EVICTED),
          "completeEviction observed unexpected state for %s",
          key);
    }

    /**
     * Re-position an unreferenced entry at the LRU tail. Returns true when the caller must
     * compensate {@code unreferencedCount} (the entry was unlinked but not re-linked because an
     * off-lock {@code tryEvict} flipped state to EVICTING between the precondition check and the
     * {@code addBefore}); false in all other cases. See the caller in {@link
     * CASFileCache#recordAccess(Iterable)} for the compensating decrement.
     */
    public boolean recordAccess(Entry header) {
      // Only re-position entries that are currently LIVE, unreferenced, AND linked. An
      // entry can be unreferenced-but-not-yet-linked transiently (between release's CAS
      // and the LRU tail-append under lruLock) or unlinked because eviction is in
      // progress; both are benign — skip the access record. The state==LIVE check keeps
      // a queued recordAccess from re-positioning an entry the evictor has already
      // chosen as a victim (state==EVICTING), which would transiently expose an
      // EVICTING entry at the LRU tail.
      if (state() == State.LIVE && refCount() == 0 && isLinked()) {
        unlink();
        // Re-check state after the unlink: tryEvict is called off-lock from
        // newLocalInput's IOException recovery and referenceIfExists's stale-deadline
        // recovery, so a racing tryEvict can flip state to EVICTING between the
        // precondition above and the addBefore below. Without the recheck we would
        // re-publish an EVICTING entry on the LRU; the racing evictor's later
        // unlinkReferenced sees isLinked==true and would still produce correct
        // accounting in Phase 1, but the Phase 2 async evictor relies on "entries on
        // the LRU are LIVE". When we skip the addBefore, the caller decrements
        // unreferencedCount to compensate — the evictor's later unlinkReferenced will
        // see isLinked==false and skip its decrement, so without compensation the
        // counter drifts positive.
        if (state() == State.LIVE) {
          addBefore(header);
          return false;
        }
        return true;
      }
      return false;
    }
  }

  private static final class SentinelEntry extends Entry {
    @Override
    public void unlink() {
      throw new UnsupportedOperationException("sentinel cannot be unlinked");
    }

    @Override
    protected void addBefore(Entry existingEntry) {
      throw new UnsupportedOperationException("sentinel cannot be added");
    }

    @Override
    public int tryAcquire() {
      throw new UnsupportedOperationException("sentinel cannot be referenced");
    }

    @Override
    public boolean release() {
      throw new UnsupportedOperationException("sentinel cannot be referenced");
    }

    @Override
    public boolean tryEvict() {
      throw new UnsupportedOperationException("sentinel cannot be evicted");
    }

    @Override
    public void completeEviction() {
      throw new UnsupportedOperationException("sentinel cannot be evicted");
    }

    @Override
    public boolean isEvictable() {
      return false;
    }

    @Override
    public boolean isPlaceholder() {
      // SentinelEntry inherits the no-arg Entry() constructor's refCount=-1, which would
      // otherwise satisfy the parent predicate. The header sentinel is the LRU anchor, not
      // the STARTING/STOPPED dummy returned by getEntry(); a future change that exposes
      // the header through getEntry() must not have it pass isPlaceholder().
      return false;
    }

    @Override
    public boolean recordAccess(Entry header) {
      throw new UnsupportedOperationException("sentinel cannot be accessed");
    }
  }

  private InputStream newExternalInput(Compressor.Value compressor, Digest digest, long offset)
      throws IOException {
    return inputStreamFactory.newInput(compressor, digest, offset);
  }

  // CAS fallback methods

  private InputStream newInputFallback(Compressor.Value compressor, Digest digest, long offset)
      throws IOException {
    checkNotNull(delegate);

    if (digest.getSize() > maxEntrySizeInBytes) {
      return delegate.newInput(compressor, digest, offset);
    }
    Write write =
        getWrite(compressor, digest, UUID.randomUUID(), RequestMetadata.getDefaultInstance());
    return newReadThroughInput(compressor, digest, offset, write);
  }

  ReadThroughInputStream newReadThroughInput(
      Compressor.Value compressor, Digest digest, long offset, Write write) throws IOException {
    return new ReadThroughInputStream(
        delegate.newInput(compressor, digest, 0),
        localOffset -> newTransparentInput(compressor, digest, localOffset),
        digest.getSize(),
        offset,
        write);
  }

  private void expireEntryFallback(FileEntryKey fileEntryKey) {
    if (delegate != null) {
      Write write;
      try {
        write =
            delegate.getWrite(
                Compressor.Value.IDENTITY,
                fileEntryKey.digest(),
                UUID.randomUUID(),
                RequestMetadata.getDefaultInstance());
      } catch (EntryLimitException e) {
        log.log(
            Level.SEVERE,
            format("error opening delegate write for expired entry %s", fileEntryKey.key()),
            e);
        return;
      }
      if (write != null) {
        performCopy(write, fileEntryKey.digest(), fileEntryKey.key());
      }
    }
  }

  /**
   * Kicks off an asynchronous copy. Does not wait for it to complete. That way we don't do IO while
   * holding the lock.
   *
   * <p>Async upload failures (arriving on the gRPC SerializingExecutor after this thread has
   * returned) do NOT propagate interrupts back to the eviction thread. The eviction loop has
   * already moved on. This should be ok because interruption during the async phase is meaningful
   * only at graceful shutdown where higher-level shutdown logic terminates the worker
   * independently.
   *
   * <p>Synchronous setup failures DO restore the interrupt flag. The eviction loop is still on this
   * thread and its end-of-iteration {@code isInterrupted()} check needs to see the flag to
   * propagate {@link InterruptedException} up through {@code charge} to the {@code put} caller.
   */
  private void performCopy(Write write, Digest digest, String key) {
    // We need to synchronously open the InputStream for streamIntoWriteFuture because
    // safeStorageRemoval unlinks the file right after this function returns.
    ListenableFuture<Long> future =
        WritesHelper.streamIntoWriteFuture(
            () -> Files.newInputStream(getPath(key)), write, digest, 1, MINUTES);
    if (future.isDone()) {
      try {
        future.get();
      } catch (ExecutionException e) {
        if (e.getCause() instanceof IOException
            && causedByInterrupted((IOException) e.getCause())) {
          Thread.currentThread().interrupt();
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    Futures.addCallback(
        future,
        new FutureCallback<Long>() {
          @Override
          public void onSuccess(Long committedSize) {}

          @Override
          public void onFailure(Throwable t) {
            log.log(Level.SEVERE, format("error delegating expired entry %s", key), t);
          }
        },
        directExecutor());
  }
}
