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

import static build.buildfarm.common.Errors.VIOLATION_TYPE_MISSING;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import build.bazel.remote.execution.v2.Action;
import build.bazel.remote.execution.v2.Command;
import build.bazel.remote.execution.v2.DigestFunction;
import build.bazel.remote.execution.v2.Directory;
import build.bazel.remote.execution.v2.ExecuteResponse;
import build.bazel.remote.execution.v2.Platform;
import build.buildfarm.cas.cfc.CASFileCache;
import build.buildfarm.cas.cfc.PutDirectoryException;
import build.buildfarm.common.Claim;
import build.buildfarm.common.ExecutionProperties;
import build.buildfarm.v1test.Digest;
import build.buildfarm.v1test.ExecuteEntry;
import build.buildfarm.v1test.QueueEntry;
import build.buildfarm.v1test.QueuedOperation;
import build.buildfarm.v1test.WorkerExecutedMetadata;
import build.buildfarm.worker.ExecDirException.ViolationException;
import build.buildfarm.worker.persistent.FetchResult;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.longrunning.Operation;
import com.google.protobuf.Any;
import com.google.protobuf.Duration;
import com.google.rpc.Code;
import com.google.rpc.DebugInfo;
import com.google.rpc.Help;
import com.google.rpc.LocalizedMessage;
import com.google.rpc.PreconditionFailure;
import com.google.rpc.RequestInfo;
import com.google.rpc.ResourceInfo;
import com.google.rpc.Status;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.attribute.UserPrincipal;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class InputFetcherTest {
  private static Command persistentWorkerCommand() {
    return Command.newBuilder()
        .addArguments("/bin/worker")
        .setPlatform(
            Platform.newBuilder()
                .addProperties(
                    Platform.Property.newBuilder()
                        .setName(ExecutionProperties.PERSISTENT_WORKER_KEY)
                        .setValue("tool-hash")
                        .build())
                .build())
        .build();
  }

  private static ExecutionContext executionContext(Operation operation) {
    ExecuteEntry executeEntry =
        ExecuteEntry.newBuilder()
            .setOperationName(operation.getName())
            .setActionDigest(
                Digest.newBuilder().setDigestFunction(DigestFunction.Value.SHA256).build())
            .build();
    QueueEntry queueEntry = QueueEntry.newBuilder().setExecuteEntry(executeEntry).build();
    return ExecutionContext.newBuilder()
        .setQueueEntry(queueEntry)
        .setOperation(operation)
        .setPoller(new build.buildfarm.common.Poller(Duration.newBuilder().setSeconds(1).build()))
        .setClaim(mock(Claim.class))
        .build();
  }

  private static QueuedOperation persistentQueuedOperation(Command command) {
    return QueuedOperation.newBuilder()
        .setCommand(command)
        .setAction(Action.getDefaultInstance())
        .setTree(build.buildfarm.v1test.Tree.getDefaultInstance())
        .build();
  }

  private static FetchResult fetchResult(CASFileCache fileCache, String key) {
    return new FetchResult(
        ImmutableList.of(),
        ImmutableSet.of(),
        ImmutableList.of(key),
        ImmutableList.of(),
        DigestFunction.Value.SHA256,
        fileCache,
        ImmutableMap.of(),
        ImmutableSet.of());
  }

  @Test
  public void onlyMissingFilesIsViolationMissingFailedPrecondition() throws Exception {
    PipelineStage error = mock(PipelineStage.class);
    Operation operation = Operation.newBuilder().setName("missing-inputs").build();
    ExecuteEntry executeEntry =
        ExecuteEntry.newBuilder().setOperationName(operation.getName()).build();
    QueueEntry queueEntry = QueueEntry.newBuilder().setExecuteEntry(executeEntry).build();
    ExecutionContext executionContext =
        ExecutionContext.newBuilder()
            .setQueueEntry(queueEntry)
            .setOperation(operation)
            .setClaim(mock(Claim.class))
            .build();
    Command command = Command.newBuilder().addArguments("/bin/false").build();
    QueuedOperation queuedOperation = QueuedOperation.newBuilder().setCommand(command).build();
    AtomicReference<Operation> failedOperationRef = new AtomicReference<>();
    WorkerContext workerContext =
        new StubWorkerContext() {
          @Override
          public QueuedOperation getQueuedOperation(QueueEntry queueEntry) {
            return queuedOperation;
          }

          @Override
          public boolean putOperation(Operation operation) {
            if (operation.getDone()) {
              return failedOperationRef.compareAndSet(null, operation);
            }
            return true;
          }

          @Override
          public Path createExecDir(
              String operationName,
              Map<build.bazel.remote.execution.v2.Digest, Directory> directoriesIndex,
              DigestFunction.Value digestFunction,
              Action action,
              Command command,
              UserPrincipal owner,
              WorkerExecutedMetadata.Builder workerExecutedMetadata)
              throws IOException {
            Path root = Path.of(operationName);
            throw new ExecDirException(
                Path.of(operationName),
                ImmutableList.of(
                    new ViolationException(
                        Digest.getDefaultInstance(),
                        root.resolve("input"),
                        /* isExecutable= */ false,
                        new NoSuchFileException("input-digest")),
                    new PutDirectoryException(
                        root.resolve("dir"),
                        Digest.getDefaultInstance(),
                        ImmutableList.of(new NoSuchFileException("dir/input-digest")))));
          }

          @Override
          public int getInputFetchStageWidth() {
            return 1;
          }
        };
    InputFetchStage owner = new InputFetchStage(workerContext, /* output= */ null, error);
    Executor executor = mock(Executor.class);
    InputFetcher inputFetcher = new InputFetcher(workerContext, executionContext, owner, executor);
    inputFetcher.fetchPolled(/* stopwatch= */ null);
    Operation failedOperation = checkNotNull(failedOperationRef.get());
    verify(error, times(1)).put(any(ExecutionContext.class));
    ExecuteResponse executeResponse = failedOperation.getResponse().unpack(ExecuteResponse.class);
    Status status = executeResponse.getStatus();
    assertThat(status.getCode()).isEqualTo(Code.FAILED_PRECONDITION.getNumber());
    for (Any detail : status.getDetailsList()) {
      if (!(detail.is(DebugInfo.class)
          || detail.is(Help.class)
          || detail.is(LocalizedMessage.class)
          || detail.is(RequestInfo.class)
          || detail.is(ResourceInfo.class))) {
        assertThat(detail.is(PreconditionFailure.class)).isTrue();
        PreconditionFailure preconditionFailure = detail.unpack(PreconditionFailure.class);
        assertThat(preconditionFailure.getViolationsCount()).isGreaterThan(0);
        assertThat(
                Iterables.all(
                    preconditionFailure.getViolationsList(),
                    violation -> violation.getType().equals(VIOLATION_TYPE_MISSING)))
            .isTrue();
      }
    }
    verifyNoMoreInteractions(executor);
  }

  @Test
  public void fetchPolled_closesFetchResultWhenLightweightExecDirThrowsRuntime() throws Exception {
    PipelineStage error = mock(PipelineStage.class);
    Operation operation = Operation.newBuilder().setName("runtime-lightweight-failure").build();
    Command command = persistentWorkerCommand();
    QueuedOperation queuedOperation = persistentQueuedOperation(command);
    CASFileCache fileCache = mock(CASFileCache.class);
    FetchResult fetchResult = fetchResult(fileCache, "sha256_runtime_failure");

    WorkerContext workerContext =
        new StubWorkerContext() {
          @Override
          public QueuedOperation getQueuedOperation(QueueEntry queueEntry) {
            return queuedOperation;
          }

          @Override
          public boolean putOperation(Operation operation) {
            return true;
          }

          @Override
          public boolean isLinkExecFileSystem() {
            return true;
          }

          @Override
          public FetchResult fetchAndRefInputs(
              Map<build.bazel.remote.execution.v2.Digest, Directory> directoriesIndex,
              DigestFunction.Value digestFunction,
              Action action,
              Command command,
              java.util.Set<String> toolInputPaths,
              WorkerExecutedMetadata.Builder workerExecutedMetadata) {
            return fetchResult;
          }

          @Override
          public Path createLightweightExecDir(String operationName, Command command) {
            throw new IllegalArgumentException("bad output path");
          }

          @Override
          public int getInputFetchStageWidth() {
            return 1;
          }
        };

    InputFetcher inputFetcher =
        new InputFetcher(
            workerContext,
            executionContext(operation),
            new InputFetchStage(workerContext, /* output= */ null, error),
            mock(Executor.class));

    assertThrows(
        IllegalArgumentException.class, () -> inputFetcher.fetchPolled(Stopwatch.createStarted()));

    assertThat(fetchResult.isTerminal()).isTrue();
    verify(fileCache)
        .decrementReferences(
            ImmutableList.of("sha256_runtime_failure"),
            ImmutableList.of(),
            DigestFunction.Value.SHA256);
  }

  @Test
  public void fetchPolled_cleansFetchResultAndExecDirWhenExecuteClaimRejected() throws Exception {
    PipelineStage output = mock(PipelineStage.class);
    PipelineStage error = mock(PipelineStage.class);
    when(output.claim(any(ExecutionContext.class))).thenReturn(false);

    Operation operation = Operation.newBuilder().setName("claim-rejected").build();
    Command command = persistentWorkerCommand();
    QueuedOperation queuedOperation = persistentQueuedOperation(command);
    Path execDir = Path.of("/exec/claim-rejected");
    AtomicBoolean destroyed = new AtomicBoolean(false);
    CASFileCache fileCache = mock(CASFileCache.class);
    FetchResult fetchResult = fetchResult(fileCache, "sha256_claim_rejected");

    WorkerContext workerContext =
        new StubWorkerContext() {
          @Override
          public QueuedOperation getQueuedOperation(QueueEntry queueEntry) {
            return queuedOperation;
          }

          @Override
          public boolean putOperation(Operation operation) {
            return true;
          }

          @Override
          public void resumePoller(
              build.buildfarm.common.Poller poller,
              String name,
              QueueEntry queueEntry,
              build.bazel.remote.execution.v2.ExecutionStage.Value stage,
              Runnable onFailure,
              io.grpc.Deadline deadline,
              Executor executor) {}

          @Override
          public boolean isLinkExecFileSystem() {
            return true;
          }

          @Override
          public FetchResult fetchAndRefInputs(
              Map<build.bazel.remote.execution.v2.Digest, Directory> directoriesIndex,
              DigestFunction.Value digestFunction,
              Action action,
              Command command,
              java.util.Set<String> toolInputPaths,
              WorkerExecutedMetadata.Builder workerExecutedMetadata) {
            return fetchResult;
          }

          @Override
          public Path createLightweightExecDir(String operationName, Command command) {
            return execDir;
          }

          @Override
          public void destroyExecDir(Path path) {
            assertThat(path).isEqualTo(execDir);
            destroyed.set(true);
          }

          @Override
          public int getInputFetchStageWidth() {
            return 1;
          }
        };

    InputFetcher inputFetcher =
        new InputFetcher(
            workerContext,
            executionContext(operation),
            new InputFetchStage(workerContext, output, error),
            mock(Executor.class));

    inputFetcher.fetchPolled(Stopwatch.createStarted());

    assertThat(fetchResult.isTerminal()).isTrue();
    assertThat(destroyed.get()).isTrue();
    verify(error).put(any(ExecutionContext.class));
    verify(fileCache)
        .decrementReferences(
            ImmutableList.of("sha256_claim_rejected"),
            ImmutableList.of(),
            DigestFunction.Value.SHA256);
  }
}
