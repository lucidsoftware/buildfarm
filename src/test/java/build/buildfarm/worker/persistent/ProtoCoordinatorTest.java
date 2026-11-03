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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import build.bazel.remote.execution.v2.Command;
import build.buildfarm.v1test.Tree;
import build.buildfarm.worker.util.WorkerTestUtils;
import build.buildfarm.worker.util.WorkerTestUtils.TreeFile;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import com.google.devtools.build.lib.worker.WorkerProtocol.Input;
import com.google.devtools.build.lib.worker.WorkerProtocol.WorkRequest;
import com.google.devtools.build.lib.worker.WorkerProtocol.WorkResponse;
import com.google.protobuf.Duration;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.pool2.PooledObject;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import persistent.bazel.client.CommonsWorkerPool;
import persistent.bazel.client.PersistentWorker;
import persistent.bazel.client.WorkerKey;
import persistent.bazel.client.WorkerResources;
import persistent.bazel.client.WorkerSupervisor;
import persistent.common.PoolExhaustedException;

@RunWith(JUnit4.class)
public class ProtoCoordinatorTest {
  private final List<ProtoCoordinator> coordinators = new ArrayList<>();

  private ProtoCoordinator newCoordinator() {
    ProtoCoordinator protoCoordinator = ProtoCoordinator.ofCommonsPool(4);
    coordinators.add(protoCoordinator);
    return protoCoordinator;
  }

  @After
  public void shutdownSchedulers() {
    for (ProtoCoordinator protoCoordinator : coordinators) {
      protoCoordinator.close();
    }
    coordinators.clear();
  }

  private WorkerKey makeWorkerKey(
      WorkFilesContext ctx, WorkerInputs workerFiles, Path workRootsDir) {
    return Keymaker.make(
        ctx.opRoot,
        workRootsDir,
        ImmutableList.of("workerExecCmd"),
        ImmutableList.of("workerInitArgs"),
        ImmutableMap.of(),
        "executionName",
        workerFiles);
  }

  private Path rootDir = null;

  public Path jimFsRoot() {
    if (rootDir == null) {
      rootDir =
          Iterables.getFirst(
              Jimfs.newFileSystem(
                      Configuration.unix().toBuilder()
                          .setAttributeViews("basic", "owner", "posix", "unix")
                          .build())
                  .getRootDirectories(),
              null);
    }
    return rootDir;
  }

  @Test(timeout = 5000)
  public void exhaustedPoolTimesOutAndCanReuseReturnedWorker() throws Exception {
    PersistentWorker worker = mock(PersistentWorker.class);
    when(worker.getExitValue()).thenReturn(Optional.empty());
    WorkerSupervisor supervisor =
        new WorkerSupervisor() {
          public PersistentWorker create(WorkerKey key) {
            return worker;
          }
        };
    try (CommonsWorkerPool pool = new CommonsWorkerPool(supervisor, 1)) {
      WorkerKey key = mock(WorkerKey.class);
      assertThat(pool.obtain(key, java.time.Duration.ZERO)).isSameInstanceAs(worker);
      assertThrows(
          PoolExhaustedException.class, () -> pool.obtain(key, java.time.Duration.ofMillis(10)));
      assertThat(pool.getNumActive()).isEqualTo(1);
      pool.release(key, worker);
      assertThat(pool.obtain(key, java.time.Duration.ZERO)).isSameInstanceAs(worker);
      pool.release(key, worker);
    }
  }

  @Test(timeout = 5000)
  public void globalPoolLimitBoundsWaitForDifferentKey() throws Exception {
    PersistentWorker worker = mock(PersistentWorker.class);
    when(worker.getExitValue()).thenReturn(Optional.empty());
    WorkerSupervisor supervisor =
        new WorkerSupervisor() {
          public PersistentWorker create(WorkerKey key) {
            return worker;
          }
        };
    try (CommonsWorkerPool pool = new CommonsWorkerPool(supervisor, 1)) {
      pool.setMaxTotal(1);
      WorkerKey first = mock(WorkerKey.class);
      assertThat(pool.obtain(first, java.time.Duration.ZERO)).isSameInstanceAs(worker);
      assertThrows(
          PoolExhaustedException.class,
          () -> pool.obtain(mock(WorkerKey.class), java.time.Duration.ZERO));
      pool.release(first, worker);
    }
  }

  @Test
  public void failedValidationIsNotPoolExhaustion() throws Exception {
    PersistentWorker worker = mock(PersistentWorker.class);
    WorkerSupervisor supervisor =
        new WorkerSupervisor() {
          public PersistentWorker create(WorkerKey key) {
            return worker;
          }

          public boolean validateObject(WorkerKey key, PooledObject<PersistentWorker> value) {
            return false;
          }
        };
    try (CommonsWorkerPool pool = new CommonsWorkerPool(supervisor, 1)) {
      IOException error =
          assertThrows(
              IOException.class, () -> pool.obtain(mock(WorkerKey.class), java.time.Duration.ZERO));
      assertThat(error).isNotInstanceOf(PoolExhaustedException.class);
    }
  }

  @Test
  public void failedLaunchIsNotPoolExhaustion() throws Exception {
    WorkerSupervisor supervisor =
        new WorkerSupervisor() {
          public PersistentWorker create(WorkerKey key) throws IOException {
            throw new IOException("launch failed");
          }
        };
    try (CommonsWorkerPool pool = new CommonsWorkerPool(supervisor, 1)) {
      IOException error =
          assertThrows(
              IOException.class, () -> pool.obtain(mock(WorkerKey.class), java.time.Duration.ZERO));
      assertThat(error).isNotInstanceOf(PoolExhaustedException.class);
    }
  }

  @Test
  public void testProtoCoordinator() throws Exception {
    ProtoCoordinator pc = ProtoCoordinator.ofCommonsPool(4);

    Path fsRoot = jimFsRoot();
    Path opRoot = fsRoot.resolve("opRoot");
    assertThat(Files.notExists(opRoot)).isTrue();
    Files.createDirectory(opRoot);

    assertThat(Files.exists(opRoot)).isTrue();

    String treeRootDir = opRoot.toString();
    List<TreeFile> fileInputs =
        ImmutableList.of(
            new TreeFile("file_1", "file contents 1"),
            new TreeFile("subdir/subdir_file_2", "file contents 2"),
            new TreeFile("tools_dir/tool_file", "tool file contents", true),
            new TreeFile("tools_dir/tool_file_2", "tool file contents 2", true));

    Tree tree = WorkerTestUtils.makeTree(treeRootDir, fileInputs);

    Command command = WorkerTestUtils.makeCommand();
    WorkFilesContext ctx = WorkFilesContext.fromContext(opRoot, tree, command);
    ImmutableList<String> requestArgs = ImmutableList.of("reqArg1");

    WorkerInputs workerFiles = WorkerInputs.from(ctx, requestArgs);

    for (Path file : workerFiles.allInputs.keySet()) {
      Files.createDirectories(file.getParent());
      Files.createFile(file);
    }

    WorkerKey key = makeWorkerKey(ctx, workerFiles, fsRoot.resolve("workRootsDir"));

    Path workRoot = key.getExecRoot();
    Path toolsRoot = key.getToolRoot();

    // Assert: all Tools are copied into "/workRootsDir/*/<tool_inputs_hash>"
    assertThat(toolsRoot.toString()).startsWith(workRoot.toString());
    assertThat(toolsRoot.toString()).endsWith(key.getWorkerFilesCombinedHash().toString());
    pc.copyToolInputsIntoWorkerToolRoot(key, workerFiles);

    assertThat(Files.exists(workRoot)).isTrue();
    assertThat(Files.exists(toolsRoot)).isTrue();
    Set<Path> expectedToolInputs = new HashSet<>();
    for (TreeFile file : fileInputs) {
      if (file.isTool) {
        expectedToolInputs.add(toolsRoot.resolve(file.path));
      }
    }
    assertThat(WorkerTestUtils.listFilesRec(workRoot))
        .containsExactlyElementsIn(expectedToolInputs);

    List<Path> expectedOpRootFiles = new ArrayList<>();
    // Create some fake output files.
    for (String pathStr : ctx.outputFiles) {
      Path file = workRoot.resolve(pathStr);
      Files.createDirectories(file.getParent());
      Files.createFile(file);
      expectedOpRootFiles.add(opRoot.resolve(pathStr));
    }
    pc.moveOutputsToOperationRoot(ctx, workRoot);

    assertThat(WorkerTestUtils.listFilesRec(opRoot)).containsAtLeastElementsIn(expectedOpRootFiles);
    // At this point, the only thing left in the `workRoot` should be the tools.
    List<Path> workRootPaths = WorkerTestUtils.listFilesRec(workRoot);
    assertThat(workRootPaths).containsAtLeastElementsIn(expectedToolInputs);
    assertThat(workRootPaths).containsNoneIn(expectedOpRootFiles);
  }

  @Test
  public void moveOutputsToOperationRoot_handlesOutputPaths() throws Exception {
    Path fsRoot = jimFsRoot();
    Path opRoot = fsRoot.resolve("opRoot_outputPaths");
    Files.createDirectory(opRoot);
    Path workerExecRoot = fsRoot.resolve("workerExecRoot");
    Files.createDirectory(workerExecRoot);

    // Command with ONLY output_paths (REAPI >= 2.1 style)
    ImmutableList<String> outputPaths = ImmutableList.of("output_file", "out_subdir/out_subfile");
    Command command = Command.newBuilder().addAllOutputPaths(outputPaths).build();

    Tree tree =
        WorkerTestUtils.makeTree(
            opRoot.toString(), ImmutableList.of(new TreeFile("dummy", "content")));
    WorkFilesContext workFilesContext = WorkFilesContext.fromContext(opRoot, tree, command);

    // Verify precondition: output_paths set, output_files/output_directories empty
    assertThat(workFilesContext.outputPaths).isNotEmpty();
    assertThat(workFilesContext.outputFiles).isEmpty();
    assertThat(workFilesContext.outputDirectories).isEmpty();

    for (String relOutput : outputPaths) {
      Path execFile = workerExecRoot.resolve(relOutput);
      Files.createDirectories(execFile.getParent());
      Files.write(execFile, "output content".getBytes());
    }

    ProtoCoordinator protoCoordinator = ProtoCoordinator.ofCommonsPool(4);
    protoCoordinator.moveOutputsToOperationRoot(workFilesContext, workerExecRoot);

    for (String relOutput : outputPaths) {
      assertThat(Files.exists(opRoot.resolve(relOutput))).isTrue();
      assertThat(Files.exists(workerExecRoot.resolve(relOutput))).isFalse();
    }
  }

  @Test
  public void moveOutputsToOperationRoot_movesOutputPathDirectoryContents() throws Exception {
    Path fsRoot = jimFsRoot();
    Path opRoot = fsRoot.resolve("opRoot_outputPathDirs");
    Files.createDirectory(opRoot);
    Path workerExecRoot = fsRoot.resolve("workerExecRoot_dirs");
    Files.createDirectory(workerExecRoot);

    // output_paths with both a file and a directory (tree artifact) entry
    ImmutableList<String> outputPaths = ImmutableList.of("output.jar", "output_dir");
    Command command = Command.newBuilder().addAllOutputPaths(outputPaths).build();

    Tree tree =
        WorkerTestUtils.makeTree(
            opRoot.toString(), ImmutableList.of(new TreeFile("dummy", "content")));
    WorkFilesContext workFilesContext = WorkFilesContext.fromContext(opRoot, tree, command);

    Files.write(workerExecRoot.resolve("output.jar"), "jar content".getBytes());
    Path outputDir = workerExecRoot.resolve("output_dir");
    Files.createDirectories(outputDir.resolve("subdir"));
    Files.write(outputDir.resolve("file1.txt"), "file1".getBytes());
    Files.write(outputDir.resolve("subdir/file2.txt"), "file2".getBytes());

    ProtoCoordinator protoCoordinator = ProtoCoordinator.ofCommonsPool(4);
    protoCoordinator.moveOutputsToOperationRoot(workFilesContext, workerExecRoot);

    assertThat(Files.exists(opRoot.resolve("output.jar"))).isTrue();
    assertThat(Files.exists(workerExecRoot.resolve("output.jar"))).isFalse();

    assertThat(Files.isDirectory(opRoot.resolve("output_dir"))).isTrue();
    assertThat(Files.exists(opRoot.resolve("output_dir/file1.txt"))).isTrue();
    assertThat(Files.exists(opRoot.resolve("output_dir/subdir/file2.txt"))).isTrue();
    assertThat(Files.exists(workerExecRoot.resolve("output_dir"))).isFalse();
  }

  @Test
  public void moveOutputsToOperationRoot_movesOutputDirectoryContents() throws Exception {
    Path fsRoot = jimFsRoot();
    Path opRoot = fsRoot.resolve("opRoot_outputDirs");
    Files.createDirectory(opRoot);
    Path workerExecRoot = fsRoot.resolve("workerExecRoot_outputDirs");
    Files.createDirectory(workerExecRoot);

    // Legacy command with output_directories (pre-REAPI 2.1)
    Command command =
        Command.newBuilder()
            .addOutputFiles("output_file")
            .addOutputDirectories("output_dir")
            .build();

    Tree tree =
        WorkerTestUtils.makeTree(
            opRoot.toString(), ImmutableList.of(new TreeFile("dummy", "content")));
    WorkFilesContext workFilesContext = WorkFilesContext.fromContext(opRoot, tree, command);

    Files.write(workerExecRoot.resolve("output_file"), "file content".getBytes());
    Path outputDir = workerExecRoot.resolve("output_dir");
    Files.createDirectories(outputDir.resolve("subdir"));
    Files.write(outputDir.resolve("file1.txt"), "file1".getBytes());
    Files.write(outputDir.resolve("subdir/file2.txt"), "file2".getBytes());

    ProtoCoordinator protoCoordinator = ProtoCoordinator.ofCommonsPool(4);
    protoCoordinator.moveOutputsToOperationRoot(workFilesContext, workerExecRoot);

    assertThat(Files.exists(opRoot.resolve("output_file"))).isTrue();

    assertThat(Files.isDirectory(opRoot.resolve("output_dir"))).isTrue();
    assertThat(Files.exists(opRoot.resolve("output_dir/file1.txt"))).isTrue();
    assertThat(Files.exists(opRoot.resolve("output_dir/subdir/file2.txt"))).isTrue();
  }
  /**
   * Create a request that is NOT in pendingReqs in order to cause a null to be encountered in the
   * handler's run() function
   */
  private RequestCtx createRequestDontAddToPendingRequests() {
    return new RequestCtx(
        WorkRequest.getDefaultInstance(), null, null, Duration.newBuilder().setSeconds(10).build());
  }

  @Test
  public void runTimeoutHandler_requestAlreadyRemoved_doesNotThrow() throws Exception {
    ProtoCoordinator protoCoordinator = newCoordinator();

    RequestCtx request = createRequestDontAddToPendingRequests();

    // Make sure the handler's run() function doesn't throw an exception
    Runnable task = protoCoordinator.new RequestTimeoutHandler(request);
    task.run();
  }

  @Test
  public void timeoutScheduler_afterRequestAlreadyRemoved_keepsScheduling() throws Exception {
    ProtoCoordinator protoCoordinator = newCoordinator();

    RequestCtx request = createRequestDontAddToPendingRequests();

    Runnable task = protoCoordinator.new RequestTimeoutHandler(request);

    // Schedule the timeout handler to fire immediately. We want to be sure that the scheduler
    // survives after encountering a null inside of run().
    ScheduledFuture<?> future =
        protoCoordinator.timeoutScheduler.schedule(task, 0, TimeUnit.MILLISECONDS);
    future.get(2, TimeUnit.SECONDS);

    // Verify the scheduler is still alive by scheduling a second task.
    CountDownLatch latch = new CountDownLatch(1);
    protoCoordinator.timeoutScheduler.schedule(latch::countDown, 0, TimeUnit.MILLISECONDS);
    assertThat(latch.await(2, TimeUnit.SECONDS)).isTrue();
  }

  @Test
  public void staleTimeoutCannotKillWorkerLeasedToNewRequest() throws Exception {
    CommonsWorkerPool workerPool = mock(CommonsWorkerPool.class);
    ProtoCoordinator protoCoordinator = new ProtoCoordinator(workerPool);
    coordinators.add(protoCoordinator);
    PersistentWorker worker = mock(PersistentWorker.class);
    protoCoordinator.lifecycle.register(worker);
    PersistentWorkerLifecycle.Lease first = protoCoordinator.lifecycle.lease(worker, "operation-1");
    assertThat(protoCoordinator.lifecycle.release(first)).isTrue();
    PersistentWorkerLifecycle.Lease second =
        protoCoordinator.lifecycle.lease(worker, "operation-2");
    RequestCtx request = createRequestDontAddToPendingRequests();

    protoCoordinator.onTimeout(request, first);

    verify(workerPool, never())
        .invalidateObject(org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any());
    PersistentWorkerLifecycle.Snapshot snapshot =
        protoCoordinator.lifecycle.snapshot(worker).orElseThrow();
    assertThat(snapshot.state()).isEqualTo(PersistentWorkerLifecycle.State.LEASED);
    assertThat(snapshot.generation()).isEqualTo(second.generation());
    assertThat(request.timedOut()).isFalse();
  }

  @Test
  public void closeShutsDownPoolAndTimeoutScheduler() {
    CommonsWorkerPool workerPool = mock(CommonsWorkerPool.class);
    ProtoCoordinator protoCoordinator = new ProtoCoordinator(workerPool);

    protoCoordinator.close();

    verify(workerPool).close();
    assertThat(protoCoordinator.timeoutScheduler.isShutdown()).isTrue();
  }

  @Test
  public void preWorkInit_cleansUpPendingReqsOnCopyFailure() throws Exception {
    ProtoCoordinator protoCoordinator = newCoordinator();

    Path fsRoot = jimFsRoot();
    Path opRoot = fsRoot.resolve("opRoot_leakTest");
    Files.createDirectory(opRoot);

    // Create a worker inputs with a non-tool input that doesn't exist on disk.
    // copyNontoolInputs will try to copy it and throw an IOException.
    Path nonExistentInput = opRoot.resolve("non_existent_input");
    Input input = Input.newBuilder().setPath("non_existent_input").build();
    WorkerInputs workerInputs =
        new WorkerInputs(
            opRoot, ImmutableSet.of(), ImmutableSet.of(), ImmutableMap.of(nonExistentInput, input));

    RequestCtx request =
        new RequestCtx(
            WorkRequest.getDefaultInstance(),
            null,
            workerInputs,
            Duration.newBuilder().setSeconds(10).build());

    PersistentWorker mockWorker = mock(PersistentWorker.class);
    when(mockWorker.getExecRoot()).thenReturn(fsRoot.resolve("workerExecRoot"));

    // Make sure that we actually caused the error we wanted to.
    protoCoordinator.lifecycle.register(mockWorker);
    PersistentWorkerLifecycle.Lease lease =
        protoCoordinator.lifecycle.lease(mockWorker, "copy-failure");
    assertThrows(
        IOException.class, () -> protoCoordinator.preWorkInit(null, request, mockWorker, lease));

    // Make sure that, despite the error, the request is cleaned up from the map of pending
    // requests
    assertThat(protoCoordinator.hasPendingRequest(request)).isFalse();
  }
  @Test
  public void postWorkCleanup_movesOutputsOnNonZeroExitCode() throws Exception {
    Path fsRoot = jimFsRoot();
    Path opRoot = fsRoot.resolve("opRoot_nonzeroExitCodeOutputs");
    Files.createDirectory(opRoot);
    Path workerExecRoot = fsRoot.resolve("workerExecRoot_nonzeroExitCodeOutputs");
    Files.createDirectory(workerExecRoot);

    // Set up a command with declared output files
    Command command = Command.newBuilder().addOutputFiles("output_file").build();
    Tree tree =
        WorkerTestUtils.makeTree(
            opRoot.toString(), ImmutableList.of(new TreeFile("dummy", "content")));
    WorkFilesContext workFilesContext = WorkFilesContext.fromContext(opRoot, tree, command);

    // Create the output file in the worker exec root (as if the action produced it)
    Files.write(workerExecRoot.resolve("output_file"), "output content".getBytes());

    // Create WorkerInputs with no non-tool inputs (so cleanUpNontoolInputs is a no-op)
    WorkerInputs workerInputs =
        new WorkerInputs(opRoot, ImmutableSet.of(), ImmutableSet.of(), ImmutableMap.of());

    RequestCtx request =
        new RequestCtx(
            WorkRequest.getDefaultInstance(),
            workFilesContext,
            workerInputs,
            Duration.newBuilder().setSeconds(10).build());

    PersistentWorker mockWorker = mock(PersistentWorker.class);
    when(mockWorker.getExecRoot()).thenReturn(workerExecRoot);
    when(mockWorker.flushStdErr()).thenReturn("");

    // Return a non-zero exit code for the action
    WorkResponse response = WorkResponse.newBuilder().setExitCode(1).build();

    ProtoCoordinator protoCoordinator = newCoordinator();
    protoCoordinator.postWorkCleanup(response, mockWorker, request);

    // Outputs should be moved to the operation root even on non-zero exit code
    assertThat(Files.exists(opRoot.resolve("output_file"))).isTrue();
    assertThat(Files.exists(workerExecRoot.resolve("output_file"))).isFalse();
  }

  @Test
  public void postWorkCleanup_cleansNontoolInputsOnNonZeroExitCode() throws Exception {
    Path fsRoot = jimFsRoot();
    Path opRoot = fsRoot.resolve("opRoot_cleanupInputsOnNonzeroExitCode");
    Files.createDirectory(opRoot);
    Path workerExecRoot = fsRoot.resolve("workerExecRoot_cleanupInputsOnNonzeroExitCode");
    Files.createDirectory(workerExecRoot);

    // Create a non-tool input file in the operation root
    Path inputFileInOpRoot = opRoot.resolve("input_file.txt");
    Files.write(inputFileInOpRoot, "input content".getBytes());
    Input input = Input.newBuilder().setPath(inputFileInOpRoot.toString()).build();

    // Copy it to worker exec root (simulating what preWorkInit does)
    Path inputFileInWorkerExecRoot = workerExecRoot.resolve("input_file.txt");
    Files.write(inputFileInWorkerExecRoot, "input content".getBytes());

    // Create WorkerInputs with one non-tool input
    WorkerInputs workerInputs =
        new WorkerInputs(
            opRoot,
            ImmutableSet.of(),
            ImmutableSet.of(),
            ImmutableMap.of(inputFileInOpRoot, input));

    // Create a Command with no outputs (so moveOutputsToOperationRoot is a no-op)
    Command command = Command.newBuilder().build();
    Tree tree =
        WorkerTestUtils.makeTree(
            opRoot.toString(), ImmutableList.of(new TreeFile("input_file.txt", "input content")));
    WorkFilesContext workFilesContext = WorkFilesContext.fromContext(opRoot, tree, command);

    RequestCtx request =
        new RequestCtx(
            WorkRequest.getDefaultInstance(),
            workFilesContext,
            workerInputs,
            Duration.newBuilder().setSeconds(10).build());

    PersistentWorker mockWorker = mock(PersistentWorker.class);
    when(mockWorker.getExecRoot()).thenReturn(workerExecRoot);
    when(mockWorker.flushStdErr()).thenReturn("");

    // Return a non-zero exit code for the action
    WorkResponse response = WorkResponse.newBuilder().setExitCode(1).build();

    ProtoCoordinator protoCoordinator = newCoordinator();
    protoCoordinator.postWorkCleanup(response, mockWorker, request);

    // Non-tool inputs should be cleaned even on non-zero exit code
    assertThat(Files.exists(inputFileInWorkerExecRoot)).isFalse();
    // Original file in the operation root should be untouched
    assertThat(Files.exists(inputFileInOpRoot)).isTrue();
  }

  @Test
  public void postWorkCleanup_movesOutputsAndCleansInputsOnZeroExitCode() throws Exception {
    Path fsRoot = jimFsRoot();
    Path opRoot = fsRoot.resolve("opRoot_zeroExitCode");
    Files.createDirectory(opRoot);
    Path workerExecRoot = fsRoot.resolve("workerExecRoot_zeroExitCode");
    Files.createDirectory(workerExecRoot);

    // Create a non-tool input file in the operation root
    Path inputFileInOpRoot = opRoot.resolve("input_file.txt");
    Files.write(inputFileInOpRoot, "input content".getBytes());
    Input input = Input.newBuilder().setPath(inputFileInOpRoot.toString()).build();

    // Copy it to worker exec root (simulating what preWorkInit does)
    Path inputFileInWorkerExecRoot = workerExecRoot.resolve("input_file.txt");
    Files.write(inputFileInWorkerExecRoot, "input content".getBytes());

    // Set up a command with a declared output file
    Command command = Command.newBuilder().addOutputFiles("output_file").build();
    Tree tree =
        WorkerTestUtils.makeTree(
            opRoot.toString(), ImmutableList.of(new TreeFile("input_file.txt", "input content")));
    WorkFilesContext workFilesContext = WorkFilesContext.fromContext(opRoot, tree, command);

    // Create the output file in the worker exec root (as if the action produced it)
    Files.write(workerExecRoot.resolve("output_file"), "output content".getBytes());

    WorkerInputs workerInputs =
        new WorkerInputs(
            opRoot,
            ImmutableSet.of(),
            ImmutableSet.of(),
            ImmutableMap.of(inputFileInOpRoot, input));

    RequestCtx request =
        new RequestCtx(
            WorkRequest.getDefaultInstance(),
            workFilesContext,
            workerInputs,
            Duration.newBuilder().setSeconds(10).build());

    PersistentWorker mockWorker = mock(PersistentWorker.class);
    when(mockWorker.getExecRoot()).thenReturn(workerExecRoot);
    when(mockWorker.flushStdErr()).thenReturn("");

    // Exit code 0 as if the operation succeeded
    WorkResponse response = WorkResponse.newBuilder().setExitCode(0).build();

    ProtoCoordinator protoCoordinator = newCoordinator();
    protoCoordinator.postWorkCleanup(response, mockWorker, request);

    // Outputs should be moved to the operation root
    assertThat(Files.exists(opRoot.resolve("output_file"))).isTrue();
    assertThat(Files.exists(workerExecRoot.resolve("output_file"))).isFalse();

    // Non-tool inputs should be cleaned from the worker exec root
    assertThat(Files.exists(inputFileInWorkerExecRoot)).isFalse();

    // Original file in the operation root should be untouched
    assertThat(Files.exists(inputFileInOpRoot)).isTrue();
  }

  @Test
  public void sequentialOperationsQuiesceBeforeReturningTheSameWorker() throws Exception {
    CommonsWorkerPool pool = mock(CommonsWorkerPool.class);
    ProtoCoordinator coordinator = new ProtoCoordinator(pool);
    coordinators.add(coordinator);
    WorkerKey key = mock(WorkerKey.class);
    PersistentWorker worker = mock(PersistentWorker.class);
    Path root = Files.createTempDirectory("pw-leased-resources-");
    AtomicBoolean idle = new AtomicBoolean(true);
    WorkerResources resources =
        new WorkerResources() {
          public void resume() {
            assertThat(idle.getAndSet(false)).isTrue();
          }

          public void idle() {
            assertThat(idle.getAndSet(true)).isFalse();
          }
        };
    when(worker.getResources()).thenReturn(resources);
    when(worker.getExecRoot()).thenReturn(root);
    when(worker.flushStdErr()).thenReturn("");
    when(pool.obtain(org.mockito.ArgumentMatchers.eq(key), any(java.time.Duration.class)))
        .thenReturn(worker);
    when(worker.doWork(any()))
        .thenAnswer(
            invocation -> {
              assertThat(idle.get()).isFalse();
              return WorkResponse.newBuilder().setOutput("done").build();
            });
    doAnswer(
            invocation -> {
              assertThat(idle.get()).isTrue();
              return null;
            })
        .when(pool)
        .release(key, worker);
    coordinator.lifecycle.register(worker);
    for (String operation : List.of("operation-a", "operation-b")) {
      Path opRoot = root.resolve(operation);
      WorkFilesContext files =
          new WorkFilesContext(
              opRoot,
              Tree.getDefaultInstance(),
              ImmutableList.of(),
              ImmutableList.of(),
              ImmutableList.of());
      WorkerInputs inputs =
          new WorkerInputs(opRoot, ImmutableSet.of(), ImmutableSet.of(), ImmutableMap.of());
      RequestCtx request =
          new RequestCtx(
              WorkRequest.getDefaultInstance(),
              files,
              inputs,
              Duration.newBuilder().setSeconds(30).build());
      request.execution =
          (leasedResources, work) -> {
            leasedResources.resume();
            return work.call();
          };
      assertThat(coordinator.runRequest(key, request).response.getOutput()).isEqualTo("done");
      assertThat(coordinator.hasPendingRequest(request)).isFalse();
    }
    verify(pool, times(2)).release(key, worker);
    verify(worker, times(2)).doWork(any());
    verify(pool, never()).invalidate(key, worker);
    Files.delete(root);
  }
}
