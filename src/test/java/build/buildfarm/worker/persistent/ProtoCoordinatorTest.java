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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import build.bazel.remote.execution.v2.ActionResult;
import build.bazel.remote.execution.v2.Command;
import build.bazel.remote.execution.v2.DigestFunction;
import build.buildfarm.cas.cfc.CASFileCache;
import build.buildfarm.v1test.Tree;
import build.buildfarm.worker.resources.ResourceLimits;
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
import com.google.rpc.Code;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import persistent.bazel.client.PersistentWorker;
import persistent.bazel.client.WorkerKey;

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
      protoCoordinator.timeoutScheduler.shutdownNow();
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
    pc.copyToolInputsIntoWorkerToolRoot(key, workerFiles, /* fetchResult= */ null);

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

  /**
   * Reproduces the production bug: when the opRoot is a lightweight exec dir (output stubs only, no
   * input files), copyToolInputsIntoWorkerToolRoot fails because it tries to copy tool input files
   * from the opRoot where they don't exist.
   *
   * <p>This is the exact failure mode seen in production when linkInputs=true and persistent
   * workers use the FetchResult path (commit c4e287b8).
   */
  @Test
  public void copyToolInputsIntoWorkerToolRoot_failsWhenOpRootHasNoInputFiles() throws Exception {
    ProtoCoordinator pc = newCoordinator();

    Path fsRoot = jimFsRoot();
    Path opRoot = fsRoot.resolve("opRoot_lightweight");
    Files.createDirectory(opRoot);

    List<TreeFile> fileInputs =
        ImmutableList.of(
            new TreeFile("file_1", "file contents 1"),
            new TreeFile("tools_dir/tool_file", "tool file contents", true),
            new TreeFile("tools_dir/tool_file_2", "tool file contents 2", true));

    Tree tree = WorkerTestUtils.makeTree(opRoot.toString(), fileInputs);
    Command command = WorkerTestUtils.makeCommand();
    WorkFilesContext ctx = WorkFilesContext.fromContext(opRoot, tree, command);
    WorkerInputs workerFiles = WorkerInputs.from(ctx, ImmutableList.of("reqArg1"));

    // Intentionally do NOT create files at opRoot — simulating the lightweight exec dir
    // that only has output stubs (no input files on disk).

    WorkerKey key = makeWorkerKey(ctx, workerFiles, fsRoot.resolve("workRootsDir_lightweight"));

    // This should fail with "source file doesn't exist" — the exact error from production logs.
    IOException thrown =
        assertThrows(
            IOException.class,
            () -> pc.copyToolInputsIntoWorkerToolRoot(key, workerFiles, /* fetchResult= */ null));
    assertThat(thrown.getMessage()).contains("source file doesn't exist");
  }

  /**
   * Key regression test: when FetchResult is provided, copyToolInputsIntoWorkerToolRoot should copy
   * tool inputs from CAS paths (in FetchResult.toolInputCasPaths) instead of from opRoot. Without
   * the fix, this test throws IOException: "source file doesn't exist".
   */
  @Test
  public void copyToolInputsIntoWorkerToolRoot_withFetchResult_copiesFromCasPaths()
      throws Exception {
    ProtoCoordinator pc = newCoordinator();

    Path fsRoot = jimFsRoot();
    Path opRoot = fsRoot.resolve("opRoot_fetchResult");
    Files.createDirectory(opRoot);

    // Create a CAS directory (simulating the local CAS storage)
    Path casDir = fsRoot.resolve("cas");
    Files.createDirectories(casDir);

    List<TreeFile> fileInputs =
        ImmutableList.of(
            new TreeFile("file_1", "file contents 1"),
            new TreeFile("tools_dir/tool_file", "tool file contents", true),
            new TreeFile("tools_dir/tool_file_2", "tool file contents 2", true));

    Tree tree = WorkerTestUtils.makeTree(opRoot.toString(), fileInputs);
    Command command = WorkerTestUtils.makeCommand();
    WorkFilesContext ctx = WorkFilesContext.fromContext(opRoot, tree, command);
    WorkerInputs workerFiles = WorkerInputs.from(ctx, ImmutableList.of("reqArg1"));

    // Do NOT create files at opRoot — simulating lightweight exec dir.
    // Instead, create tool input files in the "CAS" directory.
    Path casTool1 = casDir.resolve("tool_hash_1");
    Path casTool2 = casDir.resolve("tool_hash_2");
    Files.write(casTool1, "tool file contents".getBytes());
    Files.write(casTool2, "tool file contents 2".getBytes());

    // Build a FetchResult with toolInputCasPaths mapping relative paths → CAS paths
    CASFileCache mockCache = mock(CASFileCache.class);
    FetchResult fetchResult =
        new FetchResult(
            ImmutableList.of(), // no entries (tool inputs not in entries)
            ImmutableSet.of(),
            ImmutableList.of(),
            ImmutableList.of(),
            DigestFunction.Value.SHA256,
            mockCache,
            ImmutableMap.of(
                "tools_dir/tool_file", casTool1,
                "tools_dir/tool_file_2", casTool2),
            ImmutableSet.of());

    WorkerKey key = makeWorkerKey(ctx, workerFiles, fsRoot.resolve("workRootsDir_fetchResult"));

    // This should succeed — copying from CAS paths, not from opRoot
    pc.copyToolInputsIntoWorkerToolRoot(key, workerFiles, fetchResult);

    Path toolsRoot = key.getToolRoot();
    assertThat(Files.exists(toolsRoot)).isTrue();
    Set<Path> expectedToolInputs = new HashSet<>();
    for (TreeFile file : fileInputs) {
      if (file.isTool) {
        expectedToolInputs.add(toolsRoot.resolve(file.path));
      }
    }
    assertThat(WorkerTestUtils.listFilesRec(key.getExecRoot()))
        .containsExactlyElementsIn(expectedToolInputs);
  }

  /**
   * Regression test for zero-size tool inputs (e.g. _repo_mapping in .runfiles). Zero-size files
   * have no CAS entry, so they're not in toolInputCasPaths. Without the fix,
   * copyToolInputsIntoWorkerToolRoot throws IOException for the missing path.
   */
  @Test
  public void copyToolInputsIntoWorkerToolRoot_withFetchResult_handlesZeroSizeToolInputs()
      throws Exception {
    ProtoCoordinator pc = newCoordinator();

    Path fsRoot = jimFsRoot();
    Path opRoot = fsRoot.resolve("opRoot_zeroSize");
    Files.createDirectory(opRoot);

    Path casDir = fsRoot.resolve("cas_zeroSize");
    Files.createDirectories(casDir);

    List<TreeFile> fileInputs =
        ImmutableList.of(
            new TreeFile("file_1", "file contents 1"),
            new TreeFile("tools_dir/tool_file", "tool file contents", true),
            new TreeFile("tools_dir/zero_size_tool", "", true));

    Tree tree = WorkerTestUtils.makeTree(opRoot.toString(), fileInputs);
    Command command = WorkerTestUtils.makeCommand();
    WorkFilesContext ctx = WorkFilesContext.fromContext(opRoot, tree, command);
    WorkerInputs workerFiles = WorkerInputs.from(ctx, ImmutableList.of("reqArg1"));

    // Create non-zero tool input in CAS; zero-size tool has no CAS entry.
    Path casTool = casDir.resolve("tool_hash");
    Files.write(casTool, "tool file contents".getBytes());

    CASFileCache mockCache = mock(CASFileCache.class);
    FetchResult fetchResult =
        new FetchResult(
            ImmutableList.of(),
            ImmutableSet.of(),
            ImmutableList.of(),
            ImmutableList.of(),
            DigestFunction.Value.SHA256,
            mockCache,
            ImmutableMap.of("tools_dir/tool_file", casTool),
            ImmutableSet.of("tools_dir/zero_size_tool"));

    WorkerKey key = makeWorkerKey(ctx, workerFiles, fsRoot.resolve("workRootsDir_zeroSize"));

    // Should succeed: non-zero tool copied from CAS, zero-size tool created as empty file
    pc.copyToolInputsIntoWorkerToolRoot(key, workerFiles, fetchResult);

    Path toolsRoot = key.getToolRoot();
    assertThat(Files.exists(toolsRoot.resolve("tools_dir/tool_file"))).isTrue();
    assertThat(Files.exists(toolsRoot.resolve("tools_dir/zero_size_tool"))).isTrue();
    assertThat(Files.size(toolsRoot.resolve("tools_dir/zero_size_tool"))).isEqualTo(0);
  }

  /**
   * Verifies that copyToolInputsIntoWorkerToolRoot with FetchResult skips tool files that already
   * exist in the tool root (idempotency).
   */
  @Test
  public void copyToolInputsIntoWorkerToolRoot_withFetchResult_skipsExistingToolFiles()
      throws Exception {
    ProtoCoordinator pc = newCoordinator();

    Path fsRoot = jimFsRoot();
    Path opRoot = fsRoot.resolve("opRoot_idempotent");
    Files.createDirectory(opRoot);

    Path casDir = fsRoot.resolve("cas_idempotent");
    Files.createDirectories(casDir);

    List<TreeFile> fileInputs =
        ImmutableList.of(
            new TreeFile("file_1", "file contents 1"),
            new TreeFile("tools_dir/tool_file", "tool file contents", true));

    Tree tree = WorkerTestUtils.makeTree(opRoot.toString(), fileInputs);
    Command command = WorkerTestUtils.makeCommand();
    WorkFilesContext ctx = WorkFilesContext.fromContext(opRoot, tree, command);
    WorkerInputs workerFiles = WorkerInputs.from(ctx, ImmutableList.of("reqArg1"));

    Path casTool = casDir.resolve("tool_hash");
    Files.write(casTool, "tool file contents".getBytes());

    CASFileCache mockCache = mock(CASFileCache.class);
    FetchResult fetchResult =
        new FetchResult(
            ImmutableList.of(),
            ImmutableSet.of(),
            ImmutableList.of(),
            ImmutableList.of(),
            DigestFunction.Value.SHA256,
            mockCache,
            ImmutableMap.of("tools_dir/tool_file", casTool),
            ImmutableSet.of());

    WorkerKey key = makeWorkerKey(ctx, workerFiles, fsRoot.resolve("workRootsDir_idempotent"));

    // First call: copies from CAS
    pc.copyToolInputsIntoWorkerToolRoot(key, workerFiles, fetchResult);
    Path toolsRoot = key.getToolRoot();
    assertThat(Files.exists(toolsRoot.resolve("tools_dir/tool_file"))).isTrue();

    // Second call: should be a no-op (files already exist)
    pc.copyToolInputsIntoWorkerToolRoot(key, workerFiles, fetchResult);
    assertThat(Files.exists(toolsRoot.resolve("tools_dir/tool_file"))).isTrue();
  }

  @Test
  public void runOnPersistentWorker_workerCreationFailureClosesFetchResult() throws Exception {
    Path fsRoot = Files.createTempDirectory("persistent-worker-failure");
    Path opRoot = fsRoot.resolve("opRoot_workerFailure");
    Files.createDirectory(opRoot);
    Tree tree =
        WorkerTestUtils.makeTree(
            opRoot.toString(), ImmutableList.of(new TreeFile("dummy", "content")));
    WorkFilesContext context =
        WorkFilesContext.fromContext(opRoot, tree, Command.getDefaultInstance());

    CASFileCache mockCache = mock(CASFileCache.class);
    FetchResult fetchResult =
        new FetchResult(
            ImmutableList.of(),
            ImmutableSet.of(),
            ImmutableList.of("sha256_worker_failure"),
            ImmutableList.of(),
            DigestFunction.Value.SHA256,
            mockCache,
            ImmutableMap.of(),
            ImmutableSet.of());

    ActionResult.Builder resultBuilder = ActionResult.newBuilder();
    Code code =
        PersistentExecutor.runOnPersistentWorker(
            context,
            "operations/worker-failure",
            ImmutableList.of("/definitely/not/a/real/persistent_worker_binary"),
            ImmutableMap.of(),
            new ResourceLimits(),
            Duration.newBuilder().setSeconds(10).build(),
            fsRoot.resolve("workRoots"),
            fetchResult,
            resultBuilder);

    assertThat(code).isEqualTo(Code.OK);
    assertThat(resultBuilder.getExitCode()).isEqualTo(-1);
    assertThat(fetchResult.isClosed()).isTrue();
    verify(mockCache)
        .decrementReferences(
            ImmutableList.of("sha256_worker_failure"),
            ImmutableList.of(),
            DigestFunction.Value.SHA256);
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
    assertThrows(IOException.class, () -> protoCoordinator.preWorkInit(null, request, mockWorker));

    // Make sure that, despite the error, the request is cleaned up from the map of pending
    // requests
    assertThat(ProtoCoordinator.hasPendingRequest(request)).isFalse();
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

  // ---- FetchResult-based linking tests (Phase 2 paths) ----

  // NOTE: linkNontoolInputs tests were removed — that code path was dead code
  // (InputFetcher always produces a FetchResult when linkInputs is true).
  // Tests for symlink-from-opRoot behavior (preWorkInit_symlinksNontoolInputDirectories,
  // preWorkInit_fallsBackToFileSymlinksForDirsWithToolInputs, cleanUp_* symlink tests, etc.)
  // were deleted as the code they tested no longer exists.

  /**
   * Helper to create a FetchResult with real JimFS CAS paths so linkEntry can create actual links.
   */
  private FetchResult makeFetchResultWithRealPaths(
      Path casRoot,
      ImmutableList<FetchResult.Entry> entries,
      ImmutableSet<String> descendedDirectories,
      CASFileCache mockCache) {
    ImmutableList.Builder<String> fileKeys = ImmutableList.builder();
    ImmutableList.Builder<build.bazel.remote.execution.v2.Digest> dirDigests =
        ImmutableList.builder();
    for (FetchResult.Entry entry : entries) {
      if (entry.type() == FetchResult.EntryType.FILE && entry.key() != null) {
        fileKeys.add(entry.key());
      }
      if (entry.type() == FetchResult.EntryType.DIRECTORY && entry.digest() != null) {
        dirDigests.add(entry.digest());
      }
    }
    return new FetchResult(
        entries,
        descendedDirectories,
        fileKeys.build(),
        dirDigests.build(),
        DigestFunction.Value.SHA256,
        mockCache,
        ImmutableMap.of(),
        ImmutableSet.of());
  }

  /**
   * Helper to create a minimal RequestCtx with a FetchResult (no WorkerInputs needed for
   * FetchResult path).
   */
  private RequestCtx makeFetchResultRequest(Path opRoot, FetchResult fetchResult) {
    Tree tree =
        WorkerTestUtils.makeTree(
            opRoot.toString(), ImmutableList.of(new TreeFile("dummy", "content")));
    Command command = WorkerTestUtils.makeCommand();
    WorkFilesContext ctx = WorkFilesContext.fromContext(opRoot, tree, command);
    WorkerInputs workerFiles = WorkerInputs.from(ctx, ImmutableList.of("reqArg1"));
    return new RequestCtx(
        WorkRequest.getDefaultInstance(),
        ctx,
        workerFiles,
        Duration.newBuilder().setSeconds(10).build(),
        fetchResult);
  }

  @Test
  public void preWorkInit_linkFromFetchResult_createsAllEntryTypes() throws Exception {
    ProtoCoordinator protoCoordinator = newCoordinator();

    Path fsRoot = jimFsRoot();
    Path casRoot = fsRoot.resolve("cas_allTypes");
    Files.createDirectory(casRoot);
    Path opRoot = fsRoot.resolve("opRoot_allTypes");
    Files.createDirectory(opRoot);
    Path workerExecRoot = fsRoot.resolve("workerExecRoot_allTypes");
    Files.createDirectory(workerExecRoot);

    // Set up CAS targets — real files and directories that linkEntry will link to
    Path casFile = casRoot.resolve("file_abc123");
    Files.write(casFile, "file content".getBytes());

    Path casDir = casRoot.resolve("dir_def456");
    Files.createDirectory(casDir);
    Files.write(casDir.resolve("inner.txt"), "inner".getBytes());

    build.bazel.remote.execution.v2.Digest dirDigest =
        build.bazel.remote.execution.v2.Digest.newBuilder()
            .setHash("def456")
            .setSizeBytes(100)
            .build();

    CASFileCache mockCache = mock(CASFileCache.class);

    // SYMLINK_NODE is exercised separately in preWorkInit_linkFromFetchResult_createsSymlinkNode;
    // here we verify FILE, DIRECTORY, and ZERO_SIZE_FILE.
    ImmutableList<FetchResult.Entry> entries =
        ImmutableList.of(
            new FetchResult.Entry(
                "src/Foo.java", FetchResult.EntryType.FILE, casFile, "sha256_abc123", null, null),
            new FetchResult.Entry(
                "lib", FetchResult.EntryType.DIRECTORY, casDir, null, dirDigest, null),
            new FetchResult.Entry(
                "src/empty.txt", FetchResult.EntryType.ZERO_SIZE_FILE, null, null, null, null));

    FetchResult fetchResult =
        makeFetchResultWithRealPaths(casRoot, entries, ImmutableSet.of("src"), mockCache);

    RequestCtx request = makeFetchResultRequest(opRoot, fetchResult);

    PersistentWorker mockWorker = mock(PersistentWorker.class);
    when(mockWorker.getExecRoot()).thenReturn(workerExecRoot);

    protoCoordinator.preWorkInit(null, request, mockWorker);

    // FILE entry: hardlink to CAS file
    Path fileInExec = workerExecRoot.resolve("src/Foo.java");
    assertThat(Files.exists(fileInExec)).isTrue();
    assertThat(Files.isSymbolicLink(fileInExec)).isFalse(); // hardlink, not symlink
    // Same inode as the CAS file (a hardlink, not a byte copy — a copy would reintroduce the
    // overhead this path exists to avoid).
    assertThat(Files.isSameFile(fileInExec, casFile)).isTrue();
    assertThat(new String(Files.readAllBytes(fileInExec))).isEqualTo("file content");

    // DIRECTORY entry: symlink to CAS directory
    Path dirInExec = workerExecRoot.resolve("lib");
    assertThat(Files.isSymbolicLink(dirInExec)).isTrue();
    assertThat(Files.readSymbolicLink(dirInExec)).isEqualTo(casDir);

    // ZERO_SIZE_FILE entry: empty file created
    Path emptyInExec = workerExecRoot.resolve("src/empty.txt");
    assertThat(Files.exists(emptyInExec)).isTrue();
    assertThat(Files.size(emptyInExec)).isEqualTo(0);

    // All entries should be in trackedLinks
    assertThat(request.trackedLinks).hasSize(3);
  }

  @Test
  public void postWorkCleanup_cleanUpFetchResultLinks_deletesAll() throws Exception {
    ProtoCoordinator protoCoordinator = newCoordinator();

    Path fsRoot = jimFsRoot();
    Path casRoot = fsRoot.resolve("cas_cleanup");
    Files.createDirectory(casRoot);
    Path opRoot = fsRoot.resolve("opRoot_cleanup");
    Files.createDirectory(opRoot);
    Path workerExecRoot = fsRoot.resolve("workerExecRoot_cleanup");
    Files.createDirectory(workerExecRoot);

    // CAS targets
    Path casFile = casRoot.resolve("file_aaa");
    Files.write(casFile, "content".getBytes());
    Path casDir = casRoot.resolve("dir_bbb");
    Files.createDirectory(casDir);

    build.bazel.remote.execution.v2.Digest dirDigest =
        build.bazel.remote.execution.v2.Digest.newBuilder().setHash("bbb").setSizeBytes(50).build();

    CASFileCache mockCache = mock(CASFileCache.class);

    ImmutableList<FetchResult.Entry> entries =
        ImmutableList.of(
            new FetchResult.Entry(
                "src/A.java", FetchResult.EntryType.FILE, casFile, "sha256_aaa", null, null),
            new FetchResult.Entry(
                "lib", FetchResult.EntryType.DIRECTORY, casDir, null, dirDigest, null));

    FetchResult fetchResult =
        makeFetchResultWithRealPaths(casRoot, entries, ImmutableSet.of("src"), mockCache);

    RequestCtx request = makeFetchResultRequest(opRoot, fetchResult);

    PersistentWorker mockWorker = mock(PersistentWorker.class);
    when(mockWorker.getExecRoot()).thenReturn(workerExecRoot);

    protoCoordinator.preWorkInit(null, request, mockWorker);

    // Verify links exist before cleanup
    assertThat(Files.exists(workerExecRoot.resolve("src/A.java"))).isTrue();
    assertThat(Files.isSymbolicLink(workerExecRoot.resolve("lib"))).isTrue();

    // Cleanup via successful response
    WorkResponse response = WorkResponse.newBuilder().setExitCode(0).build();
    protoCoordinator.postWorkCleanup(response, mockWorker, request);

    // FetchResult should be closed (refs decremented)
    assertThat(fetchResult.isClosed()).isTrue();
    verify(mockCache)
        .decrementReferences(
            ImmutableList.of("sha256_aaa"),
            ImmutableList.of(dirDigest),
            DigestFunction.Value.SHA256);
  }

  @Test
  public void postWorkCleanup_nullResponse_cleanUpPartialState() throws Exception {
    ProtoCoordinator protoCoordinator = newCoordinator();

    Path fsRoot = jimFsRoot();
    Path casRoot = fsRoot.resolve("cas_partial");
    Files.createDirectory(casRoot);
    Path opRoot = fsRoot.resolve("opRoot_partial");
    Files.createDirectory(opRoot);
    Path workerExecRoot = fsRoot.resolve("workerExecRoot_partial");
    Files.createDirectory(workerExecRoot);

    Path casFile = casRoot.resolve("file_ccc");
    Files.write(casFile, "content".getBytes());

    CASFileCache mockCache = mock(CASFileCache.class);

    ImmutableList<FetchResult.Entry> entries =
        ImmutableList.of(
            new FetchResult.Entry(
                "src/B.java", FetchResult.EntryType.FILE, casFile, "sha256_ccc", null, null));

    FetchResult fetchResult =
        makeFetchResultWithRealPaths(casRoot, entries, ImmutableSet.of("src"), mockCache);

    RequestCtx request = makeFetchResultRequest(opRoot, fetchResult);

    PersistentWorker mockWorker = mock(PersistentWorker.class);
    when(mockWorker.getExecRoot()).thenReturn(workerExecRoot);

    protoCoordinator.preWorkInit(null, request, mockWorker);

    // Verify link exists before cleanup
    assertThat(Files.exists(workerExecRoot.resolve("src/B.java"))).isTrue();

    // Simulate failure: null response
    protoCoordinator.postWorkCleanup(null, mockWorker, request);

    // Partial state should be cleaned up
    assertThat(Files.exists(workerExecRoot.resolve("src/B.java"))).isFalse();

    // FetchResult should be closed
    assertThat(fetchResult.isClosed()).isTrue();
  }

  @Test
  public void postWorkCleanup_deleteFailureStillClosesFetchResult() throws Exception {
    ProtoCoordinator protoCoordinator = newCoordinator();

    Path fsRoot = jimFsRoot();
    Path opRoot = fsRoot.resolve("opRoot_deleteFailure");
    Files.createDirectory(opRoot);
    Path workerExecRoot = fsRoot.resolve("workerExecRoot_deleteFailure");
    Files.createDirectory(workerExecRoot);

    CASFileCache mockCache = mock(CASFileCache.class);
    FetchResult fetchResult =
        new FetchResult(
            ImmutableList.of(),
            ImmutableSet.of(),
            ImmutableList.of("sha256_cleanup"),
            ImmutableList.of(),
            DigestFunction.Value.SHA256,
            mockCache,
            ImmutableMap.of(),
            ImmutableSet.of());
    RequestCtx request = makeFetchResultRequest(opRoot, fetchResult);

    Path undeletable = workerExecRoot.resolve("undeletable");
    Files.createDirectories(undeletable);
    Files.write(undeletable.resolve("child"), "content".getBytes());
    request.trackedLinks.add(undeletable);

    PersistentWorker mockWorker = mock(PersistentWorker.class);
    when(mockWorker.getExecRoot()).thenReturn(workerExecRoot);

    protoCoordinator.postWorkCleanup(null, mockWorker, request);

    assertThat(fetchResult.isClosed()).isTrue();
    verify(mockCache)
        .decrementReferences(
            ImmutableList.of("sha256_cleanup"), ImmutableList.of(), DigestFunction.Value.SHA256);
  }

  // Descended directories — including a recursive-output-root descendant (bazel-out/k8, bin) that
  // is
  // absent from ExclusionSet.paths() — must be removed bottom-up so the reused worker exec root
  // does
  // not strand a real dir where a later request needs a directory symlink.
  @Test
  public void postWorkCleanup_nullResponse_cleansUpEmptyDescendedDirs() throws Exception {
    ProtoCoordinator protoCoordinator = newCoordinator();

    Path fsRoot = jimFsRoot();
    Path casRoot = fsRoot.resolve("cas_emptyDirs");
    Files.createDirectory(casRoot);
    Path opRoot = fsRoot.resolve("opRoot_emptyDirs");
    Files.createDirectory(opRoot);
    Path workerExecRoot = fsRoot.resolve("workerExecRoot_emptyDirs");
    Files.createDirectory(workerExecRoot);

    Path casFile = casRoot.resolve("file_ddd");
    Files.write(casFile, "content".getBytes());

    CASFileCache mockCache = mock(CASFileCache.class);

    // File deep in a kept-real (descended) subtree — writeFileSafe creates the intermediate dirs
    ImmutableList<FetchResult.Entry> entries =
        ImmutableList.of(
            new FetchResult.Entry(
                "bazel-out/k8/bin/file.java",
                FetchResult.EntryType.FILE,
                casFile,
                "sha256_ddd",
                null,
                null));

    FetchResult fetchResult =
        makeFetchResultWithRealPaths(
            casRoot,
            entries,
            ImmutableSet.of("bazel-out", "bazel-out/k8", "bazel-out/k8/bin"),
            mockCache);

    RequestCtx request = makeFetchResultRequest(opRoot, fetchResult);

    PersistentWorker mockWorker = mock(PersistentWorker.class);
    when(mockWorker.getExecRoot()).thenReturn(workerExecRoot);

    protoCoordinator.preWorkInit(null, request, mockWorker);

    // Verify link and intermediate dirs exist
    assertThat(Files.exists(workerExecRoot.resolve("bazel-out/k8/bin/file.java"))).isTrue();
    assertThat(Files.isDirectory(workerExecRoot.resolve("bazel-out/k8/bin"))).isTrue();
    assertThat(Files.isDirectory(workerExecRoot.resolve("bazel-out/k8"))).isTrue();
    assertThat(Files.isDirectory(workerExecRoot.resolve("bazel-out"))).isTrue();

    // Simulate failure: null response
    protoCoordinator.postWorkCleanup(null, mockWorker, request);

    // File should be deleted
    assertThat(Files.exists(workerExecRoot.resolve("bazel-out/k8/bin/file.java"))).isFalse();
    // Empty descended dirs should also be cleaned up (prevents blocking symlink creation next time)
    assertThat(Files.exists(workerExecRoot.resolve("bazel-out/k8/bin"))).isFalse();
    assertThat(Files.exists(workerExecRoot.resolve("bazel-out/k8"))).isFalse();
    assertThat(Files.exists(workerExecRoot.resolve("bazel-out"))).isFalse();
  }

  @Test
  public void preWorkInit_linkFromFetchResult_createsSymlinkNode() throws Exception {
    ProtoCoordinator protoCoordinator = newCoordinator();

    Path fsRoot = jimFsRoot();
    Path opRoot = fsRoot.resolve("opRoot_symlink");
    Files.createDirectory(opRoot);
    Path workerExecRoot = fsRoot.resolve("workerExecRoot_symlink");
    Files.createDirectory(workerExecRoot);

    // Create a target file that the symlink will point to (relative path)
    Files.createDirectories(workerExecRoot.resolve("real"));
    Files.write(workerExecRoot.resolve("real/target.txt"), "target content".getBytes());

    CASFileCache mockCache = mock(CASFileCache.class);

    // SYMLINK_NODE entry: link at "src/link.txt" -> "../real/target.txt"
    ImmutableList<FetchResult.Entry> entries =
        ImmutableList.of(
            new FetchResult.Entry(
                "src/link.txt",
                FetchResult.EntryType.SYMLINK_NODE,
                null,
                null,
                null,
                "../real/target.txt"));

    FetchResult fetchResult =
        makeFetchResultWithRealPaths(fsRoot, entries, ImmutableSet.of("src"), mockCache);

    RequestCtx request = makeFetchResultRequest(opRoot, fetchResult);

    PersistentWorker mockWorker = mock(PersistentWorker.class);
    when(mockWorker.getExecRoot()).thenReturn(workerExecRoot);

    protoCoordinator.preWorkInit(null, request, mockWorker);

    // SYMLINK_NODE entry: symlink created at the expected path
    Path symlinkPath = workerExecRoot.resolve("src/link.txt");
    assertThat(Files.exists(symlinkPath)).isTrue();
    assertThat(Files.isSymbolicLink(symlinkPath)).isTrue();

    // Symlink target should be the relative path we specified
    Path targetPath = Files.readSymbolicLink(symlinkPath);
    assertThat(targetPath.toString()).isEqualTo("../real/target.txt");

    // Should be in trackedLinks
    assertThat(request.trackedLinks).contains(symlinkPath);

    // Verify the symlink resolves to the actual target content
    assertThat(new String(Files.readAllBytes(symlinkPath))).isEqualTo("target content");
  }

  // The worker exec root is reused across requests. A prior request can leave an artifact where a
  // later request's input must link; createHardlink/createSymlink would otherwise throw
  // FileAlreadyExistsException. These tests verify the leftover is replaced (the old copy path
  // tolerated this via Files.copy(REPLACE_EXISTING)).

  @Test
  public void preWorkInit_linkFromFetchResult_replacesLeftoverFile() throws Exception {
    ProtoCoordinator protoCoordinator = newCoordinator();

    Path fsRoot = jimFsRoot();
    Path casRoot = fsRoot.resolve("cas_leftoverFile");
    Files.createDirectory(casRoot);
    Path opRoot = fsRoot.resolve("opRoot_leftoverFile");
    Files.createDirectory(opRoot);
    Path workerExecRoot = fsRoot.resolve("workerExecRoot_leftoverFile");
    Files.createDirectory(workerExecRoot);

    Path casFile = casRoot.resolve("file_new");
    Files.write(casFile, "new content".getBytes());

    // Leftover from a prior request: a stale regular file where this request's input will link.
    Path leftover = workerExecRoot.resolve("src/Foo.java");
    Files.createDirectories(leftover.getParent());
    Files.write(leftover, "stale content".getBytes());

    CASFileCache mockCache = mock(CASFileCache.class);
    ImmutableList<FetchResult.Entry> entries =
        ImmutableList.of(
            new FetchResult.Entry(
                "src/Foo.java", FetchResult.EntryType.FILE, casFile, "sha256_new", null, null));
    FetchResult fetchResult =
        makeFetchResultWithRealPaths(casRoot, entries, ImmutableSet.of("src"), mockCache);
    RequestCtx request = makeFetchResultRequest(opRoot, fetchResult);

    PersistentWorker mockWorker = mock(PersistentWorker.class);
    when(mockWorker.getExecRoot()).thenReturn(workerExecRoot);

    // No FileAlreadyExistsException: the leftover is replaced by a hardlink to the CAS file.
    protoCoordinator.preWorkInit(null, request, mockWorker);

    assertThat(Files.isSameFile(leftover, casFile)).isTrue();
    assertThat(new String(Files.readAllBytes(leftover))).isEqualTo("new content");
  }

  @Test
  public void preWorkInit_linkFromFetchResult_replacesNonEmptyLeftoverDirWithSymlink()
      throws Exception {
    ProtoCoordinator protoCoordinator = newCoordinator();

    Path fsRoot = jimFsRoot();
    Path casRoot = fsRoot.resolve("cas_leftoverDir");
    Files.createDirectory(casRoot);
    Path opRoot = fsRoot.resolve("opRoot_leftoverDir");
    Files.createDirectory(opRoot);
    Path workerExecRoot = fsRoot.resolve("workerExecRoot_leftoverDir");
    Files.createDirectory(workerExecRoot);

    Path casDir = casRoot.resolve("dir_new");
    Files.createDirectory(casDir);
    Files.write(casDir.resolve("inner.txt"), "inner".getBytes());
    build.bazel.remote.execution.v2.Digest dirDigest =
        build.bazel.remote.execution.v2.Digest.newBuilder()
            .setHash("dirnew")
            .setSizeBytes(10)
            .build();

    // Leftover from a prior request: a non-empty real directory where this request wants a symlink.
    Path leftoverDir = workerExecRoot.resolve("lib");
    Files.createDirectories(leftoverDir);
    Files.write(leftoverDir.resolve("stale_artifact.txt"), "stale".getBytes());

    CASFileCache mockCache = mock(CASFileCache.class);
    ImmutableList<FetchResult.Entry> entries =
        ImmutableList.of(
            new FetchResult.Entry(
                "lib", FetchResult.EntryType.DIRECTORY, casDir, null, dirDigest, null));
    FetchResult fetchResult =
        makeFetchResultWithRealPaths(casRoot, entries, ImmutableSet.of(), mockCache);
    RequestCtx request = makeFetchResultRequest(opRoot, fetchResult);

    PersistentWorker mockWorker = mock(PersistentWorker.class);
    when(mockWorker.getExecRoot()).thenReturn(workerExecRoot);

    protoCoordinator.preWorkInit(null, request, mockWorker);

    // The non-empty leftover directory was removed and replaced by the CAS symlink.
    assertThat(Files.isSymbolicLink(leftoverDir)).isTrue();
    assertThat(Files.readSymbolicLink(leftoverDir)).isEqualTo(casDir);
  }

  @Test
  public void preWorkInit_linkFromFetchResult_replacesLeftoverSymlinkWithoutFollowing()
      throws Exception {
    ProtoCoordinator protoCoordinator = newCoordinator();

    Path fsRoot = jimFsRoot();
    Path casRoot = fsRoot.resolve("cas_leftoverSymlink");
    Files.createDirectory(casRoot);
    Path opRoot = fsRoot.resolve("opRoot_leftoverSymlink");
    Files.createDirectory(opRoot);
    Path workerExecRoot = fsRoot.resolve("workerExecRoot_leftoverSymlink");
    Files.createDirectory(workerExecRoot);

    Path casFile = casRoot.resolve("file_real");
    Files.write(casFile, "real content".getBytes());

    // A sentinel standing in for shared CAS content the leftover symlink points at. Removing the
    // symlink must NOT follow it and delete/modify this target — that would corrupt the CAS.
    Path sentinel = fsRoot.resolve("sentinel.txt");
    Files.write(sentinel, "sentinel content".getBytes());

    // Leftover from a prior request: a symlink where this request's input file will link.
    Path leftoverLink = workerExecRoot.resolve("src/Bar.java");
    Files.createDirectories(leftoverLink.getParent());
    Files.createSymbolicLink(leftoverLink, sentinel);

    CASFileCache mockCache = mock(CASFileCache.class);
    ImmutableList<FetchResult.Entry> entries =
        ImmutableList.of(
            new FetchResult.Entry(
                "src/Bar.java", FetchResult.EntryType.FILE, casFile, "sha256_real", null, null));
    FetchResult fetchResult =
        makeFetchResultWithRealPaths(casRoot, entries, ImmutableSet.of("src"), mockCache);
    RequestCtx request = makeFetchResultRequest(opRoot, fetchResult);

    PersistentWorker mockWorker = mock(PersistentWorker.class);
    when(mockWorker.getExecRoot()).thenReturn(workerExecRoot);

    protoCoordinator.preWorkInit(null, request, mockWorker);

    // The leftover symlink was unlinked (not followed) and replaced by a hardlink to the CAS file.
    assertThat(Files.isSymbolicLink(leftoverLink)).isFalse();
    assertThat(Files.isSameFile(leftoverLink, casFile)).isTrue();
    // Critically, the symlink's former target is untouched — we never followed the link.
    assertThat(Files.exists(sentinel)).isTrue();
    assertThat(new String(Files.readAllBytes(sentinel))).isEqualTo("sentinel content");
  }

  @Test
  public void preWorkInit_linkFromFetchResult_replacesLeftoverZeroSizeAndSymlinkNode()
      throws Exception {
    ProtoCoordinator protoCoordinator = newCoordinator();

    Path fsRoot = jimFsRoot();
    Path opRoot = fsRoot.resolve("opRoot_leftoverDirect");
    Files.createDirectory(opRoot);
    Path workerExecRoot = fsRoot.resolve("workerExecRoot_leftoverDirect");
    Files.createDirectory(workerExecRoot);

    // Leftovers from a prior request at the zero-size-file and symlink-node paths (these entry
    // types are created directly via Files.* and routed through createReplacingOnConflict).
    Path zeroLeftover = workerExecRoot.resolve("gen/empty.txt");
    Files.createDirectories(zeroLeftover.getParent());
    Files.write(zeroLeftover, "stale".getBytes());
    Path symlinkLeftover = workerExecRoot.resolve("gen/link.txt");
    Files.createSymbolicLink(symlinkLeftover, fsRoot.resolve("old_target"));

    CASFileCache mockCache = mock(CASFileCache.class);
    ImmutableList<FetchResult.Entry> entries =
        ImmutableList.of(
            new FetchResult.Entry(
                "gen/empty.txt", FetchResult.EntryType.ZERO_SIZE_FILE, null, null, null, null),
            new FetchResult.Entry(
                "gen/link.txt",
                FetchResult.EntryType.SYMLINK_NODE,
                null,
                null,
                null,
                "target.txt"));
    FetchResult fetchResult =
        makeFetchResultWithRealPaths(workerExecRoot, entries, ImmutableSet.of("gen"), mockCache);
    RequestCtx request = makeFetchResultRequest(opRoot, fetchResult);

    PersistentWorker mockWorker = mock(PersistentWorker.class);
    when(mockWorker.getExecRoot()).thenReturn(workerExecRoot);

    protoCoordinator.preWorkInit(null, request, mockWorker);

    // Zero-size leftover replaced by an empty file.
    assertThat(Files.exists(zeroLeftover)).isTrue();
    assertThat(Files.isSymbolicLink(zeroLeftover)).isFalse();
    assertThat(Files.size(zeroLeftover)).isEqualTo(0);

    // Symlink-node leftover replaced with this request's relative target.
    assertThat(Files.isSymbolicLink(symlinkLeftover)).isTrue();
    assertThat(Files.readSymbolicLink(symlinkLeftover).toString()).isEqualTo("target.txt");
  }

  // End-to-end reuse: run two full preWorkInit -> postWorkCleanup cycles on ONE worker exec root
  // (the persistent worker's whole point). The second request links a directory symlink where the
  // first request's worker left an undeclared artifact, exercising both cleanup and replacement.
  @Test
  public void preWorkInit_reusedWorkerExecRoot_secondRequestSucceedsDespitePriorArtifacts()
      throws Exception {
    ProtoCoordinator protoCoordinator = newCoordinator();

    Path fsRoot = jimFsRoot();
    Path casRoot = fsRoot.resolve("cas_reuse");
    Files.createDirectory(casRoot);
    Path opRoot = fsRoot.resolve("opRoot_reuse");
    Files.createDirectory(opRoot);
    Path workerExecRoot = fsRoot.resolve("workerExecRoot_reuse");
    Files.createDirectory(workerExecRoot);

    Path casFileA = casRoot.resolve("file_a");
    Files.write(casFileA, "a content".getBytes());
    Path casDir = casRoot.resolve("dir_shared");
    Files.createDirectory(casDir);
    Files.write(casDir.resolve("inner.txt"), "inner".getBytes());
    build.bazel.remote.execution.v2.Digest dirDigest =
        build.bazel.remote.execution.v2.Digest.newBuilder()
            .setHash("shared")
            .setSizeBytes(10)
            .build();

    PersistentWorker mockWorker = mock(PersistentWorker.class);
    when(mockWorker.getExecRoot()).thenReturn(workerExecRoot);

    // Request A: a file under a kept-real (descended) directory "data".
    CASFileCache cacheA = mock(CASFileCache.class);
    ImmutableList<FetchResult.Entry> entriesA =
        ImmutableList.of(
            new FetchResult.Entry(
                "data/a.java", FetchResult.EntryType.FILE, casFileA, "key_a", null, null));
    FetchResult fetchA =
        makeFetchResultWithRealPaths(casRoot, entriesA, ImmutableSet.of("data"), cacheA);
    RequestCtx requestA = makeFetchResultRequest(opRoot, fetchA);
    protoCoordinator.preWorkInit(null, requestA, mockWorker);
    assertThat(Files.exists(workerExecRoot.resolve("data/a.java"))).isTrue();
    protoCoordinator.postWorkCleanup(
        WorkResponse.newBuilder().setExitCode(0).build(), mockWorker, requestA);
    // A's link and its now-empty descended dir are gone.
    assertThat(Files.exists(workerExecRoot.resolve("data/a.java"))).isFalse();
    assertThat(Files.exists(workerExecRoot.resolve("data"))).isFalse();

    // A's worker process left an undeclared artifact where B will link "data" as a directory unit.
    Files.createDirectories(workerExecRoot.resolve("data"));
    Files.write(workerExecRoot.resolve("data/undeclared.tmp"), "leftover".getBytes());

    // Request B (same worker exec root): "data" is now a directory symlink to CAS.
    CASFileCache cacheB = mock(CASFileCache.class);
    ImmutableList<FetchResult.Entry> entriesB =
        ImmutableList.of(
            new FetchResult.Entry(
                "data", FetchResult.EntryType.DIRECTORY, casDir, null, dirDigest, null));
    FetchResult fetchB = makeFetchResultWithRealPaths(casRoot, entriesB, ImmutableSet.of(), cacheB);
    RequestCtx requestB = makeFetchResultRequest(opRoot, fetchB);

    // Succeeds despite the leftover non-empty "data" directory from A's worker.
    protoCoordinator.preWorkInit(null, requestB, mockWorker);
    assertThat(Files.isSymbolicLink(workerExecRoot.resolve("data"))).isTrue();
    assertThat(Files.readSymbolicLink(workerExecRoot.resolve("data"))).isEqualTo(casDir);
  }
}
