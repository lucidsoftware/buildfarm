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

import build.bazel.remote.execution.v2.Command;
import build.buildfarm.v1test.Tree;
import build.buildfarm.worker.util.WorkerTestUtils;
import build.buildfarm.worker.util.WorkerTestUtils.TreeFile;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import com.google.devtools.build.lib.worker.WorkerProtocol.WorkRequest;
import com.google.protobuf.Duration;
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
}
