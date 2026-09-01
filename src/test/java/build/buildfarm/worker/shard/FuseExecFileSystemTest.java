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

package build.buildfarm.worker.shard;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import build.bazel.remote.execution.v2.Command;
import build.bazel.remote.execution.v2.DigestFunction;
import build.bazel.remote.execution.v2.Directory;
import build.bazel.remote.execution.v2.DirectoryNode;
import build.bazel.remote.execution.v2.FileNode;
import build.buildfarm.cas.cfc.CASFileCache;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.v1test.Digest;
import build.buildfarm.v1test.WorkerExecutedMetadata;
import build.buildfarm.worker.FuseCAS;
import com.google.common.jimfs.Jimfs;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.function.BiFunction;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class FuseExecFileSystemTest {
  @Test
  public void eagerlyStagesUniqueInputsAndReleasesReferences() throws Exception {
    Path workspace = Jimfs.newFileSystem().getPath("/worker");
    Files.createDirectories(workspace);
    Path localInput = workspace.resolve("cache/file");
    Files.createDirectories(localInput.getParent());
    Files.writeString(localInput, "input");

    Digest fileDigest = digest("file", 5);
    Digest childDigest = digest("child", 1);
    Digest rootDigest = digest("root", 1);
    build.bazel.remote.execution.v2.Digest reapiFile = DigestUtil.toDigest(fileDigest);
    Directory child =
        Directory.newBuilder()
            .addFiles(FileNode.newBuilder().setName("again").setDigest(reapiFile))
            .build();
    Directory root =
        Directory.newBuilder()
            .addFiles(FileNode.newBuilder().setName("input").setDigest(reapiFile))
            .addDirectories(
                DirectoryNode.newBuilder()
                    .setName("dir")
                    .setDigest(DigestUtil.toDigest(childDigest)))
            .build();

    CASFileCache fileCache = mock(CASFileCache.class);
    FuseCAS fuse = mock(FuseCAS.class);
    ExecutorService fetchService = mock(ExecutorService.class);
    ExecutorService removeService = mock(ExecutorService.class);
    ExecutorService accessRecorder = mock(ExecutorService.class);
    when(fileCache.put(eq(fileDigest), eq(false), eq(fetchService)))
        .thenReturn(immediateFuture(new CASFileCache.PathResult(localInput, true)));

    doAnswer(
            invocation -> {
              @SuppressWarnings("unchecked")
              BiFunction<Digest, Boolean, Path> resolver = invocation.getArgument(3);
              assertThat(resolver.apply(fileDigest, false)).isEqualTo(localInput);
              return null;
            })
        .when(fuse)
        .createInputRoot(eq("operation"), eq(rootDigest), any(), any());

    FuseExecFileSystem execFileSystem =
        new FuseExecFileSystem(
            workspace.resolve("execroots"),
            workspace.resolve("scratch"),
            fuse,
            fileCache,
            fetchService,
            removeService,
            accessRecorder);
    WorkerExecutedMetadata.Builder metadata = WorkerExecutedMetadata.newBuilder();

    Path execDir =
        execFileSystem.createExecDir(
            "operation",
            Map.of(DigestUtil.toDigest(rootDigest), root, DigestUtil.toDigest(childDigest), child),
            rootDigest,
            Command.getDefaultInstance(),
            null,
            metadata);

    assertThat(execDir).isEqualTo(workspace.resolve("execroots/operation"));
    assertThat(metadata.getFetchedBytes()).isEqualTo(fileDigest.getSize());
    verify(fileCache, times(1)).put(fileDigest, false, fetchService);

    execFileSystem.destroyExecDir(execDir);
    verify(fuse).destroyInputRoot("operation");
    verify(fileCache).decrementReference(CASFileCache.getKey(fileDigest, false));
  }

  @Test
  public void stagingFailureReleasesInputsThatWereAlreadyReferenced() throws Exception {
    Path workspace = Jimfs.newFileSystem().getPath("/worker");
    Files.createDirectories(workspace);
    Digest first = digest("first", 1);
    Digest second = digest("second", 1);
    Digest rootDigest = digest("root", 1);
    Directory root =
        Directory.newBuilder()
            .addFiles(FileNode.newBuilder().setName("first").setDigest(DigestUtil.toDigest(first)))
            .addFiles(
                FileNode.newBuilder().setName("second").setDigest(DigestUtil.toDigest(second)))
            .build();

    CASFileCache fileCache = mock(CASFileCache.class);
    FuseCAS fuse = mock(FuseCAS.class);
    ExecutorService fetchService = mock(ExecutorService.class);
    Path localInput = workspace.resolve("cache/first");
    when(fileCache.put(first, false, fetchService))
        .thenReturn(immediateFuture(new CASFileCache.PathResult(localInput, true)));
    when(fileCache.put(second, false, fetchService))
        .thenReturn(immediateFailedFuture(new IOException("missing input")));
    FuseExecFileSystem execFileSystem =
        new FuseExecFileSystem(
            workspace.resolve("execroots"),
            workspace.resolve("scratch"),
            fuse,
            fileCache,
            fetchService,
            mock(ExecutorService.class),
            mock(ExecutorService.class));

    assertThrows(
        IOException.class,
        () ->
            execFileSystem.createExecDir(
                "operation",
                Map.of(DigestUtil.toDigest(rootDigest), root),
                rootDigest,
                Command.getDefaultInstance(),
                null,
                WorkerExecutedMetadata.newBuilder()));

    verify(fileCache).decrementReference(CASFileCache.getKey(first, false));
    verifyNoInteractions(fuse);
  }

  private static Digest digest(String hash, long size) {
    return Digest.newBuilder()
        .setHash(hash)
        .setSize(size)
        .setDigestFunction(DigestFunction.Value.SHA256)
        .build();
  }
}
