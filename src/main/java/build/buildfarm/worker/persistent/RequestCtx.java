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

import com.google.devtools.build.lib.worker.WorkerProtocol.WorkRequest;
import com.google.protobuf.Duration;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import javax.annotation.Nullable;
import persistent.common.CtxAround;

public class RequestCtx implements CtxAround<WorkRequest> {
  public final WorkRequest request;

  public final WorkFilesContext filesContext;

  public final WorkerInputs workerInputs;

  public final Duration timeout;

  /** FetchResult from InputFetcher — carries CAS paths and ref keys for deferred linking. */
  @Nullable public final FetchResult fetchResult;

  /**
   * Tracks paths created in the worker exec root during preWorkInit (directory symlinks, file
   * hardlinks, zero-size files, symlink nodes). Populated incrementally during link creation. Read
   * during postWorkCleanup to delete all created links.
   */
  final Set<Path> trackedLinks = new HashSet<>();

  public RequestCtx(
      WorkRequest request, WorkFilesContext ctx, WorkerInputs workFiles, Duration timeout) {
    this(request, ctx, workFiles, timeout, /* fetchResult= */ null);
  }

  public RequestCtx(
      WorkRequest request,
      WorkFilesContext ctx,
      WorkerInputs workFiles,
      Duration timeout,
      @Nullable FetchResult fetchResult) {
    this.request = request;
    this.filesContext = ctx;
    this.workerInputs = workFiles;
    this.timeout = timeout;
    this.fetchResult = fetchResult;
  }

  @Override
  public WorkRequest get() {
    return request;
  }
}
