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

package build.buildfarm.instance;

import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.transform;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;

import build.bazel.remote.execution.v2.Compressor;
import build.bazel.remote.execution.v2.RequestMetadata;
import build.buildfarm.common.EntryLimitException;
import build.buildfarm.common.Write;
import build.buildfarm.common.WritesHelper;
import build.buildfarm.v1test.Digest;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/** Utility methods for the instance package. * */
public final class Utils {
  private Utils() {}

  public static ByteString getBlob(
      Instance instance,
      Compressor.Value compressor,
      Digest blobDigest,
      RequestMetadata requestMetadata)
      throws IOException, InterruptedException {
    return getBlob(
        instance, compressor, blobDigest, /* offset= */ 0, 60, TimeUnit.SECONDS, requestMetadata);
  }

  public static ByteString getBlob(
      Instance instance,
      Compressor.Value compressor,
      Digest blobDigest,
      long offset,
      long deadlineAfter,
      TimeUnit deadlineAfterUnits,
      RequestMetadata requestMetadata)
      throws IOException {
    try (InputStream in =
        instance.newBlobInput(
            compressor, blobDigest, offset, deadlineAfter, deadlineAfterUnits, requestMetadata)) {
      return ByteString.readFrom(in);
    } catch (StatusRuntimeException e) {
      if (e.getStatus().equals(Status.NOT_FOUND)) {
        return null;
      }
      throw e;
    }
  }

  private static Status invalidDigestSize(long digestSize, long contentSize) {
    return Status.INVALID_ARGUMENT.withDescription(
        String.format("digest size %d did not match content size %d", digestSize, contentSize));
  }

  public static ListenableFuture<Digest> putBlobFuture(
      Instance instance,
      Compressor.Value compressor,
      Digest digest,
      ByteString data,
      long writeDeadlineAfter,
      TimeUnit writeDeadlineAfterUnits,
      RequestMetadata requestMetadata) {
    if (digest.getSize() != data.size()) {
      return immediateFailedFuture(
          invalidDigestSize(digest.getSize(), data.size()).asRuntimeException());
    }
    Write write;
    try {
      write = instance.getBlobWrite(compressor, digest, UUID.randomUUID(), requestMetadata);
    } catch (EntryLimitException e) {
      return immediateFailedFuture(e);
    }
    ListenableFuture<Long> writtenFuture =
        WritesHelper.streamIntoWriteFuture(
            data::newInput, write, digest, writeDeadlineAfter, writeDeadlineAfterUnits);
    return transform(writtenFuture, committedSize -> digest, directExecutor());
  }

  public static Digest putBlob(
      Instance instance,
      Compressor.Value compressor,
      Digest digest,
      ByteString blob,
      long writeDeadlineAfter,
      TimeUnit writeDeadlineAfterUnits,
      RequestMetadata requestMetadata)
      throws IOException, InterruptedException, StatusException {
    try {
      return putBlobFuture(
              instance,
              compressor,
              digest,
              blob,
              writeDeadlineAfter,
              writeDeadlineAfterUnits,
              requestMetadata)
          .get();
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof IOException) {
        throw (IOException) cause;
      }
      Status status = Status.fromThrowable(cause);
      throw status.asException();
    }
  }
}
