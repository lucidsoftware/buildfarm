package build.buildfarm.common.services;

import static build.buildfarm.common.resources.ResourceParser.uploadResourceName;
import static com.google.common.truth.Truth.assertThat;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import build.bazel.remote.execution.v2.Compressor;
import build.bazel.remote.execution.v2.RequestMetadata;
import build.buildfarm.common.CASBackpressureException;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.HashFunction;
import build.buildfarm.common.Write;
import build.buildfarm.common.Write.WriteCompleteException;
import build.buildfarm.common.io.FeedbackOutputStream;
import build.buildfarm.common.resources.BlobInformation;
import build.buildfarm.common.resources.UploadBlobRequest;
import build.buildfarm.instance.Instance;
import build.buildfarm.v1test.Digest;
import com.google.bytestream.ByteStreamProto.WriteRequest;
import com.google.bytestream.ByteStreamProto.WriteResponse;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.ByteString;
import io.grpc.Context;
import io.grpc.Context.CancellableContext;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.stubbing.Answer;

@RunWith(JUnit4.class)
public class WriteStreamObserverTest {
  private static final DigestUtil DIGEST_UTIL = new DigestUtil(HashFunction.SHA256);

  @Test
  public void cancelledBeforeGetOutputIsSilent() throws Exception {
    CancellableContext context = Context.current().withCancellation();
    Instance instance = mock(Instance.class);
    ServerCallStreamObserver<WriteResponse> responseObserver = mock(ServerCallStreamObserver.class);
    ByteString cancelled = ByteString.copyFromUtf8("cancelled data");
    Digest cancelledDigest = DIGEST_UTIL.compute(cancelled);
    UUID uuid = UUID.randomUUID();
    UploadBlobRequest uploadBlobRequest =
        UploadBlobRequest.newBuilder()
            .setBlob(BlobInformation.newBuilder().setDigest(cancelledDigest))
            .setUuid(uuid.toString())
            .build();
    SettableFuture<Long> future = SettableFuture.create();
    Write write = mock(Write.class);
    when(write.getFuture()).thenReturn(future);
    FeedbackOutputStream out = mock(FeedbackOutputStream.class);
    doAnswer(
            (Answer<FeedbackOutputStream>)
                invocation -> {
                  context.cancel(new RuntimeException("Cancelled by test"));
                  return out;
                })
        .when(write)
        .getOutput(any(Long.class), any(Long.class), any(TimeUnit.class), any(Runnable.class));
    when(instance.getBlobWrite(
            eq(Compressor.Value.IDENTITY),
            eq(cancelledDigest),
            eq(uuid),
            any(RequestMetadata.class)))
        .thenReturn(write);

    WriteStreamObserver observer =
        context.call(
            () -> new WriteStreamObserver(instance, 1, SECONDS, () -> {}, responseObserver));
    observer.onNext(
        WriteRequest.newBuilder()
            .setResourceName(uploadResourceName(uploadBlobRequest))
            .setData(cancelled)
            .setFinishWrite(true)
            .build());
    verify(instance, times(1))
        .getBlobWrite(
            eq(Compressor.Value.IDENTITY),
            eq(cancelledDigest),
            eq(uuid),
            any(RequestMetadata.class));
    verify(write, times(1))
        .getOutput(any(Long.class), any(Long.class), any(TimeUnit.class), any(Runnable.class));
    // Cancellation listener closes out (releasing wrapper-held resources such as the zstd
    // decompressor's FixedBufferPool borrow) and then calls write.cancel (which on stub-backed
    // writes releases the gRPC observer's pendingWriteQueue). Closing first lets us free
    // wrapper state even for CASFileCache writes, whose cancel(String, Throwable) is the
    // interface default no-op.
    InOrder cancellation = inOrder(out, write);
    cancellation.verify(out).close();
    cancellation.verify(write).cancel(any(String.class), any(RuntimeException.class));
    verifyNoInteractions(responseObserver);
  }

  @Test
  public void noErrorWhenContextCancelled() throws Exception {
    CancellableContext context = Context.current().withCancellation();
    Instance instance = mock(Instance.class);
    ServerCallStreamObserver<WriteResponse> responseObserver = mock(ServerCallStreamObserver.class);
    ByteString cancelled = ByteString.copyFromUtf8("cancelled data");
    Digest cancelledDigest = DIGEST_UTIL.compute(cancelled);
    UUID uuid = UUID.randomUUID();
    UploadBlobRequest uploadBlobRequest =
        UploadBlobRequest.newBuilder()
            .setBlob(BlobInformation.newBuilder().setDigest(cancelledDigest))
            .setUuid(uuid.toString())
            .build();
    SettableFuture<Long> future = SettableFuture.create();
    Write write = mock(Write.class);
    when(write.getFuture()).thenReturn(future);
    when(write.isComplete()).thenReturn(Boolean.TRUE);
    when(instance.getBlobWrite(
            eq(Compressor.Value.IDENTITY),
            eq(cancelledDigest),
            eq(uuid),
            any(RequestMetadata.class)))
        .thenReturn(write);
    FeedbackOutputStream outputStream = mock(FeedbackOutputStream.class);
    when(write.getOutput(
            any(Long.class), any(Long.class), any(TimeUnit.class), any(Runnable.class)))
        .thenReturn(outputStream);

    WriteStreamObserver observer =
        context.call(
            () -> new WriteStreamObserver(instance, 1, SECONDS, () -> {}, responseObserver));
    context.run(
        () ->
            observer.onNext(
                WriteRequest.newBuilder()
                    .setResourceName(uploadResourceName(uploadBlobRequest))
                    .setData(cancelled)
                    .build()));
    context.cancel(new RuntimeException("Cancelled by test"));
    future.setException(new IOException("test cancel"));

    verify(instance, times(1))
        .getBlobWrite(
            eq(Compressor.Value.IDENTITY),
            eq(cancelledDigest),
            eq(uuid),
            any(RequestMetadata.class));
    verifyNoInteractions(responseObserver);
  }

  @Test
  public void writeFutureBackpressureMapsToResourceExhausted() throws Exception {
    // When the worker's charge() rejects a write under hard-cap backpressure, the write future
    // fails with a CASBackpressureException. errorResponse must remap it to the gRPC-canonical
    // RESOURCE_EXHAUSTED (not the UNKNOWN that Status.fromThrowable would yield) so Bazel reroutes.
    // Guards the write-path entry point against a refactor that drops the findInCauseChain remap.
    Instance instance = mock(Instance.class);
    ServerCallStreamObserver<WriteResponse> responseObserver = mock(ServerCallStreamObserver.class);
    ByteString data = ByteString.copyFromUtf8("backpressure data");
    Digest digest = DIGEST_UTIL.compute(data);
    UUID uuid = UUID.randomUUID();
    UploadBlobRequest uploadBlobRequest =
        UploadBlobRequest.newBuilder()
            .setBlob(BlobInformation.newBuilder().setDigest(digest))
            .setUuid(uuid.toString())
            .build();
    SettableFuture<Long> future = SettableFuture.create();
    Write write = mock(Write.class);
    when(write.getFuture()).thenReturn(future);
    when(instance.getBlobWrite(
            eq(Compressor.Value.IDENTITY), eq(digest), eq(uuid), any(RequestMetadata.class)))
        .thenReturn(write);
    FeedbackOutputStream outputStream = mock(FeedbackOutputStream.class);
    when(write.getOutput(
            any(Long.class), any(Long.class), any(TimeUnit.class), any(Runnable.class)))
        .thenReturn(outputStream);

    WriteStreamObserver observer =
        new WriteStreamObserver(instance, 1, SECONDS, () -> {}, responseObserver);
    observer.onNext(
        WriteRequest.newBuilder()
            .setResourceName(uploadResourceName(uploadBlobRequest))
            .setData(data)
            .build());
    future.setException(
        new CASBackpressureException(
            CASBackpressureException.Reason.HARD_CAP, 0, 0, 4096, 300, 0.0));

    ArgumentCaptor<Throwable> errorCaptor = ArgumentCaptor.forClass(Throwable.class);
    verify(responseObserver, times(1)).onError(errorCaptor.capture());
    assertThat(Status.fromThrowable(errorCaptor.getValue()).getCode())
        .isEqualTo(Status.Code.RESOURCE_EXHAUSTED);
  }

  @Test
  public void noWriteOnAlreadyCompleted() throws Exception {
    ByteString completed = ByteString.copyFromUtf8("Write already completed");
    Digest completedDigest = DIGEST_UTIL.compute(completed);
    UUID uuid = UUID.randomUUID();
    Instance instance = mock(Instance.class);
    Write write = mock(Write.class);
    SettableFuture<Long> future = SettableFuture.create();
    when(write.getFuture()).thenReturn(future);
    when(write.isComplete()).thenAnswer((Answer<Boolean>) invocation -> future.isDone());
    when(instance.getBlobWrite(
            eq(Compressor.Value.ZSTD), eq(completedDigest), eq(uuid), any(RequestMetadata.class)))
        .thenReturn(write);
    FeedbackOutputStream outputStream = mock(FeedbackOutputStream.class);
    when(write.getOutput(
            any(Long.class), any(Long.class), any(TimeUnit.class), any(Runnable.class)))
        .thenReturn(outputStream);
    ServerCallStreamObserver<WriteResponse> responseObserver = mock(ServerCallStreamObserver.class);

    // Mark write complete on getCommittedSize() call.
    doAnswer(
            invocation -> {
              long committed = Write.COMPRESSED_EXPECTED_SIZE;
              future.set(committed);
              return committed;
            })
        .when(write)
        .getCommittedSize();

    UploadBlobRequest uploadBlobRequest =
        UploadBlobRequest.newBuilder()
            .setBlob(
                BlobInformation.newBuilder()
                    .setCompressor(Compressor.Value.ZSTD)
                    .setDigest(completedDigest))
            .setUuid(uuid.toString())
            .build();
    WriteStreamObserver observer =
        new WriteStreamObserver(instance, 1, SECONDS, () -> {}, responseObserver);
    observer.onNext(
        WriteRequest.newBuilder()
            .setResourceName(uploadResourceName(uploadBlobRequest))
            .setData(completed)
            .setFinishWrite(true)
            .build());
    observer.onCompleted();

    // verify that write is not called on already completed write
    verify(outputStream, never()).write(completed.toByteArray());
    verify(responseObserver, times(1)).onNext(any(WriteResponse.class));
    verify(responseObserver, times(1)).request(Integer.MAX_VALUE);
    verify(responseObserver, times(1)).onCompleted();
    verify(responseObserver, never()).onError(any(Throwable.class));
  }

  @Test
  public void waitForFutureOnComplete() throws Exception {
    ByteString completed = ByteString.copyFromUtf8("Write already completed");
    Digest completedDigest = DIGEST_UTIL.compute(completed);
    SettableFuture<Long> future = SettableFuture.create();

    Write write = mock(Write.class);
    when(write.getFuture()).thenReturn(future);
    when(write.isComplete()).thenAnswer((Answer<Boolean>) invocation -> future.isDone());
    doAnswer(
            invocation -> {
              future.set(completedDigest.getSize());
              throw new WriteCompleteException();
            })
        .when(write)
        .getOutput(any(Long.class), any(Long.class), any(TimeUnit.class), any(Runnable.class));
    UUID uuid = UUID.randomUUID();
    Instance instance = mock(Instance.class);
    when(instance.getBlobWrite(
            eq(Compressor.Value.IDENTITY),
            eq(completedDigest),
            eq(uuid),
            any(RequestMetadata.class)))
        .thenReturn(write);

    UploadBlobRequest uploadBlobRequest =
        UploadBlobRequest.newBuilder()
            .setBlob(BlobInformation.newBuilder().setDigest(completedDigest))
            .setUuid(uuid.toString())
            .build();
    ServerCallStreamObserver<WriteResponse> responseObserver = mock(ServerCallStreamObserver.class);
    WriteStreamObserver observer =
        new WriteStreamObserver(instance, 1, SECONDS, () -> {}, responseObserver);
    observer.onNext(
        WriteRequest.newBuilder()
            .setResourceName(uploadResourceName(uploadBlobRequest))
            .setData(completed)
            .setFinishWrite(true)
            .build());

    verify(instance, times(1))
        .getBlobWrite(
            eq(Compressor.Value.IDENTITY),
            eq(completedDigest),
            eq(uuid),
            any(RequestMetadata.class));
    verify(write, atLeastOnce()).getFuture();
    verify(write, times(1))
        .getOutput(any(Long.class), any(Long.class), any(TimeUnit.class), any(Runnable.class));
    verify(responseObserver, times(1)).onNext(any(WriteResponse.class));
    verify(responseObserver, times(1)).request(Integer.MAX_VALUE);
    verify(responseObserver, times(1)).onCompleted();
    verifyNoMoreInteractions(responseObserver);
  }

  @Test
  public void commitActiveDrainsInboundBeforeOnCompleted() throws Exception {
    // Verifies that on commit, commitActive sends onNext, then requests Integer.MAX_VALUE
    // inbound credit (so any in-flight DATA frames from the client get deframed and
    // WINDOW_UPDATEs flow back), and only then sends onCompleted.
    ByteString committed = ByteString.copyFromUtf8("committed data");
    Digest committedDigest = DIGEST_UTIL.compute(committed);
    UUID uuid = UUID.randomUUID();
    Instance instance = mock(Instance.class);
    Write write = mock(Write.class);
    SettableFuture<Long> future = SettableFuture.create();
    when(write.getFuture()).thenReturn(future);
    when(write.isComplete()).thenAnswer((Answer<Boolean>) invocation -> future.isDone());
    when(instance.getBlobWrite(
            eq(Compressor.Value.IDENTITY),
            eq(committedDigest),
            eq(uuid),
            any(RequestMetadata.class)))
        .thenReturn(write);
    FeedbackOutputStream outputStream = mock(FeedbackOutputStream.class);
    when(write.getOutput(
            any(Long.class), any(Long.class), any(TimeUnit.class), any(Runnable.class)))
        .thenReturn(outputStream);
    ServerCallStreamObserver<WriteResponse> responseObserver = mock(ServerCallStreamObserver.class);

    // Trip commit when getCommittedSize is queried during the first onNext.
    doAnswer(
            invocation -> {
              long size = committedDigest.getSize();
              future.set(size);
              return size;
            })
        .when(write)
        .getCommittedSize();

    UploadBlobRequest uploadBlobRequest =
        UploadBlobRequest.newBuilder()
            .setBlob(BlobInformation.newBuilder().setDigest(committedDigest))
            .setUuid(uuid.toString())
            .build();
    WriteStreamObserver observer =
        new WriteStreamObserver(instance, 1, SECONDS, () -> {}, responseObserver);
    observer.onNext(
        WriteRequest.newBuilder()
            .setResourceName(uploadResourceName(uploadBlobRequest))
            .setData(committed)
            .setFinishWrite(true)
            .build());

    // The drain credit must be issued BEFORE onCompleted — without it, no further
    // WINDOW_UPDATEs flow to the client and its pendingWriteQueue stays stuck.
    InOrder inOrder = inOrder(responseObserver);
    inOrder.verify(responseObserver).onNext(any(WriteResponse.class));
    inOrder.verify(responseObserver).request(Integer.MAX_VALUE);
    inOrder.verify(responseObserver).onCompleted();
  }
}
