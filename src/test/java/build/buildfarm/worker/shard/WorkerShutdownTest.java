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
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertThrows;

import build.buildfarm.common.BuildfarmExecutors;
import build.buildfarm.common.ShardedCachedThreadPool;
import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.CallOptions;
import io.grpc.ManagedChannel;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.ServerServiceDefinition;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.ServerCalls;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class WorkerShutdownTest {
  private static final MethodDescriptor.Marshaller<String> STRING =
      new MethodDescriptor.Marshaller<>() {
        @Override
        public InputStream stream(String value) {
          return new ByteArrayInputStream(value.getBytes(UTF_8));
        }

        @Override
        public String parse(InputStream stream) {
          try {
            return new String(stream.readAllBytes(), UTF_8);
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        }
      };

  private static final MethodDescriptor<String, String> BLOCKING_CALL =
      MethodDescriptor.<String, String>newBuilder()
          .setType(MethodDescriptor.MethodType.UNARY)
          .setFullMethodName(MethodDescriptor.generateFullMethodName("test.Blocking", "Call"))
          .setRequestMarshaller(STRING)
          .setResponseMarshaller(STRING)
          .build();

  @Test
  public void shutdownServerAndExecutorTerminatesBoth() throws Exception {
    // With a call blocked in its handler, shutdown still terminates the server and its executor.
    ShardedCachedThreadPool executor = BuildfarmExecutors.getGrpcServerExecutor();
    CountDownLatch started = new CountDownLatch(1);
    AtomicReference<String> handlerThread = new AtomicReference<>();
    ServerServiceDefinition service =
        ServerServiceDefinition.builder("test.Blocking")
            .addMethod(
                BLOCKING_CALL,
                ServerCalls.asyncUnaryCall(
                    (request, responseObserver) -> {
                      handlerThread.set(Thread.currentThread().getName());
                      started.countDown();
                      try {
                        // Blocks until the executor's shutdownNow interrupts it.
                        new CountDownLatch(1).await();
                      } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                      }
                    }))
            .build();
    String name = InProcessServerBuilder.generateName();
    Server server =
        InProcessServerBuilder.forName(name).executor(executor).addService(service).build().start();
    ManagedChannel channel = InProcessChannelBuilder.forName(name).directExecutor().build();
    try {
      ListenableFuture<String> response =
          ClientCalls.futureUnaryCall(channel.newCall(BLOCKING_CALL, CallOptions.DEFAULT), "hi");
      assertThat(started.await(10, SECONDS)).isTrue();
      assertThat(handlerThread.get()).matches("grpc-server-executor-\\d+-\\d+");

      Worker.shutdownServerAndExecutor(server, executor, 1, SECONDS);

      assertThat(server.isTerminated()).isTrue();
      assertThat(executor.isShutdown()).isTrue();
      assertThat(executor.awaitTermination(10, SECONDS)).isTrue();
      // The in-flight call was cut off rather than completed.
      assertThrows(ExecutionException.class, () -> response.get(10, SECONDS));
    } finally {
      channel.shutdownNow();
      server.shutdownNow();
      executor.shutdownNow();
    }
  }
}
