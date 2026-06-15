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

package build.buildfarm.common;

import static com.google.common.truth.Truth.assertThat;

import build.buildfarm.common.CASBackpressureException.Reason;
import io.grpc.Status;
import java.io.IOException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class CASBackpressureExceptionTest {
  @Test
  public void hardCapReasonShouldMapToResourceExhausted() {
    CASBackpressureException e =
        new CASBackpressureException(
            Reason.HARD_CAP, 3, 215_400_000_000L, 216_000_000_000L, 300, 0.0);
    assertThat(e.toStatus().getCode()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED);
    assertThat(e.toStatus().getCause()).isSameInstanceAs(e);
  }

  @Test
  public void shardDeadReasonShouldMapToUnavailable() {
    CASBackpressureException e =
        new CASBackpressureException(Reason.SHARD_DEAD, 1, 0, 216_000_000_000L, 0, 0.0);
    assertThat(e.toStatus().getCode()).isEqualTo(Status.Code.UNAVAILABLE);
  }

  @Test
  public void hardCapMessageShouldCarryStructuredFields() {
    CASBackpressureException e =
        new CASBackpressureException(
            Reason.HARD_CAP, 7, 215_400_000_000L, 216_000_000_000L, 300, 0.0);
    String message = e.getMessage();
    assertThat(message).contains("cas charge backpressure");
    assertThat(message).contains("shard=7");
    assertThat(message).contains("parkSeconds=300");
    assertThat(message).contains("shardBytes=215.4GB");
    assertThat(message).contains("shardCap=216.0GB");
    assertThat(message).contains("evictionRate=0/s");
    assertThat(message).contains("try another worker");
  }

  @Test
  public void fractionalEvictionRateShouldRenderWithOneDecimal() {
    CASBackpressureException e =
        new CASBackpressureException(Reason.HARD_CAP, 0, 0, 4096, 300, 12.5);
    assertThat(e.getMessage()).contains("evictionRate=12.5/s");
  }

  @Test
  public void shardDeadMessageShouldNameTheShardAndRedeploy() {
    CASBackpressureException e =
        new CASBackpressureException(Reason.SHARD_DEAD, 5, 0, 4096, 0, 0.0);
    assertThat(e.getMessage())
        .isEqualTo("cas shard 5 unavailable: evictor terminated; redeploy required");
  }

  @Test
  public void findInCauseChainShouldReturnDirectInstance() {
    CASBackpressureException e =
        new CASBackpressureException(Reason.HARD_CAP, 0, 0, 4096, 300, 0.0);
    assertThat(CASBackpressureException.findInCauseChain(e)).isSameInstanceAs(e);
  }

  @Test
  public void findInCauseChainShouldUnwrapNestedCause() {
    CASBackpressureException backpressure =
        new CASBackpressureException(Reason.HARD_CAP, 0, 0, 4096, 300, 0.0);
    // The synchronous handleRequest catch path wraps the backpressure exception as a
    // Status.UNKNOWN exception cause; the chain walk must still find it.
    Throwable wrapped = Status.fromThrowable(backpressure).asException();
    assertThat(CASBackpressureException.findInCauseChain(wrapped)).isSameInstanceAs(backpressure);
    // And one level deeper.
    Throwable doubleWrapped = new RuntimeException("outer", wrapped);
    assertThat(CASBackpressureException.findInCauseChain(doubleWrapped))
        .isSameInstanceAs(backpressure);
  }

  @Test
  public void findInCauseChainShouldReturnNullForUnrelatedThrowable() {
    assertThat(CASBackpressureException.findInCauseChain(null)).isNull();
    assertThat(CASBackpressureException.findInCauseChain(new IOException("unrelated"))).isNull();
  }

  @Test(timeout = 5000)
  public void findInCauseChainShouldTerminateOnCyclicCause() {
    // A pathological 2-node cause cycle (constructible via initCause) must terminate rather than
    // spin forever. Neither node is a backpressure exception, so the bounded walk returns null.
    IOException a = new IOException("a");
    IOException b = new IOException("b");
    b.initCause(a);
    a.initCause(b);
    assertThat(CASBackpressureException.findInCauseChain(a)).isNull();
  }
}
