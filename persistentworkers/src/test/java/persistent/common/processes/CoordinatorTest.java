// Copyright 2023-2025 The Buildfarm Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package persistent.common.processes;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import persistent.common.Coordinator;
import persistent.common.Coordinator.SimpleCoordinator;
import persistent.common.CtxAround.Id;
import persistent.common.MapPool;
import persistent.common.ObjectPool;
import persistent.common.Worker;

@RunWith(JUnit4.class)
public class CoordinatorTest {
  @SuppressWarnings("CheckReturnValue")
  @Test
  public void simpleTestWorks() throws Exception {
    // Creates an objectpool that uses Strings as a Key for its Workers
    // Workers increment an integer and returns its string value.
    ObjectPool<String, Worker<Integer, String>> spool =
        new MapPool<>(
            key ->
                new Worker<Integer, String>() {
                  @Override
                  public String doWork(Integer request) {
                    return String.valueOf(request + 1);
                  }
                });

    SimpleCoordinator<String, Integer, String, Worker<Integer, String>> pc =
        Coordinator.simple(spool);

    assertThat(pc.runRequest("someWorkerKey", Id.of(1))).isEqualTo(Id.of("2"));
  }

  private static final String THROWING_WORKER_ERROR_MESSAGE = "simulated worker failure";

  /** Worker that throws a RuntimeException from doWork and tracks whether it's been destroyed */
  private static class ThrowingWorker implements Worker<Integer, String> {
    boolean destroyed = false;

    @Override
    public String doWork(Integer request) {
      throw new RuntimeException(THROWING_WORKER_ERROR_MESSAGE);
    }

    @Override
    public void destroy() {
      destroyed = true;
    }
  }

  @Test
  public void doWork_exception_invalidatesAndDestroysWorker() throws Exception {
    ThrowingWorker worker = new ThrowingWorker();
    ObjectPool<String, Worker<Integer, String>> pool = new MapPool<>(key -> worker);

    SimpleCoordinator<String, Integer, String, Worker<Integer, String>> coordinator =
        Coordinator.simple(pool);

    RuntimeException thrown =
        assertThrows(RuntimeException.class, () -> coordinator.runRequest("key", Id.of(42)));

    assertThat(thrown.getMessage()).isEqualTo(THROWING_WORKER_ERROR_MESSAGE);
    assertThat(worker.destroyed).isTrue();
  }

  @Test
  public void doWork_exception_destroysWorkerWhenInvalidationFails() throws Exception {
    ThrowingWorker worker = new ThrowingWorker();
    RuntimeException invalidateFailure = new RuntimeException("pool invalidation failed");

    ObjectPool<String, Worker<Integer, String>> pool =
        new ObjectPool<>() {
          @Override
          public Worker<Integer, String> obtain(String key) {
            return worker;
          }

          @Override
          public void release(String key, Worker<Integer, String> obj) {}

          @Override
          public void invalidate(String key, Worker<Integer, String> obj) throws Exception {
            throw invalidateFailure;
          }
        };

    SimpleCoordinator<String, Integer, String, Worker<Integer, String>> coordinator =
        Coordinator.simple(pool);

    RuntimeException thrown =
        assertThrows(RuntimeException.class, () -> coordinator.runRequest("key", Id.of(42)));

    assertThat(thrown.getMessage()).isEqualTo(THROWING_WORKER_ERROR_MESSAGE);
    assertThat(worker.destroyed).isTrue();
    assertThat(thrown.getSuppressed()).asList().contains(invalidateFailure);
  }
}
