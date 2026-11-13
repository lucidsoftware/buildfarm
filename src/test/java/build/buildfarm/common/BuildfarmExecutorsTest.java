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
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertThrows;

import com.google.common.util.concurrent.ListeningExecutorService;
import java.util.concurrent.CountDownLatch;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class BuildfarmExecutorsTest {
  @Test
  public void transformServicePoolRunsConfiguredNumberOfTasksConcurrently() throws Exception {
    int nThreads = 3;
    ListeningExecutorService pool = BuildfarmExecutors.getTransformServicePool(nThreads);
    try {
      CountDownLatch allStarted = new CountDownLatch(nThreads);
      CountDownLatch release = new CountDownLatch(1);
      for (int i = 0; i < nThreads; i++) {
        pool.execute(
            () -> {
              allStarted.countDown();
              try {
                release.await();
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
            });
      }
      // Only succeeds if the pool has at least nThreads threads.
      assertThat(allStarted.await(10, SECONDS)).isTrue();
      release.countDown();
    } finally {
      pool.shutdownNow();
    }
  }

  @Test
  public void transformServicePoolRejectsNonPositiveSize() {
    assertThrows(
        IllegalArgumentException.class, () -> BuildfarmExecutors.getTransformServicePool(0));
    assertThrows(
        IllegalArgumentException.class, () -> BuildfarmExecutors.getTransformServicePool(-1));
  }
}
