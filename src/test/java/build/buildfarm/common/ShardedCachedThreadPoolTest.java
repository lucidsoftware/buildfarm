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
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertThrows;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ShardedCachedThreadPoolTest {
  private ShardedCachedThreadPool pool;

  @After
  public void tearDown() throws InterruptedException {
    if (pool != null) {
      pool.shutdownNow();
      assertThat(pool.awaitTermination(10, SECONDS)).isTrue();
    }
  }

  @Test
  public void shardCountRoundsUpWithAtLeastOneShard() {
    // One shard per 8 processors, rounded up; non-positive counts still get one shard.
    assertThat(ShardedCachedThreadPool.calculateShardCount(1)).isEqualTo(1);
    assertThat(ShardedCachedThreadPool.calculateShardCount(8)).isEqualTo(1);
    assertThat(ShardedCachedThreadPool.calculateShardCount(9)).isEqualTo(2);
    assertThat(ShardedCachedThreadPool.calculateShardCount(16)).isEqualTo(2);
    assertThat(ShardedCachedThreadPool.calculateShardCount(96)).isEqualTo(12);
    assertThat(ShardedCachedThreadPool.calculateShardCount(192)).isEqualTo(24);
    assertThat(ShardedCachedThreadPool.calculateShardCount(0)).isEqualTo(1);
    assertThat(ShardedCachedThreadPool.calculateShardCount(-1)).isEqualTo(1);
  }

  @Test
  public void blockedTasksNeverStarve() throws InterruptedException {
    // Every task starts even while all earlier ones are blocked: no shard queues work.
    pool = new ShardedCachedThreadPool("test", 4);
    int tasks = 64;
    CountDownLatch started = new CountDownLatch(tasks);
    CountDownLatch release = new CountDownLatch(1);
    CountDownLatch finished = new CountDownLatch(tasks);
    for (int i = 0; i < tasks; i++) {
      pool.execute(
          () -> {
            started.countDown();
            try {
              release.await();
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
            finished.countDown();
          });
    }
    assertThat(started.await(10, SECONDS)).isTrue();
    release.countDown();
    assertThat(finished.await(10, SECONDS)).isTrue();
  }

  @Test
  public void executeUsesEveryShard() throws InterruptedException {
    // Random placement reaches every shard; a false failure has probability below 1e-1200.
    pool = new ShardedCachedThreadPool("spread", 4);
    int tasks = 10_000;
    Set<String> shards = ConcurrentHashMap.newKeySet();
    CountDownLatch done = new CountDownLatch(tasks);
    for (int i = 0; i < tasks; i++) {
      pool.execute(
          () -> {
            String name = Thread.currentThread().getName();
            shards.add(name.substring(0, name.lastIndexOf('-')));
            done.countDown();
          });
    }
    assertThat(done.await(10, SECONDS)).isTrue();
    assertThat(shards).containsExactly("spread-0", "spread-1", "spread-2", "spread-3");
  }

  @Test
  public void threadsAreDaemonAndNamedByShard() throws InterruptedException {
    // Threads are daemon, like grpc's default pool, and named <prefix>-<shard>-<n>.
    pool = new ShardedCachedThreadPool("grpc-server-executor", 2);
    Set<Thread> threads = ConcurrentHashMap.newKeySet();
    CountDownLatch done = new CountDownLatch(100);
    for (int i = 0; i < 100; i++) {
      pool.execute(
          () -> {
            threads.add(Thread.currentThread());
            done.countDown();
          });
    }
    assertThat(done.await(10, SECONDS)).isTrue();
    assertThat(threads).isNotEmpty();
    for (Thread thread : threads) {
      assertThat(thread.isDaemon()).isTrue();
      assertThat(thread.getName()).matches("grpc-server-executor-[01]-\\d+");
    }
  }

  @Test
  public void shutdownRejectsNewTasksAndTerminatesOnlyAfterRunningTasksFinish()
      throws InterruptedException {
    // shutdown lets running tasks finish on every shard before the pool terminates.
    pool = new ShardedCachedThreadPool("shutdown", 4);
    int tasks = 16;
    CountDownLatch started = new CountDownLatch(tasks);
    CountDownLatch release = new CountDownLatch(1);
    AtomicBoolean interrupted = new AtomicBoolean(false);
    for (int i = 0; i < tasks; i++) {
      pool.execute(
          () -> {
            started.countDown();
            try {
              release.await();
            } catch (InterruptedException e) {
              interrupted.set(true);
            }
          });
    }
    assertThat(started.await(10, SECONDS)).isTrue();

    pool.shutdown();
    assertThat(pool.isShutdown()).isTrue();
    assertThrows(RejectedExecutionException.class, () -> pool.execute(() -> {}));
    // While the tasks are blocked, the pool cannot terminate.
    assertThat(pool.awaitTermination(10, MILLISECONDS)).isFalse();
    assertThat(pool.isTerminated()).isFalse();

    release.countDown();
    assertThat(pool.awaitTermination(10, SECONDS)).isTrue();
    assertThat(pool.isTerminated()).isTrue();
    assertThat(interrupted.get()).isFalse();
  }

  @Test
  public void shutdownNowInterruptsRunningTasks() throws InterruptedException {
    // shutdownNow interrupts the blocked tasks on every shard, so the pool terminates.
    pool = new ShardedCachedThreadPool("shutdown-now", 4);
    int tasks = 16;
    CountDownLatch started = new CountDownLatch(tasks);
    CountDownLatch interrupted = new CountDownLatch(tasks);
    CountDownLatch never = new CountDownLatch(1);
    for (int i = 0; i < tasks; i++) {
      pool.execute(
          () -> {
            started.countDown();
            try {
              never.await();
            } catch (InterruptedException e) {
              interrupted.countDown();
            }
          });
    }
    assertThat(started.await(10, SECONDS)).isTrue();
    assertThat(pool.awaitTermination(10, MILLISECONDS)).isFalse();

    assertThat(pool.shutdownNow()).isEmpty();
    assertThat(interrupted.await(10, SECONDS)).isTrue();
    assertThat(pool.awaitTermination(10, SECONDS)).isTrue();
  }
}
