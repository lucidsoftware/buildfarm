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

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * An unbounded cached thread pool split into independent shards. Each shard is a plain {@link
 * Executors#newCachedThreadPool}.
 *
 * <p>This exists to prevent concurrency bottlenecks on machines with a very large number of
 * processors. The default {@link Executors#newCachedThreadPool} uses a SynchronousQueue, which can
 * result in cache line bouncing problems on machines with enough cores.
 */
public final class ShardedCachedThreadPool extends AbstractExecutorService {
  private static final int PROCESSORS_PER_SHARD = 8;

  private final ExecutorService[] shards;

  /** Creates a pool sized based on the numver of available processors. */
  public ShardedCachedThreadPool(String namePrefix) {
    this(namePrefix, calculateShardCount(SystemProcessors.get()));
  }

  ShardedCachedThreadPool(String namePrefix, int shardCount) {
    checkArgument(shardCount > 0, "shardCount must be positive: %s", shardCount);
    shards = new ExecutorService[shardCount];
    for (int i = 0; i < shardCount; i++) {
      shards[i] =
          Executors.newCachedThreadPool(
              new ThreadFactoryBuilder()
                  .setNameFormat(namePrefix + "-" + i + "-%d")
                  .setDaemon(true)
                  .build());
    }
  }

  /** One shard per 8 processors, rounded up. There's always at least one shard. */
  public static int calculateShardCount(int processors) {
    return Math.max(1, Math.ceilDiv(processors, PROCESSORS_PER_SHARD));
  }

  public int getShardCount() {
    return shards.length;
  }

  @Override
  public void execute(Runnable command) {
    shards[ThreadLocalRandom.current().nextInt(shards.length)].execute(command);
  }

  @Override
  public void shutdown() {
    for (ExecutorService shard : shards) {
      shard.shutdown();
    }
  }

  @Override
  public List<Runnable> shutdownNow() {
    List<Runnable> pending = new ArrayList<>();
    for (ExecutorService shard : shards) {
      pending.addAll(shard.shutdownNow());
    }
    return pending;
  }

  @Override
  public boolean isShutdown() {
    for (ExecutorService shard : shards) {
      if (!shard.isShutdown()) {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean isTerminated() {
    for (ExecutorService shard : shards) {
      if (!shard.isTerminated()) {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
    long deadline = System.nanoTime() + unit.toNanos(timeout);
    for (ExecutorService shard : shards) {
      if (!shard.awaitTermination(deadline - System.nanoTime(), NANOSECONDS)) {
        return false;
      }
    }
    return true;
  }
}
