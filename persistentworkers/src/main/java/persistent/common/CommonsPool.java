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

package persistent.common;

import java.io.IOException;
import java.time.Duration;
import java.util.NoSuchElementException;
import org.apache.commons.pool2.BaseKeyedPooledObjectFactory;
import org.apache.commons.pool2.impl.EvictionPolicy;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;

/**
 * Pool based on Apache Commons, ripped from Bazel as usual
 *
 * @param <K>
 * @param <V>
 */
public class CommonsPool<K, V> extends CommonsObjPool<K, V> {
  public CommonsPool(BaseKeyedPooledObjectFactory<K, V> factory, int maxPerKey) {
    super(factory, makeConfig(maxPerKey));
  }

  public CommonsPool(
      BaseKeyedPooledObjectFactory<K, V> factory,
      int maxPerKey,
      int maxTotal,
      int minIdlePerKey,
      Duration evictionInterval,
      EvictionPolicy<V> evictionPolicy) {
    super(factory, makeConfig(maxPerKey, maxTotal, minIdlePerKey, evictionInterval));
    setEvictionPolicy(evictionPolicy);
  }

  @Override
  public V borrowObject(K key) throws IOException, InterruptedException {
    try {
      return super.borrowObject(key);
    } catch (IOException | InterruptedException checkedException) {
      throw checkedException;
    } catch (NoSuchElementException e) {
      // Thrown when maxWait expires and no worker is available
      throw new IOException("Timed out waiting for a persistent worker from the pool", e);
    } catch (Throwable t) {
      throw new RuntimeException("unexpected@<borrowObject>", t);
    }
  }

  /** Bound only pool contention; creation/validation failures must not trigger native fallback. */
  public V obtain(K key, Duration maxWait) throws IOException, InterruptedException {
    try {
      return super.borrowObject(key, maxWait);
    } catch (NoSuchElementException e) {
      // Commons Pool also uses this exception for failed validation (even without a cause).
      // Only its two exhaustion cases are safe to retry through native execution.
      String message = e.getMessage();
      if (e.getCause() == null
          && !isClosed()
          && message != null
          && (message.startsWith("Timeout waiting for idle object")
              || message.startsWith("Pool exhausted"))) {
        throw new PoolExhaustedException(e);
      }
      throw new IOException("Unable to create or validate a persistent worker", e);
    } catch (IOException | InterruptedException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException("Unable to obtain a persistent worker", e);
    }
  }

  @Override
  public void invalidateObject(K key, V obj) throws IOException, InterruptedException {
    try {
      super.invalidateObject(key, obj);
    } catch (IOException | InterruptedException checkedException) {
      throw checkedException;
    } catch (Throwable t) {
      throw new RuntimeException("unexpected@<invalidateObject>", t);
    }
  }

  static <V> GenericKeyedObjectPoolConfig<V> makeConfig(int max) {
    return makeConfig(max, -1, max, Duration.ofMillis(-1));
  }

  static <V> GenericKeyedObjectPoolConfig<V> makeConfig(
      int maxPerKey, int maxTotal, int minIdlePerKey, Duration evictionInterval) {
    GenericKeyedObjectPoolConfig<V> config = new GenericKeyedObjectPoolConfig<>();

    // It's better to re-use a worker as often as possible and keep it hot, in order to profit
    // from JIT optimizations as much as possible.
    config.setLifo(true);

    // Configure per-key and pool-wide process limits.
    config.setMaxIdlePerKey(maxPerKey);
    config.setMaxTotalPerKey(maxPerKey);
    config.setMinIdlePerKey(minIdlePerKey);

    // A negative total preserves Commons Pool's unlimited behavior.
    config.setMaxTotal(maxTotal);

    // Wait for a worker to become ready when a thread needs one.
    config.setBlockWhenExhausted(true);

    // Always test the liveliness of worker processes.
    config.setTestOnBorrow(true);
    // Validate at borrow instead: Commons Pool turns failed create-time validation into
    // apparent exhaustion, which would incorrectly allow native fallback after a launch failure.
    config.setTestOnCreate(false);
    config.setTestOnReturn(true);

    // A negative interval disables eviction; otherwise inspect every idle object on each run.
    config.setTimeBetweenEvictionRuns(evictionInterval);
    if (!evictionInterval.isNegative()) {
      config.setNumTestsPerEvictionRun(-1);
    }
    return config;
  }
}
