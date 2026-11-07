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

import static build.buildfarm.common.ZstdCompressingInputStreamTest.compressibleBytes;
import static build.buildfarm.common.ZstdCompressingInputStreamTest.randomBytes;
import static com.google.common.truth.Truth.assertWithMessage;
import static java.util.concurrent.TimeUnit.SECONDS;

import build.buildfarm.common.ZstdDecompressingOutputStream.FixedBufferPool;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ZstdStreamsConcurrencyTest {
  private static final int THREADS = 8;
  private static final int BLOBS_PER_THREAD = 50;

  @Test
  public void concurrentCompressionAndDecompressionRoundTrips() throws Exception {
    // Concurrent streams sharing a bounded buffer pool reconstruct every blob.
    ExecutorService executor = Executors.newFixedThreadPool(THREADS);
    try (FixedBufferPool pool = new FixedBufferPool(2)) {
      List<Future<?>> futures = new ArrayList<>();
      for (int t = 0; t < THREADS; t++) {
        long seed = t;
        futures.add(
            executor.submit(
                () -> {
                  Random random = new Random(seed);
                  for (int i = 0; i < BLOBS_PER_THREAD; i++) {
                    int size = random.nextInt(400 * 1024);
                    byte[] blob =
                        random.nextBoolean()
                            ? compressibleBytes(random, size)
                            : randomBytes(random, size);
                    byte[] compressed = compress(blob, random);
                    byte[] decoded = decompress(compressed, random, pool);
                    assertWithMessage("thread %s blob %s", seed, i)
                        .that(Arrays.equals(decoded, blob))
                        .isTrue();
                  }
                  return null;
                }));
      }
      for (Future<?> future : futures) {
        future.get(60, SECONDS);
      }
    } finally {
      executor.shutdownNow();
    }
  }

  private static byte[] compress(byte[] blob, Random random) throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    byte[] buf = new byte[1 + random.nextInt(64 * 1024)];
    try (InputStream in = new ZstdCompressingInputStream(new ByteArrayInputStream(blob))) {
      for (int n = in.read(buf); n != -1; n = in.read(buf)) {
        out.write(buf, 0, n);
      }
    }
    return out.toByteArray();
  }

  private static byte[] decompress(byte[] compressed, Random random, FixedBufferPool pool)
      throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    int chunk = 1 + random.nextInt(64 * 1024);
    try (OutputStream zos = new ZstdDecompressingOutputStream(out, pool)) {
      for (int off = 0; off < compressed.length; off += chunk) {
        zos.write(compressed, off, Math.min(chunk, compressed.length - off));
      }
    }
    return out.toByteArray();
  }
}
