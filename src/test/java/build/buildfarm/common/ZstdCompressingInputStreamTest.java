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
import static org.junit.Assert.assertThrows;

import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdInputStreamNoFinalizer;
import com.google.common.io.ByteStreams;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ZstdCompressingInputStreamTest {
  private static final int BLOCK = 128 * 1024;
  private static final int[] SIZES = {0, 1, 491, 492, BLOCK - 1, BLOCK, BLOCK + 1, 4 * 1024 * 1024};

  private enum ReadMode {
    SINGLE_BYTE,
    SMALL_BUFFER,
    LARGE_BUFFER,
  }

  static byte[] randomBytes(Random random, int size) {
    byte[] data = new byte[size];
    random.nextBytes(data);
    return data;
  }

  // Runs of a small alphabet: compresses well, but still exercises the match finder.
  static byte[] compressibleBytes(Random random, int size) {
    byte[] data = new byte[size];
    int i = 0;
    while (i < size) {
      byte b = (byte) ('a' + random.nextInt(4));
      int run = 1 + random.nextInt(16);
      for (int j = 0; j < run && i < size; j++) {
        data[i++] = b;
      }
    }
    return data;
  }

  private static byte[] readAll(InputStream in, ReadMode mode) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    switch (mode) {
      case SINGLE_BYTE:
        for (int b = in.read(); b != -1; b = in.read()) {
          out.write(b);
        }
        break;
      case SMALL_BUFFER:
        copy(in, out, new byte[7]);
        break;
      case LARGE_BUFFER:
        copy(in, out, new byte[8 * 1024]);
        break;
    }
    return out.toByteArray();
  }

  private static void copy(InputStream in, ByteArrayOutputStream out, byte[] buf)
      throws IOException {
    for (int n = in.read(buf); n != -1; n = in.read(buf)) {
      out.write(buf, 0, n);
    }
  }

  static void assertSingleFrameOf(byte[] compressed, byte[] expected) throws IOException {
    assertThat(Zstd.findFrameCompressedSize(compressed)).isEqualTo(compressed.length);
    assertThat(Arrays.equals(Zstd.decompress(compressed, expected.length), expected)).isTrue();
    try (InputStream zis = new ZstdInputStreamNoFinalizer(new ByteArrayInputStream(compressed))) {
      assertThat(Arrays.equals(ByteStreams.toByteArray(zis), expected)).isTrue();
    }
  }

  @Test
  public void readsOfVariousSizesRoundTripAsSingleFrame() throws IOException {
    // Every size, data shape and read style yields exactly one standard frame of the input.
    Random random = new Random(1);
    for (int size : SIZES) {
      byte[][] inputs = {randomBytes(random, size), compressibleBytes(random, size)};
      for (byte[] input : inputs) {
        for (ReadMode mode : ReadMode.values()) {
          byte[] compressed;
          try (InputStream in = new ZstdCompressingInputStream(new ByteArrayInputStream(input))) {
            compressed = readAll(in, mode);
          }
          try {
            assertSingleFrameOf(compressed, input);
          } catch (AssertionError e) {
            throw new AssertionError("size " + size + " mode " + mode, e);
          }
        }
      }
    }
  }

  @Test
  public void readAfterEofReturnsMinusOneRepeatedly() throws IOException {
    // Once the frame has been drained, every further read reports EOF; zero-length reads report 0.
    byte[] input = compressibleBytes(new Random(2), 1000);
    try (InputStream in = new ZstdCompressingInputStream(new ByteArrayInputStream(input))) {
      assertSingleFrameOf(readAll(in, ReadMode.LARGE_BUFFER), input);
      byte[] buf = new byte[16];
      for (int i = 0; i < 3; i++) {
        assertThat(in.read()).isEqualTo(-1);
        assertThat(in.read(buf)).isEqualTo(-1);
        assertThat(in.read(buf, 3, 5)).isEqualTo(-1);
        assertThat(in.read(buf, 3, 0)).isEqualTo(0);
      }
    }
  }

  // Reads exactly n bytes, or to EOF if that comes first.
  private static byte[] readN(InputStream in, int n) throws IOException {
    byte[] buf = new byte[n];
    int count = 0;
    while (count < n) {
      int r = in.read(buf, count, n - count);
      if (r == -1) {
        return Arrays.copyOf(buf, count);
      }
      count += r;
    }
    return buf;
  }

  @Test
  public void readResumedOnFreshThreadAtEachOffsetRoundTrips() throws Exception {
    // A stream whose last reader thread has exited can still be read by another thread.
    byte[] input = randomBytes(new Random(4), 8 * 1024);
    for (int k = 1; k <= 1024; k++) {
      try (InputStream in = new ZstdCompressingInputStream(new ByteArrayInputStream(input))) {
        final int prefixLength = k;
        byte[][] prefix = new byte[1][];
        Throwable[] failure = new Throwable[1];
        // Start/join a fresh thread: a pooled thread would stay alive and hide the bug.
        Thread reader =
            new Thread(
                () -> {
                  try {
                    prefix[0] = readN(in, prefixLength);
                  } catch (Throwable t) {
                    failure[0] = t;
                  }
                });
        reader.start();
        reader.join();
        if (failure[0] != null) {
          throw new AssertionError("prefix read failed at offset " + k, failure[0]);
        }
        byte[] rest;
        try {
          rest = readAll(in, ReadMode.LARGE_BUFFER);
        } catch (IOException e) {
          throw new AssertionError("resumed read failed at offset " + k, e);
        }
        assertSingleFrameOf(concat(prefix[0], rest), input);
      }
    }
  }

  private static byte[] concat(byte[] a, byte[] b) {
    byte[] result = Arrays.copyOf(a, a.length + b.length);
    System.arraycopy(b, 0, result, a.length, b.length);
    return result;
  }

  @Test
  public void closeBeforeEofClosesSource() throws IOException {
    // Abandoning an unfinished frame still closes the underlying source.
    AtomicInteger closes = new AtomicInteger();
    InputStream source =
        new ByteArrayInputStream(randomBytes(new Random(7), 3 * BLOCK)) {
          @Override
          public void close() {
            closes.incrementAndGet();
          }
        };
    InputStream in = new ZstdCompressingInputStream(source);
    assertThat(readN(in, 1024)).hasLength(1024);
    in.close();
    assertThat(closes.get()).isEqualTo(1);
    // Bazel permits repeated close but does not promise exceptions on reads after close.
    in.close();
  }

  @Test
  public void interleavedStreamsRoundTripIndependently() throws IOException {
    // Two open streams read interleaved on one thread both decode.
    Random random = new Random(8);
    byte[] a = compressibleBytes(random, 3 * BLOCK);
    byte[] b = randomBytes(random, 3 * BLOCK);
    ByteArrayOutputStream outA = new ByteArrayOutputStream();
    ByteArrayOutputStream outB = new ByteArrayOutputStream();
    try (InputStream inA = new ZstdCompressingInputStream(new ByteArrayInputStream(a));
        InputStream inB = new ZstdCompressingInputStream(new ByteArrayInputStream(b))) {
      byte[] buf = new byte[7];
      boolean doneA = false;
      boolean doneB = false;
      while (!doneA || !doneB) {
        doneA = doneA || copyOnce(inA, outA, buf);
        doneB = doneB || copyOnce(inB, outB, buf);
      }
    }
    assertSingleFrameOf(outA.toByteArray(), a);
    assertSingleFrameOf(outB.toByteArray(), b);
  }

  // Copies one read's worth of bytes, returning true at EOF.
  private static boolean copyOnce(InputStream in, ByteArrayOutputStream out, byte[] buf)
      throws IOException {
    int n = in.read(buf);
    if (n == -1) {
      return true;
    }
    out.write(buf, 0, n);
    return false;
  }

  @Test
  public void underlyingIOExceptionIsPropagatedAndSourceIsClosed() throws IOException {
    // A failing source surfaces its IOException and is still closed by the wrapper.
    IOException failure = new IOException("disk on fire");
    AtomicInteger closes = new AtomicInteger();
    byte[] prefix = randomBytes(new Random(9), 3 * BLOCK);
    InputStream failing =
        new SequenceInputStream(
            new ByteArrayInputStream(prefix),
            new InputStream() {
              @Override
              public int read() throws IOException {
                throw failure;
              }

              @Override
              public int read(byte[] b, int off, int len) throws IOException {
                throw failure;
              }

              @Override
              public void close() {
                closes.incrementAndGet();
              }
            });
    InputStream in = new ZstdCompressingInputStream(failing);
    IOException thrown = assertThrows(IOException.class, () -> readAll(in, ReadMode.LARGE_BUFFER));
    assertThat(thrown).isSameInstanceAs(failure);
    in.close();
    assertThat(closes.get()).isEqualTo(1);
  }
}
