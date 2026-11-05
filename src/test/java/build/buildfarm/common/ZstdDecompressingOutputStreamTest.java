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
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.junit.Assert.assertThrows;

import build.buildfarm.common.ZstdDecompressingOutputStream.FixedBufferPool;
import com.github.luben.zstd.Zstd;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Random;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ZstdDecompressingOutputStreamTest {
  private final FixedBufferPool pool = new FixedBufferPool(2);

  @After
  public void tearDown() {
    pool.close();
  }

  private static byte[] concat(byte[]... parts) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    for (byte[] part : parts) {
      out.writeBytes(part);
    }
    return out.toByteArray();
  }

  // Decompresses compressed by writing it in pieces split at each of the given offsets.
  private byte[] decompressInPieces(byte[] compressed, int... splits) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (OutputStream zos = new ZstdDecompressingOutputStream(out, pool)) {
      int start = 0;
      for (int split : splits) {
        zos.write(compressed, start, split - start);
        start = split;
      }
      zos.write(compressed, start, compressed.length - start);
    }
    return out.toByteArray();
  }

  @Test
  public void writeInChunksReconstructsInput() throws IOException {
    // Output is independent of how the compressed input is split across writes.
    Random random = new Random(10);
    byte[] first = compressibleBytes(random, 300 * 1024);
    byte[] second = randomBytes(random, 200 * 1024);
    byte[] firstFrame = Zstd.compress(first);
    byte[] secondFrame = Zstd.compress(second);

    // One-byte writes.
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (OutputStream zos = new ZstdDecompressingOutputStream(out, pool)) {
      for (byte b : firstFrame) {
        zos.write(b);
      }
    }
    assertThat(Arrays.equals(out.toByteArray(), first)).isTrue();

    // A split in the middle of a frame.
    assertThat(Arrays.equals(decompressInPieces(firstFrame, firstFrame.length / 2), first))
        .isTrue();

    // Concatenated frames, in one write and split across the frame boundary.
    byte[] both = concat(firstFrame, secondFrame);
    byte[] expected = concat(first, second);
    assertThat(Arrays.equals(decompressInPieces(both), expected)).isTrue();
    assertThat(Arrays.equals(decompressInPieces(both, firstFrame.length - 3), expected)).isTrue();
    assertThat(
            Arrays.equals(
                decompressInPieces(both, firstFrame.length / 2, firstFrame.length + 7), expected))
        .isTrue();

    // One write of a tiny frame that expands far beyond the output buffer.
    byte[] zeros = new byte[4 * 1024 * 1024];
    byte[] zerosFrame = Zstd.compress(zeros);
    assertWithMessage("compressed zeros size").that(zerosFrame.length).isLessThan(1024);
    assertThat(Arrays.equals(decompressInPieces(zerosFrame), zeros)).isTrue();
  }

  @Test
  public void closeWithTruncatedFrameDoesNotThrow() throws IOException {
    // A frame cut short is not an error at close: the stream decodes continuously across writes.
    byte[] input = compressibleBytes(new Random(12), 512 * 1024);
    byte[] frame = Zstd.compress(input);
    byte[] truncated = Arrays.copyOf(frame, frame.length - 10);

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    OutputStream zos = new ZstdDecompressingOutputStream(out, pool);
    zos.write(truncated);
    zos.close();

    byte[] decoded = out.toByteArray();
    assertThat(decoded.length).isLessThan(input.length);
    assertThat(Arrays.equals(decoded, Arrays.copyOf(input, decoded.length))).isTrue();
  }

  @Test
  public void writeCorruptInputThrowsIOException() throws IOException {
    // Input that is not a zstd frame fails with the checked exception expected by callers.
    byte[] garbage = randomBytes(new Random(11), 4096);
    try (OutputStream zos = new ZstdDecompressingOutputStream(new ByteArrayOutputStream(), pool)) {
      assertThrows(IOException.class, () -> zos.write(garbage));
    }
    assertThat(pool.getNumActive()).isEqualTo(0);
  }

  @Test
  public void closeTwiceReturnsBufferOnceAndRejectsWrites() throws IOException {
    // A stream holds one pooled buffer until close and returns it only once.
    OutputStream zos = new ZstdDecompressingOutputStream(new ByteArrayOutputStream(), pool);
    assertThat(pool.getNumActive()).isEqualTo(1);
    zos.write(Zstd.compress(new byte[1000]));
    zos.close();
    assertThat(pool.getNumActive()).isEqualTo(0);
    assertThat(pool.getNumIdle()).isEqualTo(1);
    zos.close();
    assertThat(pool.getNumActive()).isEqualTo(0);
    assertThat(pool.getNumIdle()).isEqualTo(1);
    assertThrows(IOException.class, () -> zos.write(1));
    assertThrows(IOException.class, () -> zos.write(new byte[1]));
  }

  @Test
  public void closeReturnsBufferWhenDownstreamThrows() throws IOException {
    // A failing downstream close propagates after the decompressor returns its buffer.
    IOException failure = new IOException("close failed");
    OutputStream downstream =
        new ByteArrayOutputStream() {
          @Override
          public void close() throws IOException {
            throw failure;
          }
        };
    OutputStream zos = new ZstdDecompressingOutputStream(downstream, pool);
    zos.write(Zstd.compress(new byte[1000]));
    IOException thrown = assertThrows(IOException.class, zos::close);
    assertThat(thrown).isSameInstanceAs(failure);

    assertThat(pool.getNumActive()).isEqualTo(0);
    assertThrows(IOException.class, () -> zos.write(new byte[1]));
  }
}
