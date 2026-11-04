// Copyright 2026 The Buildfarm Authors. All rights reserved.
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

package build.buildfarm.common.blake3;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.nio.ByteBuffer;
import java.security.DigestException;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class Blake3MessageDigestTest {
  private record Vector(int length, String hash) {}

  /**
   * These hashes come from BLAKE3's test vectors:
   * https://github.com/BLAKE3-team/BLAKE3/blob/1.8.2/test_vectors/test_vectors.json
   *
   * These vectors cover various test cases, which is why they're here. Please see the details
   * in the BLAKE3 test_vectors.json file for more info.
   */
  private static final List<Vector> VECTORS =
      List.of(
          new Vector(0, "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262"),
          new Vector(1, "2d3adedff11b61f14c886e35afa036736dcd87a74d27b5c1510225d0f592e213"),
          new Vector(63, "e9bc37a594daad83be9470df7f7b3798297c3d834ce80ba85d6e207627b7db7b"),
          new Vector(64, "4eed7141ea4a5cd4b788606bd23f46e212af9cacebacdc7d1f4c6dc7f2511b98"),
          new Vector(65, "de1e5fa0be70df6d2be8fffd0e99ceaa8eb6e8c93a63f2d8d1c30ecb6b263dee"),
          new Vector(1023, "10108970eeda3eb932baac1428c7a2163b0e924c9a9e25b35bba72b28f70bd11"),
          new Vector(1024, "42214739f095a406f3fc83deb889744ac00df831c10daa55189b5d121c855af7"),
          new Vector(1025, "d00278ae47eb27b34faecf67b4fe263f82d5412916c1ffd97c8cb7fb814b8444"),
          new Vector(2048, "e776b6028c7cd22a4d0ba182a8bf62205d2ef576467e838ed6f2529b85fba24a"),
          new Vector(4096, "015094013f57a5277b59d8475c0501042c0b642e531b0a1c8f58d2163229e969"));

  private static byte[] input(int length) {
    byte[] data = new byte[length];
    for (int i = 0; i < length; i++) {
      data[i] = (byte) (i % 251);
    }
    return data;
  }

  private static void assertDigest(Vector vector, byte[] digest) {
    assertEquals(
        "input length " + vector.length(), vector.hash(), HexFormat.of().formatHex(digest));
  }

  @Test
  public void knownVectorsAndDigestReuse() {
    Blake3MessageDigest digest = new Blake3MessageDigest();
    for (Vector vector : VECTORS) {
      assertDigest(vector, digest.digest(input(vector.length())));
      assertDigest(VECTORS.getFirst(), digest.digest());
    }
  }

  @Test
  public void incrementalUpdatesWithOffsets() {
    for (Vector vector : VECTORS) {
      byte[] data = input(vector.length());
      byte[] padded = new byte[data.length + 7];
      System.arraycopy(data, 0, padded, 3, data.length);
      Blake3MessageDigest digest = new Blake3MessageDigest();
      for (int offset = 0; offset < data.length; offset += 63) {
        digest.update(padded, offset + 3, Math.min(63, data.length - offset));
      }
      assertDigest(vector, digest.digest());
    }
  }

  @Test
  public void byteUpdatesAndExplicitReset() {
    Blake3MessageDigest digest = new Blake3MessageDigest();
    for (Vector vector : VECTORS) {
      digest.update(input(65));
      digest.reset();
      for (byte value : input(vector.length())) {
        digest.update(value);
      }
      assertDigest(vector, digest.digest());
    }
  }

  @Test
  public void heapAndDirectByteBuffersRespectPositionAndLimit() {
    Vector vector = VECTORS.getLast();
    byte[] data = input(vector.length());
    for (ByteBuffer buffer :
        List.of(ByteBuffer.allocate(data.length + 7), ByteBuffer.allocateDirect(data.length + 7))) {
      buffer.position(3);
      buffer.put(data);
      buffer.limit(buffer.position());
      buffer.position(3);
      Blake3MessageDigest digest = new Blake3MessageDigest();
      digest.update(buffer);
      assertEquals(buffer.limit(), buffer.position());
      assertDigest(vector, digest.digest());
    }
  }

  @Test
  public void outputBufferPreservesSurroundingBytesAndResets() throws DigestException {
    Vector vector = VECTORS.getLast();
    Blake3MessageDigest digest = new Blake3MessageDigest();
    digest.update(input(vector.length()));
    byte[] output = new byte[48];
    Arrays.fill(output, (byte) 0x5a);
    assertEquals(32, digest.digest(output, 7, 32));
    assertDigest(vector, Arrays.copyOfRange(output, 7, 39));
    for (int i = 0; i < output.length; i++) {
      if (i < 7 || i >= 39) {
        assertEquals((byte) 0x5a, output[i]);
      }
    }
    assertDigest(VECTORS.getFirst(), digest.digest());
  }

  @Test
  public void shortOutputBufferDoesNotConsumeState() {
    Vector vector = VECTORS.getLast();
    Blake3MessageDigest digest = new Blake3MessageDigest();
    digest.update(input(vector.length()));
    assertThrows(DigestException.class, () -> digest.digest(new byte[31], 0, 31));
    assertThrows(IllegalArgumentException.class, () -> digest.digest(new byte[32], 1, 32));
    assertDigest(vector, digest.digest());
  }

  private static byte[] initializedState() {
    byte[] state = new byte[Blake3MessageDigest.hasher_size()];
    Blake3MessageDigest.initialize_hasher(state);
    return state;
  }

  @Test
  public void nativeFinalizationPreservesStateAndAllowsEmptyBuffers() {
    byte[] state = initializedState();
    Blake3MessageDigest.blake3_hasher_update(state, new byte[0], 0, 0);
    Blake3MessageDigest.blake3_hasher_finalize(state, new byte[0], 0);
    Vector vector = VECTORS.getLast();
    Blake3MessageDigest.blake3_hasher_update(state, input(vector.length()), 0, vector.length());
    byte[] savedState = state.clone();
    for (int i = 0; i < 2; i++) {
      byte[] output = new byte[32];
      Blake3MessageDigest.blake3_hasher_finalize(state, output, output.length);
      assertDigest(vector, output);
      assertArrayEquals(savedState, state);
    }
  }
}
