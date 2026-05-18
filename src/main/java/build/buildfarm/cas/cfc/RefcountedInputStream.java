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

package build.buildfarm.cas.cfc;

import static com.google.common.base.Throwables.throwIfUnchecked;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Wraps a CAS-backed {@link InputStream} so that the refcount taken by {@code tryAcquire} on open
 * is released exactly once when the stream is closed. Phase 1 invariant: every read path that opens
 * a CAS file under a digest must hold a refcount for the full file-descriptor lifetime — POSIX
 * unlink semantics protect an already-opened fd from concurrent eviction, but the refcount
 * discipline prevents eviction from picking the entry as a victim while a reader's fd is still in
 * use.
 */
final class RefcountedInputStream extends FilterInputStream {
  private final Runnable releaseRef;
  private final AtomicBoolean closed = new AtomicBoolean(false);

  RefcountedInputStream(InputStream in, Runnable releaseRef) {
    super(in);
    this.releaseRef = releaseRef;
  }

  @Override
  public void close() throws IOException {
    // Capture super.close()'s throw explicitly so a throw from releaseRef.run() (e.g.
    // IllegalStateException on refcount underflow) does not replace the close error the
    // caller cares about — without addSuppressed, the try/finally semantics would silently
    // drop the original. Catch Exception (not Throwable) on both sites so a non-IOException
    // RuntimeException from a wrapped stream's close (e.g. a JNI-backed compressor) does not
    // skip the refcount release or drop the close error. An Error (OutOfMemoryError,
    // LinkageError, ...) is deliberately NOT caught: it propagates immediately, skipping the
    // refcount release. We make no attempt to clean up after an unrecoverable error — the JVM
    // state is undefined and letting it die is the intended behavior.
    Exception pending = null;
    try {
      super.close();
    } catch (Exception t) {
      pending = t;
    }
    if (closed.compareAndSet(false, true)) {
      try {
        releaseRef.run();
      } catch (Exception e) {
        if (pending == null) {
          pending = e;
        } else {
          pending.addSuppressed(e);
        }
      }
    }
    if (pending != null) {
      throwIfUnchecked(pending);
      throw (IOException) pending;
    }
  }
}
