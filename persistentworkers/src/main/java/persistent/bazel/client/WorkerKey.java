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

package persistent.bazel.client;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.hash.HashCode;
import java.nio.file.Path;
import java.nio.file.attribute.UserPrincipal;
import java.util.Objects;
import java.util.SortedMap;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.ToString;

@ToString(onlyExplicitlyIncluded = true)
public class WorkerKey {
  @Getter @ToString.Include private final BasicWorkerKey basicWorkerKey;

  /** The user the worker process is running under and the owner of the worker's files. */
  @Getter @Nullable private final UserPrincipal owner;

  /**
   * Cached value for the hash of this key, because the value is expensive to calculate
   * (ImmutableMap and ImmutableList do not cache their hashcodes).
   */
  private final int hash;

  public WorkerKey(BasicWorkerKey basicWorkerKey, @Nullable UserPrincipal owner) {
    this.basicWorkerKey = Preconditions.checkNotNull(basicWorkerKey);
    this.owner = owner;
    this.hash = calculateHashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }

    if (other == null || getClass() != other.getClass()) {
      return false;
    }

    WorkerKey otherWorkerKey = (WorkerKey) other;

    if (!basicWorkerKey.equals(otherWorkerKey.basicWorkerKey)) {
      return false;
    }

    if (owner == null && otherWorkerKey.owner == null) {
      return true;
    }

    return owner != null && otherWorkerKey.owner != null && owner.equals(otherWorkerKey.owner);
  }

  public ImmutableList<String> getArgs() {
    return basicWorkerKey.getArgs();
  }

  public ImmutableList<String> getCmd() {
    return basicWorkerKey.getCmd();
  }

  public ImmutableMap<String, String> getEnv() {
    return basicWorkerKey.getEnv();
  }

  public Path getExecRoot() {
    return basicWorkerKey.getExecRoot();
  }

  public String getMnemonic() {
    return basicWorkerKey.getMnemonic();
  }

  public HashCode getWorkerFilesCombinedHash() {
    return basicWorkerKey.getWorkerFilesCombinedHash();
  }

  public SortedMap<Path, HashCode> getWorkerFilesWithHashes() {
    return basicWorkerKey.getWorkerFilesWithHashes();
  }

  public Path getToolRoot() {
    return basicWorkerKey.getToolRoot();
  }

  @Override
  public int hashCode() {
    return hash;
  }

  public boolean isCancellable() {
    return basicWorkerKey.isCancellable();
  }

  public boolean isSandboxed() {
    return basicWorkerKey.isSandboxed();
  }

  private int calculateHashCode() {
    return Objects.hash(basicWorkerKey, owner);
  }
}
