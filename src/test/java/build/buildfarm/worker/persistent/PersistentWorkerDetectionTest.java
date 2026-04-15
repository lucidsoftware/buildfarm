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

package build.buildfarm.worker.persistent;

import static com.google.common.truth.Truth.assertThat;

import build.bazel.remote.execution.v2.Command;
import build.bazel.remote.execution.v2.Platform;
import build.buildfarm.common.ExecutionProperties;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class PersistentWorkerDetectionTest {
  private static Command commandWithPersistentWorkerKey(String key) {
    return Command.newBuilder()
        .setPlatform(
            Platform.newBuilder()
                .addProperties(
                    Platform.Property.newBuilder()
                        .setName(ExecutionProperties.PERSISTENT_WORKER_KEY)
                        .setValue(key)
                        .build())
                .build())
        .build();
  }

  @Test
  public void isPersistentWorkerAction_withKeyAndAllowedMnemonic() {
    // Default allowlist is ["*"], which allows all mnemonics
    Command command = commandWithPersistentWorkerKey("abc123hash");
    assertThat(PersistentWorkerDetection.isPersistentWorkerAction(command, "Javac")).isTrue();
  }

  @Test
  public void isPersistentWorkerAction_withoutKey() {
    Command command = Command.getDefaultInstance();
    assertThat(PersistentWorkerDetection.isPersistentWorkerAction(command, "Javac")).isFalse();
  }

  @Test
  public void isPersistentWorkerAction_withEmptyKey() {
    Command command = commandWithPersistentWorkerKey("");
    assertThat(PersistentWorkerDetection.isPersistentWorkerAction(command, "Javac")).isFalse();
  }

  @Test
  public void extractPersistentWorkerKey_present() {
    Command command = commandWithPersistentWorkerKey("abc123hash");
    assertThat(PersistentWorkerDetection.extractPersistentWorkerKey(command))
        .isEqualTo("abc123hash");
  }

  @Test
  public void extractPersistentWorkerKey_absent() {
    Command command = Command.getDefaultInstance();
    assertThat(PersistentWorkerDetection.extractPersistentWorkerKey(command)).isEmpty();
  }

  @Test
  public void extractPersistentWorkerKey_emptyPlatform() {
    Command command = Command.newBuilder().setPlatform(Platform.getDefaultInstance()).build();
    assertThat(PersistentWorkerDetection.extractPersistentWorkerKey(command)).isEmpty();
  }
}
