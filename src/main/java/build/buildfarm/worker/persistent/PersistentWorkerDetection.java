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

import build.bazel.remote.execution.v2.Command;
import build.bazel.remote.execution.v2.Platform;
import build.buildfarm.common.ExecutionProperties;
import build.buildfarm.common.config.BuildfarmConfigs;
import java.util.Collection;

/**
 * Shared utility for detecting persistent worker actions. Used by both InputFetcher (for early
 * detection) and Executor (for execution routing). Using a single shared function ensures both
 * stages make the same decision.
 */
public final class PersistentWorkerDetection {
  /** Wildcard entry in the mnemonic allowlist — matches every action mnemonic. */
  public static final String ALL_MNEMONICS = "*";

  private PersistentWorkerDetection() {}

  /**
   * Determines whether an action should run on a persistent worker. Checks two conditions:
   *
   * <ol>
   *   <li>The action's platform properties contain a non-empty persistentWorkerKey
   *   <li>The action's mnemonic is in the server-side allowlist (or the allowlist contains {@link
   *       #ALL_MNEMONICS})
   * </ol>
   *
   * @param command the action's command (contains platform properties)
   * @param actionMnemonic the action's mnemonic (e.g., "Javac", "Scalac")
   * @return true if both conditions are met
   */
  public static boolean isPersistentWorkerAction(Command command, String actionMnemonic) {
    String key = extractPersistentWorkerKey(command);
    if (key.isEmpty()) {
      return false;
    }
    Collection<String> allowlist =
        BuildfarmConfigs.getInstance().getWorker().getPersistentWorkerActionMnemonicAllowlist();
    return allowlist.contains(ALL_MNEMONICS) || allowlist.contains(actionMnemonic);
  }

  /** Extracts the persistentWorkerKey from the command's platform properties. */
  public static String extractPersistentWorkerKey(Command command) {
    Platform platform = command.getPlatform();
    for (Platform.Property property : platform.getPropertiesList()) {
      if (property.getName().equals(ExecutionProperties.PERSISTENT_WORKER_KEY)) {
        return property.getValue();
      }
    }
    return "";
  }
}
