// Copyright 2026 The Buildfarm Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package build.buildfarm.worker.persistent;

import build.bazel.remote.execution.v2.RequestMetadata;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import persistent.bazel.client.WorkerKey;

/** Temporary diagnostics for the key actually passed to the persistent-worker pool. */
final class PersistentWorkerKeyLogger {
  private static final Logger logger = LoggerFactory.getLogger(PersistentWorkerKeyLogger.class);

  private PersistentWorkerKeyLogger() {}

  static void log(
      WorkerKey key, String incomingKey, String operationName, RequestMetadata metadata) {
    if (!logger.isInfoEnabled()) {
      return;
    }
    Map<String, String> previous = MDC.getCopyOfContextMap();
    try {
      fields(key, incomingKey, operationName, metadata).forEach(MDC::put);
      logger.info("Persistent worker key mapping (before pool selection)");
    } finally {
      if (previous == null) {
        MDC.clear();
      } else {
        MDC.setContextMap(previous);
      }
    }
  }

  static Map<String, String> fields(
      WorkerKey key, String incomingKey, String operationName, RequestMetadata metadata) {
    // These components cover WorkerKey.equals, including the resource compatibility identity.
    // Keep exact execRoot semantics: normalizing it could conceal a real pool split.
    Map<String, String> components = new LinkedHashMap<>();
    components.put("buildfarm_cmd_hash", fingerprint(key.getCmd()));
    components.put("buildfarm_args_hash", fingerprint(key.getArgs()));
    components.put("buildfarm_env_hash", environmentFingerprint(key.getEnv()));
    components.put("buildfarm_exec_root_hash", fingerprint(List.of(key.getExecRoot().toString())));
    components.put("worker_execution_name", key.getMnemonic());
    components.put("buildfarm_tools_hash", key.getWorkerFilesCombinedHash().toString());
    components.put("worker_sandboxed", Boolean.toString(key.isSandboxed()));
    components.put("worker_cancellable", Boolean.toString(key.isCancellable()));
    components.put(
        "buildfarm_resource_profile_hash",
        fingerprint(List.of(key.getResourceProfile().identity())));

    Map<String, String> fields = new LinkedHashMap<>(components);
    fields.put("event_type", "persistent_worker_key_mapping");
    fields.put("key_mapping_version", "1");
    fields.put("key_mapping_stage", "before_pool_selection");
    fields.put("action_mnemonic", metadata.getActionMnemonic());
    fields.put("bazel_worker_key", incomingKey);
    fields.put("operation_name", operationName);
    fields.put("tool_invocation_id", metadata.getToolInvocationId());
    fields.put("action_id", metadata.getActionId());
    fields.put("buildfarm_pool_key", environmentFingerprint(components));
    fields.put("worker_tool_count", Integer.toString(key.getWorkerFilesWithHashes().size()));
    return fields;
  }

  private static String environmentFingerprint(Map<String, String> values) {
    // Map equality ignores insertion order. Sort to preserve that property in diagnostics.
    Hasher hasher = Hashing.sha256().newHasher();
    hasher.putInt(values.size());
    new TreeMap<>(values)
        .forEach(
            (name, value) -> {
              putString(hasher, name);
              putString(hasher, value);
            });
    return hasher.hash().toString();
  }

  private static String fingerprint(List<String> values) {
    Hasher hasher = Hashing.sha256().newHasher();
    hasher.putInt(values.size());
    values.forEach(value -> putString(hasher, value));
    return hasher.hash().toString();
  }

  private static void putString(Hasher hasher, String value) {
    byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
    // Length prefixes distinguish ["ab", "c"] from ["a", "bc"], including non-ASCII input.
    hasher.putInt(bytes.length).putBytes(bytes);
  }
}
