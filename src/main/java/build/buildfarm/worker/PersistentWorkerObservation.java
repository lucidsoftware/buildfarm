// Copyright 2017 The Buildfarm Authors. All rights reserved.
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

package build.buildfarm.worker;

import com.google.common.hash.Hashing;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import java.nio.charset.StandardCharsets;
import java.util.TreeMap;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.logging.Logger;
import persistent.bazel.client.WorkerKey;

/** Bounded, per-action log tracing; never retains keys globally or publishes per-key metrics. */
final class PersistentWorkerObservation {
  private static final Gson JSON = new Gson();
  private static final String SESSION = UUID.randomUUID().toString();
  private static final Logger logger =
      Logger.getLogger(PersistentWorkerObservation.class.getName());
  private final JsonObject fields;
  private final Consumer<String> sink;
  private long started;

  static boolean shouldSample(String operation, double fraction) {
    if (fraction <= 0) {
      return false;
    }
    if (fraction >= 1) {
      return true;
    }
    long value = Hashing.sha256().hashString(operation, StandardCharsets.UTF_8).asLong() >>> 1;
    return (double) value / Long.MAX_VALUE < fraction;
  }

  static String fingerprint(WorkerKey key) {
    JsonObject identity = new JsonObject();
    identity.add("cmd", JSON.toJsonTree(key.getCmd()));
    identity.add("args", JSON.toJsonTree(key.getArgs()));
    identity.add("env", JSON.toJsonTree(new TreeMap<>(key.getEnv())));
    identity.addProperty("root", key.getExecRoot().toString());
    identity.addProperty("mnemonic", key.getMnemonic());
    identity.addProperty("tools", key.getWorkerFilesCombinedHash().toString());
    identity.addProperty("sandboxed", key.isSandboxed());
    identity.addProperty("cancellable", key.isCancellable());
    identity.addProperty("resources", key.getResourceProfile().identity());
    return hash(identity.toString());
  }

  private static String hash(String value) {
    return Hashing.sha256().hashString(value, StandardCharsets.UTF_8).toString();
  }

  PersistentWorkerObservation(ExecutionContext context, String worker, double sampleRate) {
    this(context, worker, sampleRate, message -> logger.info("PW_OBSERVATION " + message));
  }

  PersistentWorkerObservation(
      ExecutionContext context, String worker, double sampleRate, Consumer<String> sink) {
    this.sink = sink;
    fields = new JsonObject();
    fields.addProperty("schema_version", 1);
    fields.addProperty("worker", worker);
    fields.addProperty("worker_session", SESSION);
    fields.addProperty("operation", context.operation.getName());
    fields.addProperty("attempt", UUID.randomUUID().toString());
    fields.addProperty("sample_rate", sampleRate);
    fields.addProperty("invocation", context.metadata.getRequestMetadata().getToolInvocationId());
    fields.addProperty("mnemonic", context.metadata.getRequestMetadata().getActionMnemonic());
    var metadata = context.metadata.getExecuteOperationMetadata().getPartialExecutionMetadata();
    timestamp("queued_at", metadata.getQueuedTimestamp());
    timestamp("worker_started_at", metadata.getWorkerStartTimestamp());
    timestamp("execution_stage_started_at", metadata.getExecutionStartTimestamp());
    fields.addProperty("host_architecture", System.getProperty("os.arch"));
    for (var property : context.command.getPlatform().getPropertiesList()) {
      if (property.getName().equals("cpu-architecture")) {
        fields.addProperty("action_architecture", property.getValue());
      }
    }
  }

  private void timestamp(String name, Timestamp value) {
    if (!value.equals(Timestamp.getDefaultInstance())) {
      fields.addProperty(name, Timestamps.toString(value));
    }
  }

  void key(WorkerKey key) {
    fields.addProperty("key", fingerprint(key));
    fields.addProperty("tools_hash", key.getWorkerFilesCombinedHash().toString());
    fields.addProperty("environment_hash", hash(JSON.toJson(new TreeMap<>(key.getEnv()))));
    fields.addProperty(
        "launch_hash", hash(JSON.toJson(java.util.List.of(key.getCmd(), key.getArgs()))));
    fields.addProperty("resource_profile_hash", hash(key.getResourceProfile().identity()));
    fields.addProperty("work_root_hash", hash(key.getExecRoot().toString()));
    fields.addProperty("key_status", "computed");
  }

  void keyError(Exception error) {
    fields.remove("key");
    fields.addProperty("key_status", "error");
    // Exception messages may contain command arguments or environment values.
    fields.addProperty("error_type", error.getClass().getSimpleName());
  }

  void start(long preparationNanos) {
    fields.addProperty("observation_preparation_ms", preparationNanos / 1_000_000.0);
    started = System.nanoTime();
    emit("start", null, null);
  }

  void finish(String status, int exitCode) {
    emit("finish", status, exitCode);
  }

  private void emit(String event, String status, Integer exitCode) {
    try {
      JsonObject record = fields.deepCopy();
      record.addProperty("event", event);
      record.addProperty("timestamp", java.time.Instant.now().toString());
      if (status != null) {
        record.addProperty("status", status);
        record.addProperty("exit_code", exitCode);
        record.addProperty("native_attempt_ms", (System.nanoTime() - started) / 1_000_000.0);
      }
      sink.accept(JSON.toJson(record));
    } catch (RuntimeException ignored) {
      // Diagnostics must never change the result of a normal execution.
    }
  }
}
