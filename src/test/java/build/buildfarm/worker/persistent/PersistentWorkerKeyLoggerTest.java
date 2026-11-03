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

import static com.google.common.truth.Truth.assertThat;

import build.bazel.remote.execution.v2.RequestMetadata;
import build.buildfarm.common.config.PersistentWorkers;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.OutputStreamAppender;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.hash.HashCode;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import net.logstash.logback.composite.loggingevent.LoggingEventJsonProviders;
import net.logstash.logback.composite.loggingevent.MdcJsonProvider;
import net.logstash.logback.composite.loggingevent.MessageJsonProvider;
import net.logstash.logback.encoder.LoggingEventCompositeJsonEncoder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import persistent.bazel.client.WorkerKey;
import persistent.bazel.client.WorkerResources;

@RunWith(JUnit4.class)
public class PersistentWorkerKeyLoggerTest {
  private static final RequestMetadata METADATA =
      RequestMetadata.newBuilder()
          .setActionMnemonic("Javac")
          .setToolInvocationId("build-1")
          .setActionId("action-1")
          .build();

  private static WorkerKey key(String changed, ImmutableMap<String, String> env) {
    WorkerKey key =
        new WorkerKey(
            changed.equals("cmd") ? ImmutableList.of("a", "bc") : ImmutableList.of("ab", "c"),
            ImmutableList.of(changed.equals("args") ? "other-flag" : "--persistent_worker"),
            env,
            Path.of(changed.equals("root") ? "/different-root" : "/persistent-root"),
            changed.equals("mnemonic") ? "Scalac" : "JavaBuilder",
            HashCode.fromInt(changed.equals("tools") ? 2 : 1),
            new TreeMap<>(),
            !changed.equals("sandboxed"),
            changed.equals("cancellable"));
    if (changed.equals("resources")) {
      key =
          key.withResourceProfile(
              new WorkerResources.Profile() {
                public String identity() {
                  return "managed";
                }

                public WorkerResources create() {
                  return WorkerResources.NONE;
                }
              });
    }
    return key;
  }

  private static Map<String, String> fields(WorkerKey key) {
    return PersistentWorkerKeyLogger.fields(key, "bazel-X", "operation-1", METADATA);
  }

  @Test
  public void disabledByDefault() {
    assertThat(new PersistentWorkers().isLogKeyMappings()).isFalse();
  }

  @Test
  public void usesActualActionMnemonicAndRetainsIncomingKey() {
    Map<String, String> fields = fields(key("", ImmutableMap.of()));
    assertThat(fields.get("action_mnemonic")).isEqualTo("Javac");
    assertThat(fields.get("worker_execution_name")).isEqualTo("JavaBuilder");
    assertThat(fields.get("bazel_worker_key")).isEqualTo("bazel-X");
    assertThat(fields.get("operation_name")).isEqualTo("operation-1");
    assertThat(fields.get("tool_invocation_id")).isEqualTo("build-1");
    assertThat(fields.get("buildfarm_tools_hash")).isEqualTo(HashCode.fromInt(1).toString());
  }

  @Test
  public void equalKeysHaveStableFingerprintsRegardlessOfEnvironmentInsertionOrder() {
    WorkerKey a = key("", ImmutableMap.of("A", "1", "B", "2"));
    WorkerKey b = key("", ImmutableMap.of("B", "2", "A", "1"));
    assertThat(a).isEqualTo(b);
    assertThat(fields(a)).isEqualTo(fields(b));
    // Request/build metadata must not change the fingerprint for a compatible pool.
    assertThat(
            PersistentWorkerKeyLogger.fields(
                    b, "bazel-Z", "another-op", RequestMetadata.getDefaultInstance())
                .get("buildfarm_pool_key"))
        .isEqualTo(fields(a).get("buildfarm_pool_key"));
  }

  @Test
  public void distinguishesEveryPoolCompatibilityComponentAndAmbiguousConcatenations() {
    WorkerKey original = key("", ImmutableMap.of());
    String originalHash = fields(original).get("buildfarm_pool_key");
    for (String component :
        List.of(
            "cmd", "args", "root", "mnemonic", "tools", "sandboxed", "cancellable", "resources")) {
      WorkerKey different = key(component, ImmutableMap.of());
      assertThat(different).isNotEqualTo(original);
      assertThat(fields(different).get("buildfarm_pool_key")).isNotEqualTo(originalHash);
    }
    assertThat(fields(key("", ImmutableMap.of("ENV", "different"))).get("buildfarm_pool_key"))
        .isNotEqualTo(originalHash);
    assertThat(fields(key("", ImmutableMap.of("ab", "c"))).get("buildfarm_env_hash"))
        .isNotEqualTo(fields(key("", ImmutableMap.of("a", "bc"))).get("buildfarm_env_hash"));
  }

  @Test
  public void jsonEncoderEmitsOneFlatEventAndRestoresMdc() {
    Logger logger = (Logger) LoggerFactory.getLogger(PersistentWorkerKeyLogger.class);
    Level previousLevel = logger.getLevel();
    boolean previousAdditive = logger.isAdditive();
    Map<String, String> previousMdc = MDC.getCopyOfContextMap();
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    OutputStreamAppender<ILoggingEvent> appender = new OutputStreamAppender<>();
    LoggingEventCompositeJsonEncoder encoder = new LoggingEventCompositeJsonEncoder();
    // These are the message and MDC providers used by Lucid's production logback.xml.
    LoggingEventJsonProviders providers = new LoggingEventJsonProviders();
    providers.addMessage(new MessageJsonProvider());
    providers.addMdc(new MdcJsonProvider());
    encoder.setContext(logger.getLoggerContext());
    encoder.setProviders(providers);
    encoder.start();
    appender.setContext(logger.getLoggerContext());
    appender.setEncoder(encoder);
    appender.setOutputStream(output);
    appender.start();
    logger.addAppender(appender);
    logger.setLevel(Level.INFO);
    logger.setAdditive(false);
    MDC.clear();
    MDC.put("action_mnemonic", "previous-request");
    MDC.put("unrelated", "preserved");
    try {
      PersistentWorkerKeyLogger.log(
          key("", ImmutableMap.of("PRIVATE_ENV", "private-value")),
          "bazel-X",
          "operation-1",
          METADATA);
      String json = output.toString(StandardCharsets.UTF_8);
      assertThat(json.lines().count()).isEqualTo(1L);
      JsonObject event = JsonParser.parseString(json).getAsJsonObject();
      assertThat(event.get("event_type").getAsString()).isEqualTo("persistent_worker_key_mapping");
      assertThat(event.get("action_mnemonic").getAsString()).isEqualTo("Javac");
      assertThat(event.get("bazel_worker_key").getAsString()).isEqualTo("bazel-X");
      assertThat(event.get("buildfarm_pool_key").getAsString()).hasLength(64);
      assertThat(json).doesNotContain("PRIVATE_ENV");
      assertThat(json).doesNotContain("private-value");
      assertThat(MDC.getCopyOfContextMap())
          .containsExactly("action_mnemonic", "previous-request", "unrelated", "preserved");
      MDC.clear();
      PersistentWorkerKeyLogger.log(key("", ImmutableMap.of()), "bazel-X", "operation-2", METADATA);
      assertThat(MDC.getCopyOfContextMap()).isNull();
    } finally {
      logger.detachAppender(appender);
      logger.setLevel(previousLevel);
      logger.setAdditive(previousAdditive);
      appender.stop();
      encoder.stop();
      if (previousMdc == null) {
        MDC.clear();
      } else {
        MDC.setContextMap(previousMdc);
      }
    }
  }
}
