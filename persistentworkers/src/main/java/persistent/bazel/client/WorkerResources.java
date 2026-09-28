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

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;

/** Process-owned resources, independent of any particular request or pool key. */
public interface WorkerResources {
  WorkerResources NONE = new WorkerResources() {};

  default List<String> launchPrefix() {
    return List.of();
  }

  default Map<String, Long> sample() {
    return Map.of();
  }

  default void setCpu(int cpuMicros) throws IOException {}

  default void resume() throws IOException, InterruptedException {}

  default void idle() throws IOException, InterruptedException {}

  default boolean terminate(Duration grace) throws IOException, InterruptedException {
    return true;
  }

  /** Stable compatibility identity; factories and individual process IDs are not identity. */
  interface Profile {
    Profile NONE =
        new Profile() {
          public String identity() {
            return "unmanaged";
          }

          public WorkerResources create() {
            return WorkerResources.NONE;
          }
        };

    String identity();

    /** Allocate resources for one new process, initially inactive until resume(). */
    WorkerResources create() throws IOException;
  }
}
