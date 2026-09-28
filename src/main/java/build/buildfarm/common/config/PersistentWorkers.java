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

package build.buildfarm.common.config;

import lombok.Data;

/** Persistent-worker lifecycle settings. */
@Data
public class PersistentWorkers {
  public enum IdleRetirementMode {
    DISABLED,
    SHADOW,
    ENABLED
  }

  // Zero immediately falls back when the compatible pool is full.
  private long poolWaitTimeoutMillis = 1000;
  private int maxWorkersPerKey = 6;
  private int maxWorkersTotal = 100;
  private int warmIdleWorkersPerKey = 0;
  private IdleRetirementMode idleRetirementMode = IdleRetirementMode.DISABLED;
  private long idleTimeoutSeconds = 900;
  private long idleCheckIntervalSeconds = 30;
  private long gracefulTerminationSeconds = 5;
}
