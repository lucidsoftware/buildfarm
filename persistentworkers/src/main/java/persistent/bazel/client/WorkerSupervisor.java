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

import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import persistent.common.CommonsSupervisor;

public abstract class WorkerSupervisor extends CommonsSupervisor<WorkerKey, PersistentWorker> {
  private final Logger logger = Logger.getLogger(this.getClass().getName());
  private final WorkerIndex workerIndex;

  public WorkerSupervisor(WorkerIndex workerIndex) {
    this.workerIndex = workerIndex;
  }

  public abstract PersistentWorker createUnderlying(WorkerKey workerKey) throws Exception;

  public final PersistentWorker create(WorkerKey key) throws Exception {
    PersistentWorker worker = createUnderlying(key);

    // The worker is about to be entered into the pool, so add it to the index
    workerIndex.registerWorker(worker);

    return worker;
  }

  @Override
  public PooledObject<PersistentWorker> wrap(PersistentWorker persistentWorker) {
    return new DefaultPooledObject<>(persistentWorker);
  }

  @Override
  public boolean validateObject(WorkerKey key, PooledObject<PersistentWorker> p) {
    PersistentWorker worker = p.getObject();
    Optional<Integer> exitValue = worker.getExitValue();
    if (exitValue.isPresent()) {
      String errorStr;
      try {
        String err = worker.getStdErr();
        errorStr = "Stderr:\n" + err;
      } catch (Exception e) {
        errorStr = "Couldn't read Stderr: " + e;
      }
      String msg =
          String.format(
              "Worker unexpectedly died with exit code %d. Key:\n%s\n%s",
              exitValue.get(), key, errorStr);
      logger.log(Level.SEVERE, msg);
      return false;
    }

    return true;
  }
}
