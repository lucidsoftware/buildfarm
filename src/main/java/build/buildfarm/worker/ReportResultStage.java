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

import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;
import java.util.logging.Logger;
import javax.annotation.concurrent.GuardedBy;
import lombok.extern.java.Log;

@Log
public class ReportResultStage extends SuperscalarPipelineStage {
  private static final Gauge reportResultSlotUsage =
      Gauge.build().name("report_result_slot_usage").help("Report result slot Usage.").register();
  private static final Histogram reportResultTime =
      Histogram.build()
          .name("report_result_time_ms")
          .help("Report result time in ms.")
          .buckets(
              1,
              2.5,
              5,
              10,
              25,
              50,
              100,
              250,
              500,
              1000,
              2500,
              5000,
              10000,
              30000,
              60000,
              120000,
              300000)
          .register();
  private static final Histogram reportResultStallTime =
      Histogram.build()
          .name("report_result_stall_time_ms")
          .help("Report result stall time in ms.")
          .buckets(
              0.01,
              0.025,
              0.05,
              0.1,
              0.25,
              0.5,
              1,
              2.5,
              5,
              10,
              25,
              50,
              100,
              250,
              500,
              1000,
              5000,
              30000)
          .register();

  @GuardedBy("this")
  private int slotUsage;

  public ReportResultStage(WorkerContext workerContext, PipelineStage output, PipelineStage error) {
    super(
        "ReportResultStage",
        "reporter",
        workerContext,
        output,
        error,
        workerContext.getReportResultStageWidth());
  }

  @Override
  protected Logger getLogger() {
    return log;
  }

  synchronized int removeAndRelease(String operationName) {
    releaseClaim(operationName, 1);
    slotUsage--;
    reportResultSlotUsage.set(slotUsage);
    return slotUsage;
  }

  public void releaseResultReporter(
      String operationName, long usecs, long stallUSecs, boolean success) {
    int size = removeAndRelease(operationName);
    reportResultTime.observe(usecs / 1000.0);
    reportResultStallTime.observe(stallUSecs / 1000.0);
    complete(
        operationName,
        usecs,
        stallUSecs,
        String.format("%s, %s", success ? "Success" : "Failure", getUsage(size)));
  }

  @Override
  protected int claimsRequired(ExecutionContext executionContext) {
    return 1;
  }

  @Override
  protected void iterate() throws InterruptedException {
    if (!workerContext.inGracefulShutdown() && isPaused()) {
      return;
    }
    ExecutionContext executionContext = take();
    ResultReporter reporter =
        new ResultReporter(workerContext, executionContext, this, pollerExecutor);

    synchronized (this) {
      slotUsage++;
      reportResultSlotUsage.set(slotUsage);
      start(executionContext.queueEntry.getExecuteEntry().getOperationName(), getUsage(slotUsage));
      executor.execute(reporter);
    }
  }
}
