package build.buildfarm.common;

import org.slf4j.bridge.SLF4JBridgeHandler;

public abstract class LoggingMain {
  static {
    // Ship all java.util.logging logs to SLF4J.
    SLF4JBridgeHandler.removeHandlersForRootLogger();
    SLF4JBridgeHandler.install();
    System.setProperty("java.util.logging.manager", WaitingLogManager.class.getName());
  }

  protected abstract void onShutdown() throws InterruptedException;

  private void shutdown() {
    try {
      onShutdown();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } finally {
      WaitingLogManager.release();
    }
  }

  protected LoggingMain(String applicationName) {
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                /* group= */ null,
                /* target= */ this::shutdown,
                /* name= */ applicationName + "-Shutdown"));
  }
}
