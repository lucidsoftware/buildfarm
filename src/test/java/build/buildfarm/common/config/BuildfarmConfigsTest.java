package build.buildfarm.common.config;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import javax.naming.ConfigurationException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class BuildfarmConfigsTest {
  private Path tempDir;

  @Before
  public void setUp() throws IOException {
    tempDir = Files.createTempDirectory("buildfarm-test");
    BuildfarmConfigs configs = BuildfarmConfigs.getInstance();
    configs.setServer(new Server());
    configs.setBackplane(new Backplane());
  }

  @After
  public void tearDown() throws IOException {
    Files.walk(tempDir)
        .sorted((a, b) -> -a.compareTo(b))
        .forEach(
            path -> {
              try {
                Files.delete(path);
              } catch (IOException e) {
              }
            });
  }

  @Test
  public void loadConfigs_withRelativePathNoParent_shouldNotThrowNPE() throws IOException {
    Path configFile = tempDir.resolve("server.yaml");
    String yamlContent = "server:\n  port: 8980\n";
    Files.write(configFile, yamlContent.getBytes());

    BuildfarmConfigs configs = BuildfarmConfigs.loadConfigs(configFile);
    assertNotNull(configs);
  }

  @Test
  public void loadConfigs_withNonExistentFile_shouldThrowException() {
    Path nonExistentFile = tempDir.resolve("nonexistent.yaml");
    assertThrows(NoSuchFileException.class, () -> BuildfarmConfigs.loadConfigs(nonExistentFile));
  }

  @Test
  public void loadConfigs_withInvalidYaml_shouldThrowException() throws IOException {
    Path configFile = tempDir.resolve("invalid.yaml");
    String invalidYaml = "invalid: yaml: content";
    Files.write(configFile, invalidYaml.getBytes());

    assertThrows(RuntimeException.class, () -> BuildfarmConfigs.loadConfigs(configFile));
  }

  @Test
  public void loadConfigs_withValidYaml_shouldLoadSuccessfully() throws IOException {
    Path configFile = tempDir.resolve("valid.yaml");
    String validYaml = "server:\n  port: 8980\nbackplane:\n  redisUri: redis://localhost:6379";
    Files.write(configFile, validYaml.getBytes());

    BuildfarmConfigs configs = BuildfarmConfigs.loadConfigs(configFile);
    assertNotNull(configs);
    assertNotNull(configs.getServer());
    assertNotNull(configs.getBackplane());
  }

  @Test
  public void loadConfigs_withPersistentWorkerLifecycleSettings_parsesCorrectly()
      throws IOException {
    Path configFile = tempDir.resolve("persistent-workers.yaml");
    String yamlContent =
        "worker:\n"
            + "  persistentWorkers:\n"
            + "    observationOnly: true\n"
            + "    observationSampleRate: 0.25\n"
            + "    observationLogEvents: false\n"
            + "    observationWindowSeconds: 60\n"
            + "    observationMaxKeys: 100\n"
            + "    poolWaitTimeoutMillis: 250\n"
            + "    maxWorkersPerKey: 4\n"
            + "    maxWorkersTotal: 40\n"
            + "    warmIdleWorkersPerKey: 1\n"
            + "    idleRetirementMode: ENABLED\n"
            + "    idleTimeoutSeconds: 120\n"
            + "    idleCheckIntervalSeconds: 15\n"
            + "    gracefulTerminationSeconds: 3\n";
    Files.write(configFile, yamlContent.getBytes());

    BuildfarmConfigs configs = BuildfarmConfigs.loadConfigs(configFile);
    PersistentWorkers settings = configs.getWorker().getPersistentWorkers();
    assertEquals(true, settings.isObservationOnly());
    assertEquals(0.25, settings.getObservationSampleRate(), 0.0);
    assertEquals(false, settings.isObservationLogEvents());
    assertEquals(60L, settings.getObservationWindowSeconds());
    assertEquals(100, settings.getObservationMaxKeys());
    assertEquals(250L, settings.getPoolWaitTimeoutMillis());
    assertEquals(4, settings.getMaxWorkersPerKey());
    assertEquals(40, settings.getMaxWorkersTotal());
    assertEquals(1, settings.getWarmIdleWorkersPerKey());
    assertEquals(PersistentWorkers.IdleRetirementMode.ENABLED, settings.getIdleRetirementMode());
    assertEquals(120L, settings.getIdleTimeoutSeconds());
    assertEquals(15L, settings.getIdleCheckIntervalSeconds());
    assertEquals(3L, settings.getGracefulTerminationSeconds());
  }

  @Test
  public void validatePersistentWorkers_rejectsWarmCountAbovePerKeyMaximum() {
    PersistentWorkers settings = new PersistentWorkers();
    settings.setMaxWorkersPerKey(2);
    settings.setWarmIdleWorkersPerKey(3);

    assertThrows(
        ConfigurationException.class, () -> BuildfarmConfigs.validatePersistentWorkers(settings));
  }

  @Test
  public void validatePersistentWorkers_rejectsNegativePoolWait() {
    PersistentWorkers settings = new PersistentWorkers();
    settings.setPoolWaitTimeoutMillis(-1);
    assertThrows(
        ConfigurationException.class, () -> BuildfarmConfigs.validatePersistentWorkers(settings));
  }

  @Test
  public void validatePersistentWorkers_rejectsInvalidObservationSampling() {
    PersistentWorkers settings = new PersistentWorkers();
    for (double rate : new double[] {-0.1, 1.1, Double.NaN, Double.POSITIVE_INFINITY}) {
      settings.setObservationSampleRate(rate);
      assertThrows(
          ConfigurationException.class, () -> BuildfarmConfigs.validatePersistentWorkers(settings));
    }
  }

  @Test
  public void validatePersistentWorkers_rejectsUnboundedKeyTracking() {
    PersistentWorkers settings = new PersistentWorkers();
    settings.setObservationMaxKeys(0);
    assertThrows(
        ConfigurationException.class, () -> BuildfarmConfigs.validatePersistentWorkers(settings));
    settings.setObservationMaxKeys(10);
    settings.setObservationWindowSeconds(Long.MAX_VALUE);
    assertThrows(
        ConfigurationException.class, () -> BuildfarmConfigs.validatePersistentWorkers(settings));
    settings.setObservationWindowSeconds(0);
    assertThrows(
        ConfigurationException.class, () -> BuildfarmConfigs.validatePersistentWorkers(settings));
  }
}
