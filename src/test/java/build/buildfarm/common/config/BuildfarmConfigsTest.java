package build.buildfarm.common.config;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

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
    configs.setWorker(new Worker());
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
  public void validateCasStorageSizeConfig_bothSet_throws() {
    Cas cas = new Cas();
    cas.setMaxSizeBytes(1000);
    cas.setMaxSizePercent(50);
    // Both maxSizeBytes and maxSizePercent can't be non-zero simultaneously
    assertThrows(
        ConfigurationException.class, () -> BuildfarmConfigs.validateCasStorageSizeConfig(cas));
  }

  @Test
  public void validateCasStorageSizeConfig_bytesNegative_throws() {
    Cas cas = new Cas();
    cas.setMaxSizeBytes(-1);
    // maxSizeBytes must be non-negative
    assertThrows(
        ConfigurationException.class, () -> BuildfarmConfigs.validateCasStorageSizeConfig(cas));
  }

  @Test
  public void validateCasStorageSizeConfig_percentNegative_throws() {
    Cas cas = new Cas();
    cas.setMaxSizePercent(-1);
    // maxSizePercent must be positive
    assertThrows(
        ConfigurationException.class, () -> BuildfarmConfigs.validateCasStorageSizeConfig(cas));
  }

  @Test
  public void validateCasStorageSizeConfig_percentOver100_throws() {
    Cas cas = new Cas();
    cas.setMaxSizePercent(101);
    // maxSizePercent must be 100 or smaller
    assertThrows(
        ConfigurationException.class, () -> BuildfarmConfigs.validateCasStorageSizeConfig(cas));
  }

  private void setCasMaxSizePercentAndValidateSize(int maxSizePercent)
      throws ConfigurationException {
    Worker worker = new Worker();
    worker.setRoot(tempDir.toString());
    BuildfarmConfigs.getInstance().setWorker(worker);

    Cas cas = new Cas();
    cas.setMaxSizePercent(maxSizePercent);
    cas.setMaxSizeBytes(0);
    // When both maxSizePercent and maxSizeBytes are 0, maxSizePercent is preferred and set to
    // DEFAULT_MAX_SIZE_PERCENT
    if (maxSizePercent == 0) {
      maxSizePercent = BuildfarmConfigs.DEFAULT_MAX_SIZE_PERCENT;
    }
    BuildfarmConfigs.deriveCasStorage(cas);
    long expectedBytes = (long) (tempDir.toFile().getTotalSpace() * maxSizePercent / 100.0);
    assertEquals(expectedBytes, cas.getMaxSizeBytes());
  }

  @Test
  public void deriveCasStorage_validMaxSizePercent_computesCorrectBytes()
      throws ConfigurationException {
    // When valid maxSizePercent values are used, the CAS size is limited correctly
    setCasMaxSizePercentAndValidateSize(100);
    setCasMaxSizePercentAndValidateSize(1);
  }

  @Test
  public void deriveCasStorage_maxSizePercentAndMaxSizeBytesAt0_defaultsToDefaultMaxSizePercent()
      throws ConfigurationException {
    setCasMaxSizePercentAndValidateSize(0);
  }

  @Test
  public void deriveCasStorage_maxSizeBytesAlone_limitSetCorrectly() throws ConfigurationException {
    Cas cas = new Cas();
    cas.setMaxSizeBytes(1000);
    cas.setMaxSizePercent(0);
    BuildfarmConfigs.deriveCasStorage(cas);
    // When maxSizePercent is 0, maxSizeBytes is used and set correctly
    assertEquals(1000, cas.getMaxSizeBytes());
  }

  @Test
  public void loadConfigs_withMaxSizePercent_parsesCorrectly() throws IOException {
    Path configFile = tempDir.resolve("percent.yaml");
    String yamlContent = "worker:\n  storages:\n    - maxSizePercent: 75\n";
    Files.write(configFile, yamlContent.getBytes());

    BuildfarmConfigs configs = BuildfarmConfigs.loadConfigs(configFile);
    assertEquals(75, configs.getWorker().getStorages().get(0).getMaxSizePercent());
  }

  @Test
  public void validateCasEvictorConfig_lowWatermarkEqualToMax_throws() {
    Cas cas = new Cas();
    cas.setMaxSizePercent(90);
    cas.setLowWatermarkPercent(90); // equal to max — must be strictly less
    ConfigurationException ex =
        assertThrows(
            ConfigurationException.class, () -> BuildfarmConfigs.validateCasEvictorConfig(cas));
    assertTrue(
        "message should reference lowWatermarkPercent: " + ex.getMessage(),
        ex.getMessage().contains("lowWatermarkPercent"));
  }

  @Test
  public void validateCasEvictorConfig_lowWatermarkAboveMax_throws() {
    Cas cas = new Cas();
    cas.setMaxSizePercent(90);
    cas.setLowWatermarkPercent(95);
    assertThrows(
        ConfigurationException.class, () -> BuildfarmConfigs.validateCasEvictorConfig(cas));
  }

  @Test
  public void validateCasEvictorConfig_lowWatermarkOneBelowMax_passes()
      throws ConfigurationException {
    Cas cas = new Cas();
    cas.setMaxSizePercent(90);
    cas.setLowWatermarkPercent(89);
    BuildfarmConfigs.validateCasEvictorConfig(cas);
  }

  @Test
  public void validateCasEvictorConfig_lowWatermarkNegative_throws() {
    Cas cas = new Cas();
    cas.setLowWatermarkPercent(-1);
    assertThrows(
        ConfigurationException.class, () -> BuildfarmConfigs.validateCasEvictorConfig(cas));
  }

  @Test
  public void validateCasEvictorConfig_lowWatermarkZero_passes() throws ConfigurationException {
    // 0 is the sentinel for auto-derive; allowed.
    Cas cas = new Cas();
    cas.setLowWatermarkPercent(0);
    BuildfarmConfigs.validateCasEvictorConfig(cas);
  }

  @Test
  public void validateCasEvictorConfig_lowWatermarkBytesModeHighPercent_passes()
      throws ConfigurationException {
    // bytes-mode (maxSizePercent == 0) interprets lowWatermarkPercent as a percent of the cap
    // itself, so values up to 99 are legal.
    Cas cas = new Cas();
    cas.setMaxSizeBytes(1000);
    cas.setMaxSizePercent(0);
    cas.setLowWatermarkPercent(95);
    BuildfarmConfigs.validateCasEvictorConfig(cas);
  }

  @Test
  public void validateCasEvictorConfig_lowWatermarkBytesModeAtHundred_throws() {
    // 100 % of cap is the cap itself — must be strictly less.
    Cas cas = new Cas();
    cas.setMaxSizeBytes(1000);
    cas.setMaxSizePercent(0);
    cas.setLowWatermarkPercent(100);
    assertThrows(
        ConfigurationException.class, () -> BuildfarmConfigs.validateCasEvictorConfig(cas));
  }

  @Test
  public void validateCasEvictorConfig_lowWatermarkSetButNoCapConfigured_throws() {
    // Operator set lowWatermarkPercent but did not configure either maxSizeBytes or
    // maxSizePercent — there is no cap to compute a watermark against.
    Cas cas = new Cas();
    cas.setMaxSizeBytes(0);
    cas.setMaxSizePercent(0);
    cas.setLowWatermarkPercent(50);
    ConfigurationException ex =
        assertThrows(
            ConfigurationException.class, () -> BuildfarmConfigs.validateCasEvictorConfig(cas));
    assertTrue(
        "message should mention the missing cap: " + ex.getMessage(),
        ex.getMessage().contains("maxSizeBytes") && ex.getMessage().contains("maxSizePercent"));
  }

  @Test
  public void validateCasEvictorConfig_wakeBudgetBelowMinimum_throws() {
    Cas cas = new Cas();
    cas.setEvictorWakeBudgetMillis(0L); // below 1 ms floor
    ConfigurationException ex =
        assertThrows(
            ConfigurationException.class, () -> BuildfarmConfigs.validateCasEvictorConfig(cas));
    assertTrue(
        "message should reference evictorWakeBudgetMillis: " + ex.getMessage(),
        ex.getMessage().contains("evictorWakeBudgetMillis"));
  }

  @Test
  public void validateCasEvictorConfig_wakeBudgetAboveMaximum_throws() {
    Cas cas = new Cas();
    cas.setEvictorWakeBudgetMillis(2000L); // above 1 s cap
    assertThrows(
        ConfigurationException.class, () -> BuildfarmConfigs.validateCasEvictorConfig(cas));
  }

  @Test
  public void validateCasEvictorConfig_heartbeatBelowMinimum_throws() {
    Cas cas = new Cas();
    cas.setEvictorIdleHeartbeatMillis(50L); // below 100 ms floor
    assertThrows(
        ConfigurationException.class, () -> BuildfarmConfigs.validateCasEvictorConfig(cas));
  }

  @Test
  public void validateCasEvictorConfig_heartbeatAboveMaximum_throws() {
    Cas cas = new Cas();
    cas.setEvictorIdleHeartbeatMillis(120_000L); // 2 min — above 1 min cap
    assertThrows(
        ConfigurationException.class, () -> BuildfarmConfigs.validateCasEvictorConfig(cas));
  }

  @Test
  public void validateCasEvictorConfig_wakeBudgetAtOrAboveHeartbeat_throws() {
    // The heartbeat is the backstop for missed wake signals and must outlast a sweep.
    Cas cas = new Cas();
    cas.setEvictorWakeBudgetMillis(500L);
    cas.setEvictorIdleHeartbeatMillis(500L); // equal, must be strictly greater
    ConfigurationException ex =
        assertThrows(
            ConfigurationException.class, () -> BuildfarmConfigs.validateCasEvictorConfig(cas));
    assertTrue(
        "message should reference the cross-field constraint: " + ex.getMessage(),
        ex.getMessage().contains("evictorWakeBudgetMillis")
            && ex.getMessage().contains("evictorIdleHeartbeatMillis"));
  }

  @Test
  public void validateCasEvictorConfig_validValues_pass() throws ConfigurationException {
    Cas cas = new Cas();
    cas.setMaxSizePercent(90);
    cas.setLowWatermarkPercent(80);
    cas.setEvictorWakeBudgetMillis(50L);
    cas.setEvictorIdleHeartbeatMillis(2000L);
    BuildfarmConfigs.validateCasEvictorConfig(cas);
  }

  @Test
  public void validateCasEvictorConfig_evictorShardsZero_passes() throws ConfigurationException {
    // 0 is the auto-derive sentinel.
    Cas cas = new Cas();
    cas.setEvictorShards(0);
    BuildfarmConfigs.validateCasEvictorConfig(cas);
  }

  @Test
  public void validateCasEvictorConfig_evictorShardsPowerOfTwo_passes()
      throws ConfigurationException {
    for (int n : new int[] {1, 2, 4, 8, 16, 32}) {
      Cas cas = new Cas();
      cas.setEvictorShards(n);
      BuildfarmConfigs.validateCasEvictorConfig(cas);
    }
  }

  @Test
  public void validateCasEvictorConfig_evictorShardsNotPowerOfTwo_throws() {
    Cas cas = new Cas();
    cas.setEvictorShards(3);
    ConfigurationException ex =
        assertThrows(
            ConfigurationException.class, () -> BuildfarmConfigs.validateCasEvictorConfig(cas));
    assertTrue(
        "message should reference evictorShards: " + ex.getMessage(),
        ex.getMessage().contains("evictorShards"));
  }

  @Test
  public void validateCasEvictorConfig_evictorShardsNegative_throws() {
    Cas cas = new Cas();
    cas.setEvictorShards(-1);
    assertThrows(
        ConfigurationException.class, () -> BuildfarmConfigs.validateCasEvictorConfig(cas));
  }

  @Test
  public void validateCasEvictorConfig_evictorShardsTooHighForMaxEntry_throws() {
    BuildfarmConfigs configs = BuildfarmConfigs.getInstance();
    long previousMaxEntrySizeBytes = configs.getMaxEntrySizeBytes();
    try {
      configs.setMaxEntrySizeBytes(1024);
      Cas cas = new Cas();
      cas.setMaxSizeBytes(4096);
      cas.setEvictorShards(8);

      ConfigurationException ex =
          assertThrows(
              ConfigurationException.class, () -> BuildfarmConfigs.validateCasEvictorConfig(cas));
      assertTrue(ex.getMessage().contains("maxEntrySizeBytes"));
    } finally {
      configs.setMaxEntrySizeBytes(previousMaxEntrySizeBytes);
    }
  }

  @Test
  public void validateCasEvictorConfig_percentModeEvictorShardsTooHigh_throws() {
    // The shard bound must be enforced in percent mode too (maxSizeBytes still 0). An absurd
    // power-of-two count exceeds the bound for any plausible resolved cap, so it throws regardless
    // of the test machine's disk size.
    BuildfarmConfigs configs = BuildfarmConfigs.getInstance();
    long previousMaxEntrySizeBytes = configs.getMaxEntrySizeBytes();
    try {
      configs.setMaxEntrySizeBytes(1L << 20); // 1 MiB
      Cas cas = new Cas();
      cas.setMaxSizePercent(50); // percent mode: maxSizeBytes stays 0
      cas.setEvictorShards(1 << 28); // power of two; cap / 1 MiB < 2^28 for any cap < 256 TiB

      ConfigurationException ex =
          assertThrows(
              ConfigurationException.class, () -> BuildfarmConfigs.validateCasEvictorConfig(cas));
      assertTrue(ex.getMessage().contains("maxEntrySizeBytes"));
    } finally {
      configs.setMaxEntrySizeBytes(previousMaxEntrySizeBytes);
    }
  }

  @Test
  public void validateCasEvictorConfig_allDefaults_passes() throws ConfigurationException {
    // Default Cas (operator left every field unset) must clear the validator.
    BuildfarmConfigs.validateCasEvictorConfig(new Cas());
  }

  @Test
  public void loadConfigs_withLowWatermarkPercent_parsesCorrectly() throws IOException {
    Path configFile = tempDir.resolve("low-watermark.yaml");
    String yamlContent = "worker:\n  storages:\n    - lowWatermarkPercent: 70\n";
    Files.write(configFile, yamlContent.getBytes());

    BuildfarmConfigs configs = BuildfarmConfigs.loadConfigs(configFile);
    assertEquals(70, configs.getWorker().getStorages().get(0).getLowWatermarkPercent());
  }

  @Test
  public void loadWorkerConfigs_lowWatermarkPercentWithDerivedDefaultCap_passes() throws Exception {
    Path configFile = tempDir.resolve("worker-low-watermark-derived-cap.yaml");
    String yamlContent =
        "worker:\n"
            + "  root: '"
            + tempDir.toString().replace("'", "''")
            + "'\n"
            + "  storages:\n"
            + "    - lowWatermarkPercent: 70\n";
    Files.write(configFile, yamlContent.getBytes());

    BuildfarmConfigs configs =
        BuildfarmConfigs.loadWorkerConfigs(new String[] {configFile.toString()});
    Cas storage = configs.getWorker().getStorages().get(0);
    assertEquals(70, storage.getLowWatermarkPercent());
    assertEquals(0, storage.getMaxSizePercent());
    assertTrue(storage.getMaxSizeBytes() > 0);
  }

  @Test
  public void loadConfigs_withEvictorMillisFields_parsesCorrectly() throws IOException {
    // Ensure SnakeYAML resolves long-typed fields through the Lombok setters.
    Path configFile = tempDir.resolve("evictor-millis.yaml");
    String yamlContent =
        "worker:\n"
            + "  storages:\n"
            + "    - evictorWakeBudgetMillis: 25\n"
            + "      evictorIdleHeartbeatMillis: 3000\n";
    Files.write(configFile, yamlContent.getBytes());

    BuildfarmConfigs configs = BuildfarmConfigs.loadConfigs(configFile);
    Cas storage = configs.getWorker().getStorages().get(0);
    assertEquals(25L, storage.getEvictorWakeBudgetMillis());
    assertEquals(3000L, storage.getEvictorIdleHeartbeatMillis());
  }
}
