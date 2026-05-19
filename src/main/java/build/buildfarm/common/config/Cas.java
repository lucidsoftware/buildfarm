package build.buildfarm.common.config;

import com.google.common.base.Strings;
import java.nio.file.Path;
import javax.naming.ConfigurationException;
import lombok.Data;

@Data
public class Cas {
  public enum TYPE {
    FILESYSTEM,
    GRPC,
    MEMORY,
    FUSE
  }

  private TYPE type = TYPE.FILESYSTEM;

  // MEMORY/FILESYSTEM
  private String path = "cache";
  private int hexBucketLevels = 0;
  private long maxSizeBytes = 0;
  private int maxSizePercent = 0;
  private boolean fileDirectoriesIndexInMemory = false;
  private boolean skipLoad = false;

  // Evictor watermark/sweep tunables (Phase 2.1; sharding arrives in Phase 2.2).
  //
  // lowWatermarkPercent: sweep target. The evictor sweeps until totalBytes drops to lowBytes.
  //   The base differs by mode (see Worker.computeLowBytes): in percent-mode (maxSizePercent set)
  //   it is a percent of the FILESYSTEM total -- lowBytes = filesystemSize * lowWatermarkPercent /
  //   100 -- so the same value denotes a HIGHER fraction of the cap than the number suggests (e.g.
  //   lowWatermarkPercent=80 with maxSizePercent=90 sweeps to ~89% of the cap). In bytes-mode
  //   (maxSizeBytes set) it is a percent of the CAP itself (80 -> 80% of the cap). A sentinel of 0
  //   triggers the auto-default of 80% of the cap in both modes.
  // evictorWakeBudgetMillis: wall-clock budget per sweep wake. The evictor yields after
  //   ~this many milliseconds of sweeping regardless of per-eviction cost. Tunable from
  //   observed charger park p99. Default 50 ms.
  // evictorIdleHeartbeatMillis: idle heartbeat for the evictor's await(); backstop
  //   against missed signals. Producer wake signals drive the evictor under load.
  //   Default 2000 ms. Other timing fields in Buildfarm config use ms/s suffixes; nanos
  //   in YAML would force operators to write 50_000_000 / 2_000_000_000 literals.
  private int lowWatermarkPercent = 0;
  private long evictorWakeBudgetMillis = 50L;
  private long evictorIdleHeartbeatMillis = 2000L;

  // if creating a hardlink fails, copy the file instead
  private boolean execRootCopyFallback = false;

  // GRPC
  private String target;
  private boolean readonly = false;

  public Path getValidPath(Path root) throws ConfigurationException {
    if (Strings.isNullOrEmpty(path)) {
      throw new ConfigurationException("Cas cache directory value in config missing");
    }
    return root.resolve(path);
  }
}
