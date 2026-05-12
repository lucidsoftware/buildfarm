package build.buildfarm.tools;

import static build.bazel.remote.execution.v2.Compressor.Value.ZSTD;
import static build.buildfarm.common.grpc.Channels.createChannel;
import static java.util.concurrent.TimeUnit.DAYS;

import build.bazel.remote.execution.v2.RequestMetadata;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.Write;
import build.buildfarm.common.WritesHelper;
import build.buildfarm.common.ZstdCompressingInputStream;
import build.buildfarm.instance.Instance;
import build.buildfarm.instance.stub.StubInstance;
import build.buildfarm.v1test.Digest;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.util.Durations;
import io.grpc.ManagedChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.Callable;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

/** Upload a file to the buildfarm CAS. */
@Command(
    name = "upload",
    mixinStandardHelpOptions = true,
    description = "Upload a file to the buildfarm CAS")
class Upload implements Callable<Integer> {
  @Parameters(index = "0", description = CliConstants.BUILDFARM_HOST)
  private String host;

  @Parameters(index = "1", description = CliConstants.INSTANCE_NAME)
  private String instanceName;

  @Parameters(index = "2", description = CliConstants.HASH_FUNCTION)
  private String hashFunction;

  @Parameters(index = "3", description = "The path to the file to upload")
  private Path path;

  @Override
  public Integer call() throws Exception {
    DigestUtil digestUtil = DigestUtil.forHash(hashFunction);

    ManagedChannel channel = createChannel(host);
    Instance instance =
        new StubInstance(instanceName, "bf-upload", channel, Durations.fromDays(10));

    try {
      Digest digest = digestUtil.compute(path);
      Write write =
          instance.getBlobWrite(
              ZSTD, digest, UUID.randomUUID(), RequestMetadata.getDefaultInstance());
      // WritesHelper handles closing the InputStream, so the Zstd stream shouldn't leak
      ListenableFuture<Long> writtenFuture =
          WritesHelper.streamIntoWriteFuture(
              () -> new ZstdCompressingInputStream(Files.newInputStream(path)),
              write,
              digest,
              10,
              DAYS);
      try {
        writtenFuture.get();
      } catch (InterruptedException e) {
        writtenFuture.cancel(true);
        Thread.currentThread().interrupt();
        throw e;
      }
      System.out.println("Completed uploading " + DigestUtil.toString(digest));
    } finally {
      instance.stop();
    }
    return 0;
  }

  public static void main(String[] args) {
    int exitCode = new CommandLine(new Upload()).execute(args);
    System.exit(exitCode);
  }
}
