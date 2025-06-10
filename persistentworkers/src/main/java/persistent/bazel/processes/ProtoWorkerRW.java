package persistent.bazel.processes;

import com.google.devtools.build.lib.worker.WorkerProtocol.WorkRequest;
import com.google.devtools.build.lib.worker.WorkerProtocol.WorkResponse;
import com.google.protobuf.GeneratedMessage;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.function.Supplier;
import lombok.Getter;
import org.apache.commons.io.IOUtils;
import persistent.common.processes.ProcessWrapper;

/**
 * Based off Google's ProtoWorkerProtocol Slightly generified to encapsulate read/writes Should be
 * used by both the PersistentWorker (client-side) and the WorkRequestHandler (in the persistent
 * worker process)
 *
 * <p>Writes WorkRequest protos to the persistent worker Reads WorkResponse protos from the
 * persistent worker
 *
 * <p>Static methods also expose some useful(?) utilities
 *
 * <p>TODO: What happens to input/output streams when the process dies? Presumably, it is closed (as
 * per tests).
 */
public class ProtoWorkerRW {
  @Getter private final ProcessWrapper processWrapper;

  private final InputStream readStream;

  private final OutputStream writeStream;

  public ProtoWorkerRW(ProcessWrapper processWrapper) {
    this.processWrapper = processWrapper;
    this.readStream = processWrapper.getStdOut();
    this.writeStream = processWrapper.getStdIn();
  }

  public void write(WorkRequest req) throws IOException {
    writeTo(req, this.writeStream);
  }

  public WorkResponse waitAndRead() throws IOException {
    return readResponse(readStream);
  }

  public static <R extends GeneratedMessage> void writeTo(R req, OutputStream outputStream)
      throws IOException {
    try {
      req.writeDelimitedTo(outputStream);
    } finally {
      outputStream.flush();
    }
  }

  public static WorkResponse readResponse(InputStream inputStream) throws IOException {
    return WorkResponse.parseDelimitedFrom(inputStream);
  }

  public static WorkRequest readRequest(InputStream inputStream) throws IOException {
    return WorkRequest.parseDelimitedFrom(inputStream);
  }
}
