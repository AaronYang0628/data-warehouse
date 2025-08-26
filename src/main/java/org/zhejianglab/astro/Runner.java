package org.zhejianglab.astro;

import com.sun.net.httpserver.HttpServer;
import io.javaoperatorsdk.operator.Operator;
import java.io.IOException;
import java.net.InetSocketAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zhejianglab.astro.probes.LivenessHandler;
import org.zhejianglab.astro.probes.StartupHandler;
import org.zhejianglab.astro.reconciler.MetadataOperatorCornReconciler;
import org.zhejianglab.astro.reconciler.MetadataOperatorFlinkReconciler;

public class Runner {

  private static final Logger log = LoggerFactory.getLogger(Runner.class);

  /**
   * Main entry point for the Metadata Ingest Operator. apply the following command: kubectl apply
   * -f ./target/classes/META-INF/fabric8/metadataingesttasks.org.zhejianglab.astro-v1.yml
   *
   * @param args
   * @throws IOException
   */
  public static void main(String[] args) throws IOException {
    log.info("Metadata Ingest Operator starting!");
    Operator operator =
        new Operator(
            o ->
                o.withStopOnInformerErrorDuringStartup(false)
                    .checkingCRDAndValidateLocalModel(true));

    operator.register(new MetadataOperatorFlinkReconciler());
    operator.register(new MetadataOperatorCornReconciler());
    // operator.register(new MetadataOperatorJavaReconciler());
    operator.start();

    log.info("Metadata Ingest Operator started.");

    HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
    server.createContext("/startup", new StartupHandler(operator));
    server.createContext("/healthz", new LivenessHandler(operator));
    server.setExecutor(null);
    server.start();

    log.info("Metadata Ingest Operator Healthy Probes started.");
  }
}
