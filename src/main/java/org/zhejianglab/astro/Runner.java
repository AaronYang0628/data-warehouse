package org.zhejianglab.astro;

import com.sun.net.httpserver.HttpServer;
import io.javaoperatorsdk.operator.Operator;
import java.io.IOException;
import java.net.InetSocketAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zhejianglab.astro.probes.LivenessHandler;
import org.zhejianglab.astro.probes.StartupHandler;
import org.zhejianglab.astro.reconciler.MetadataOperatorDefaultReconciler;
import org.zhejianglab.astro.reconciler.MetadataOperatorDevReconciler;

public class Runner {

  private static final String METADATA_OPERATOR_MODE = "METADATA_OPERATOR_MODE";

  private static final String METADATA_OPERATOR_DEV_MODE = "dev";

  private static final String METADATA_OPERATOR_PROD_MODE = "prod";

  private static final Logger log = LoggerFactory.getLogger(Runner.class);

  /**
   * Main entry point for the Metadata Ingest Operator. ## remember to add the following environment
   * variables: METADATA_OPERATOR_MODE=dev or prod and apply the following command: kubectl apply -f
   * ./target/classes/META-INF/fabric8/metadataingesttasks.org.zhejianglab.astro-v1.yml
   *
   * @param args
   * @throws IOException
   */
  public static void main(String[] args) throws IOException {
    log.info("Metadata Ingest Operator starting!");
    Operator operator = new Operator(o -> o.withStopOnInformerErrorDuringStartup(false));
    String mode = System.getenv(METADATA_OPERATOR_MODE);

    if (METADATA_OPERATOR_DEV_MODE.equals(mode)) {
      log.info("Metadata Ingest Operator running in dev mode.");
      operator.register(new MetadataOperatorDevReconciler());
    } else if (METADATA_OPERATOR_PROD_MODE.equals(mode)) {
      log.info("Metadata Ingest Operator running in prod mode.");
      operator.register(new MetadataOperatorDefaultReconciler());
    } else {
      log.error("Invalid METADATA_OPERATOR_MODE: {}", mode);
      System.out.println("Invalid METADATA_OPERATOR_MODE: " + mode);
      System.exit(1);
    }
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
