package org.zhejianglab.astro;

import com.sun.net.httpserver.HttpServer;
import io.javaoperatorsdk.operator.Operator;
import io.javaoperatorsdk.operator.api.config.ControllerConfiguration;
// 新增导入
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.function.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zhejianglab.astro.customresource.MetadataIngestTask;
import org.zhejianglab.astro.customresource.Platform;
import org.zhejianglab.astro.probes.LivenessHandler;
import org.zhejianglab.astro.probes.StartupHandler;
import org.zhejianglab.astro.reconciler.MetadataOperatorFlinkReconciler;
import org.zhejianglab.astro.reconciler.MetadataOperatorJavaReconciler;

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

    // Define the filter predicate
    Predicate<MetadataIngestTask> flinkReconcilerFilter = r -> !isVirtualPlatform(r);

    ControllerConfiguration<MetadataIngestTask> flinkConfig =
        ControllerConfigurationBuilder<MetadataIngestTask>.builder()
            .withName("flink-reconciler")
            .withFilter(flinkReconcilerFilter)
            .build();
    // Register Flink reconciler with filter
    operator.register(new MetadataOperatorFlinkReconciler(), flinkConfig);

    operator.register(new MetadataOperatorJavaReconciler());
    operator.start();

    log.info("Metadata Ingest Operator started.");

    HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
    server.createContext("/startup", new StartupHandler(operator));
    server.createContext("/healthz", new LivenessHandler(operator));
    server.setExecutor(null);
    server.start();

    log.info("Metadata Ingest Operator Healthy Probes started.");
  }

  private static boolean isVirtualPlatform(MetadataIngestTask ingestTask) {
    return ingestTask.getSpec().getPlatform().equalsIgnoreCase(Platform.VIRTUAL.getProtocol());
  }
}
