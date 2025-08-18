package org.zhejianglab.astro.customresource.flink;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.PodSpecBuilder;
import io.fabric8.kubernetes.api.model.PodTemplateSpec;
import io.fabric8.kubernetes.api.model.PodTemplateSpecBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.PostConstruct;
import lombok.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.kubernetes.operator.api.spec.*;
import org.apache.logging.log4j.core.util.UuidUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zhejianglab.astro.utils.FlinkUtils;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
public class FlinkJobConfig {

  private static final Logger log = LoggerFactory.getLogger(FlinkJobConfig.class);

  public static final String FLINK_SERVICE_ACCOUNT = "metadata-ingest-flink-sa";

  private static final String FLINK_TASKMANAGER_NUMBER_OF_TASK_SLOTS =
      "taskmanager.numberOfTaskSlots";

  @Builder.Default private String repository = "docker.io/library";

  private String image;

  @Builder.Default private String serviceAccount = FLINK_SERVICE_ACCOUNT;

  @Builder.Default private FlinkVersion flinkVersion = FlinkVersion.v1_20;

  private IngressSpec ingress;

  private PodTemplateSpec podTemplate;

  private JobManagerSpec jobManager;

  private FlinkIngestTaskManagerSpec taskManager;

  private JobSpec job;

  private Map<String, String> flinkConfiguration;

  @Builder.Default private KubernetesDeploymentMode mode = KubernetesDeploymentMode.NATIVE;

  private static KubernetesClient kubernetesClient;

  @PostConstruct
  public void init() {
    if (kubernetesClient == null) {
      try {
        kubernetesClient =
            new io.fabric8.kubernetes.client.DefaultKubernetesClient(
                io.fabric8.kubernetes.client.Config.autoConfigure(null));
        log.info("connect to local k8s");
      } catch (Exception e) {
        log.warn("Failed to initialize Kubernetes client: {}", e.getMessage());
      }
    }
  }

  public FlinkJobConfig getDeploymentDefaultConfig() {
    return FlinkJobConfig.builder()
        .image(this.getRepository() + "/" + FlinkUtils.getImageVersion(this.getFlinkVersion()))
        .flinkVersion(this.getFlinkVersion())
        .ingress(
            IngressSpec.builder()
                .template("/{{namespace}}/{{name}}(/|$)(.*)")
                .className("nginx")
                .annotations(Map.of("nginx.ingress.kubernetes.io/rewrite-target", "/$2"))
                .build())
        .flinkConfiguration(new ConcurrentHashMap<>())
        .serviceAccount(this.getServiceAccount())
        .podTemplate(
            new PodTemplateSpecBuilder()
                .withSpec(
                    new PodSpecBuilder()
                        .withContainers(
                            new ContainerBuilder()
                                .withName("flink-main-container")
                                .withImagePullPolicy("IfNotPresent")
                                .build())
                        .build())
                .build())
        .jobManager(
            JobManagerSpec.builder().replicas(1).resource(new Resource(1.0, "2Gi", "1Gi")).build())
        .taskManager(
            FlinkIngestTaskManagerSpec.builder().resource(new Resource(1.0, "2Gi", "1Gi")).build())
        .job(
            JobSpec.builder()
                .jarURI("local:///opt/flink/examples/streaming/StateMachineExample.jar")
                .entryClass("org.apache.flink.streaming.examples.statemachine.StateMachineExample")
                .parallelism(2)
                .upgradeMode(UpgradeMode.STATELESS)
                .build())
        .mode(this.getMode())
        .build();
  }

  public FlinkJobConfig getSessionJobDefaultConfig(FlinkIngestTaskSpec primarSpec) {

    // 从secret中读取S3_ACCESS_KEY的值
    String s3AccessKey = "";
    String s3AccessSecret = "";
    String s3Endpoint = "";

    if (kubernetesClient != null && primarSpec.getExtraSecret() != null) {
      try {
        // 读取secret
        io.fabric8.kubernetes.api.model.Secret secret =
            kubernetesClient
                .secrets()
                .inNamespace(primarSpec.getExtraSecret().split(".")[0])
                .withName(primarSpec.getExtraSecret().split(".")[1])
                .get();

        if (secret != null && secret.getData() != null) {
          if (secret.getData().containsKey("s3-access-key")) {
            s3AccessKey =
                new String(
                    java.util.Base64.getDecoder().decode(secret.getData().get("s3-access-key")));
          }
          if (secret.getData().containsKey("s3-access-secret")) {
            s3AccessSecret =
                new String(
                    java.util.Base64.getDecoder().decode(secret.getData().get("s3-access-secret")));
          }
          if (secret.getData().containsKey("s3-endpoint")) {
            s3Endpoint =
                new String(
                    java.util.Base64.getDecoder().decode(secret.getData().get("s3-endpoint")));
          }
        }
      } catch (KubernetesClientException e) {
        log.error("Failed to read secret: {}", e.getMessage());
      }
    }

    return FlinkJobConfig.builder()
        .job(
            JobSpec.builder()
                .jarURI(
                    "http://data-warehouse-minio.metadata.svc.cluster.local:9000/flink/jars/flink-es-ingest-job-1.0.0-all.jar")
                .parallelism(primarSpec.getJobParallelism())
                .upgradeMode(UpgradeMode.STATELESS)
                .entryClass("com.zhejianglab.astronomy.metadata.Main")
                .args(
                    new String[] {
                      "BATCH_ID=" + UuidUtil.getTimeBasedUuid().toString(),
                      "SCAN_CONFIG=" + generateScanConfig(),
                      "PLATFORM=" + primarSpec.getPlatform(),
                      "SCAN_PATH=" + primarSpec.getPath(),
                      "KAFKA_BOOTSTRAP_SERVER=metadata-kafka.metadata.sve.cluster.local:9092",
                      "S3_ENDPOINT=" + s3Endpoint,
                      "S3_ACCESS_KEY=" + s3AccessKey,
                      "S3_ACCESS_SECRET=" + s3AccessSecret,
                      "S3_TABLE_NAME=" + primarSpec.getS3TableName()
                    })
                .build())
        .build();
  }

  public void updateJobParallelism(Integer jobParallelism) {
    JobSpec jobSpec = this.getJob();
    if (jobSpec == null) {
      throw new IllegalStateException("JobSpec is null, cannot update parallelism.");
    }
    jobSpec.setParallelism(jobParallelism);
    this.setJob(jobSpec);
  }

  public void updateTaskSlots(Integer taskSlots) {
    this.getFlinkConfiguration().put(FLINK_TASKMANAGER_NUMBER_OF_TASK_SLOTS, taskSlots.toString());
  }

  private String generateScanConfig() {
    return "";
  }
}
