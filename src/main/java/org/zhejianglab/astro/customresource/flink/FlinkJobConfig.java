package org.zhejianglab.astro.customresource.flink;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.PodSpecBuilder;
import io.fabric8.kubernetes.api.model.PodTemplateSpec;
import io.fabric8.kubernetes.api.model.PodTemplateSpecBuilder;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.kubernetes.operator.api.spec.*;
import org.zhejianglab.astro.utils.FlinkUtils;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
public class FlinkJobConfig {

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

  public FlinkJobConfig getDefaultConfig() {
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
}
