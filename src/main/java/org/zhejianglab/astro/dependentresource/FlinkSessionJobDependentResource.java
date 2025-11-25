package org.zhejianglab.astro.dependentresource;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.CRUDKubernetesDependentResource;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.KubernetesDependent;
import java.util.List;
import java.util.Map;
import org.apache.flink.kubernetes.operator.api.FlinkSessionJob;
import org.apache.flink.kubernetes.operator.api.spec.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zhejianglab.astro.customresource.FlinkIngestTask;
import org.zhejianglab.astro.customresource.enums.IngestStatus;
import org.zhejianglab.astro.customresource.flink.FlinkJobConfig;

@KubernetesDependent
public class FlinkSessionJobDependentResource
    extends CRUDKubernetesDependentResource<FlinkSessionJob, FlinkIngestTask> {

  private static final Logger log = LoggerFactory.getLogger(FlinkSessionJobDependentResource.class);

  private static final String FLINK_SESSION_CLUSTER_NAME = "metadata-flink-session-cluster";

  public FlinkSessionJobDependentResource() {
    super(FlinkSessionJob.class);
  }

  @Override
  protected FlinkSessionJob desired(FlinkIngestTask primary, Context<FlinkIngestTask> context) {

    if (primary.getSpec().getFlinkJobConfig() == null) {
      return null;
    }

    // 关键修改：如果任务已完成，直接返回 null，让框架忽略这个资源
    if (primary.getStatus() != null
        && primary.getStatus().getIngestStatus() == IngestStatus.FINISHED) {
      log.info(
          "Task {} is FINISHED, returning null to stop managing FlinkSessionJob",
          primary.getMetadata().getName());
      return null;
    }

    boolean isFinished =
        primary.getStatus() != null
            && primary.getStatus().getIngestStatus() == IngestStatus.FINISHED;

    // 后面是正常的创建逻辑
    ObjectMeta metadata =
        new ObjectMetaBuilder()
            .withName(primary.getMetadata().getName())
            .withNamespace(primary.getMetadata().getNamespace())
            .build();

    metadata.setOwnerReferences(
        List.of(
            new OwnerReferenceBuilder()
                .withApiVersion(primary.getApiVersion())
                .withKind(primary.getKind())
                .withName(primary.getMetadata().getName())
                .withUid(primary.getMetadata().getUid())
                .build()));

    FlinkSessionJobSpec.FlinkSessionJobSpecBuilder<?, ?> flinkSessionJobSpecBuilder =
        FlinkSessionJobSpec.builder();

    flinkSessionJobSpecBuilder.deploymentName(FLINK_SESSION_CLUSTER_NAME);

    FlinkJobConfig updatedJobConfig =
        primary
            .getSpec()
            .getFlinkJobConfig()
            .updateJobArgsMap(
                primary.getSpec().getJobParallelism(),
                primary.getSpec().getBatchId(),
                primary.getSpec().getPlatform(),
                primary.getSpec().getPaths(),
                primary.getSpec().getUserProperties(),
                primary.getSpec().getActivatedHandlers(),
                primary.getSpec().getTags(),
                primary.getSpec().getPathPatterns(),
                primary.getSpec().getAllowedSuffixes(),
                primary.getSpec().getExtraEnvs(),
                primary.getSpec().getExtraSecret());

    updatedJobConfig
        .getFlinkConfiguration()
        .putAll(Map.of("kubernetes.operator.job.restart.failed", "false"));

    JobSpec jobSpec = updatedJobConfig.getJob();
    if (isFinished && jobSpec.getState() != JobState.SUSPENDED) {
      log.info("Task is finished, setting JobState to SUSPENDED");
      jobSpec.setState(JobState.SUSPENDED);
    }

    flinkSessionJobSpecBuilder.job(jobSpec);
    flinkSessionJobSpecBuilder.flinkConfiguration(updatedJobConfig.getFlinkConfiguration());

    FlinkSessionJob sessionJob = new FlinkSessionJob();
    sessionJob.setMetadata(metadata);
    sessionJob.setSpec(flinkSessionJobSpecBuilder.build());

    log.debug(" current FlinkSessionJobDependentResource -> {}", sessionJob.toString());
    return sessionJob;
  }
}
