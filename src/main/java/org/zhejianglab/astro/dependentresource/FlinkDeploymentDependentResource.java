package org.zhejianglab.astro.dependentresource;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.CRUDKubernetesDependentResource;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.KubernetesDependent;
import java.util.Optional;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.spec.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zhejianglab.astro.customresource.MetadataIngestTask;

@KubernetesDependent
public class FlinkDeploymentDependentResource
    extends CRUDKubernetesDependentResource<FlinkDeployment, MetadataIngestTask> {

  private static final Logger log = LoggerFactory.getLogger(FlinkDeploymentDependentResource.class);

  public FlinkDeploymentDependentResource() {
    super(FlinkDeployment.class);
  }

  //  @Override
  //  public ErrorStatusHandler<MetadataIngestTask> getErrorStatusHandler() {
  //    return (retryInfo, exception, context) -> {
  //      // 记录错误日志
  //      log.error("Error processing FlinkDeployment", exception);
  //
  //      // 标记自定义资源状态为错误
  //      primary.getStatus().setError(exception.getMessage());
  //
  //      // 设置重试策略
  //      return ErrorStatusUpdateControl.updateStatus(primary)
  //              .withRetry(true)
  //              .withRetryAfter(Duration.ofMinutes(5));
  //    };
  //  }

  @Override
  protected FlinkDeployment desired(
      MetadataIngestTask primary, Context<MetadataIngestTask> context) {

    if (primary.getSpec().getFlinkJobConfig() == null) {
      return null;
    } else {
      ObjectMeta metadata =
          new ObjectMetaBuilder()
              .withName(primary.getMetadata().getName() + "-flink-job")
              .withNamespace(primary.getMetadata().getNamespace())
              .build();

      FlinkDeploymentSpec.FlinkDeploymentSpecBuilder<?, ?> flinkDeploymentSpecBuilder =
          FlinkDeploymentSpec.builder();

      flinkDeploymentSpecBuilder
          .image(
              Optional.ofNullable(primary.getSpec().getFlinkJobConfig().getImage())
                  .orElse("flink:1.20"))
          .serviceAccount(
              Optional.ofNullable(primary.getSpec().getFlinkJobConfig().getServiceAccount())
                  .orElse("flink"))
          .flinkVersion(
              Optional.ofNullable(primary.getSpec().getFlinkJobConfig().getFlinkVersion())
                  .orElse(FlinkVersion.v1_20))
          .jobManager(primary.getSpec().getFlinkJobConfig().getJobManager())
          .taskManager(
              TaskManagerSpec.builder()
                  .resource(primary.getSpec().getFlinkJobConfig().getTaskManager().getResource())
                  .podTemplate(
                      primary.getSpec().getFlinkJobConfig().getTaskManager().getPodTemplate())
                  .replicas(primary.getSpec().getFlinkJobConfig().getTaskManager().getReplicas())
                  .build())
          .job(primary.getSpec().getFlinkJobConfig().getJob());

      FlinkDeployment deployment = new FlinkDeployment();
      deployment.setMetadata(metadata);
      deployment.setSpec(flinkDeploymentSpecBuilder.build());

      return deployment;
    }
  }
}
