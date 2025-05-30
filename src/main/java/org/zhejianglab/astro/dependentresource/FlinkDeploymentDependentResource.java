package org.zhejianglab.astro.dependentresource;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.CRUDKubernetesDependentResource;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.KubernetesDependent;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.spec.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zhejianglab.astro.customresource.FlinkIngestTask;
import org.zhejianglab.astro.customresource.flink.FlinkJobConfig;
import org.zhejianglab.astro.utils.FlinkUtils;

@KubernetesDependent
public class FlinkDeploymentDependentResource
    extends CRUDKubernetesDependentResource<FlinkDeployment, FlinkIngestTask> {

  private static final Logger log = LoggerFactory.getLogger(FlinkDeploymentDependentResource.class);

  private static final String FLINK_JOB_SUFFIX = "-flink-job";

  public FlinkDeploymentDependentResource() {
    super(FlinkDeployment.class);
  }

  @Override
  protected FlinkDeployment desired(FlinkIngestTask primary, Context<FlinkIngestTask> context) {

    if (primary.getSpec().getFlinkJobConfig() == null) {
      return null;
    } else {
      FlinkVersion flinkVersion =
          Optional.ofNullable(primary.getSpec().getFlinkJobConfig().getFlinkVersion())
              .orElse(FlinkVersion.v1_20);

      ObjectMeta metadata =
          new ObjectMetaBuilder()
              .withName(primary.getMetadata().getName() + FLINK_JOB_SUFFIX)
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

      FlinkDeploymentSpec.FlinkDeploymentSpecBuilder<?, ?> flinkDeploymentSpecBuilder =
          FlinkDeploymentSpec.builder();

      flinkDeploymentSpecBuilder
          .flinkConfiguration(
              Optional.ofNullable(primary.getSpec().getFlinkJobConfig().getFlinkConfiguration())
                  .orElse(Map.of()))
          .image(
              Optional.ofNullable(primary.getSpec().getFlinkJobConfig().getImage())
                  .orElse(FlinkUtils.getImageVersion(flinkVersion)))
          .serviceAccount(
              Optional.ofNullable(primary.getSpec().getFlinkJobConfig().getServiceAccount())
                  .orElse(FlinkJobConfig.FLINK_SERVICE_ACCOUNT))
          .flinkVersion(flinkVersion)
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
