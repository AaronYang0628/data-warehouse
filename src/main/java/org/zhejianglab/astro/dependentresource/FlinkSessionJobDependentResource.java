package org.zhejianglab.astro.dependentresource;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.CRUDKubernetesDependentResource;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.KubernetesDependent;
import java.util.List;
import org.apache.flink.kubernetes.operator.api.FlinkSessionJob;
import org.apache.flink.kubernetes.operator.api.spec.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zhejianglab.astro.customresource.FlinkIngestTask;

@KubernetesDependent
public class FlinkSessionJobDependentResource
    extends CRUDKubernetesDependentResource<FlinkSessionJob, FlinkIngestTask> {

  private static final Logger log = LoggerFactory.getLogger(FlinkSessionJobDependentResource.class);

  private static final String FLINK_SESSION_JOB_SUFFIX = "-flink-session-job";

  private static final String FLINK_SESSION_CLUSTER_NAME = "metadata-flink-session-cluster";

  public FlinkSessionJobDependentResource() {
    super(FlinkSessionJob.class);
  }

  @Override
  protected FlinkSessionJob desired(FlinkIngestTask primary, Context<FlinkIngestTask> context) {

    if (primary.getSpec().getFlinkJobConfig() == null) {
      return null;
    } else {
      ObjectMeta metadata =
          new ObjectMetaBuilder()
              .withName(primary.getMetadata().getName() + FLINK_SESSION_JOB_SUFFIX)
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
      flinkSessionJobSpecBuilder.job(primary.getSpec().getFlinkJobConfig().getJob());

      FlinkSessionJob sessionJob = new FlinkSessionJob();
      sessionJob.setMetadata(metadata);
      sessionJob.setSpec(flinkSessionJobSpecBuilder.build());

      log.info(sessionJob.toString());
      return sessionJob;
    }
  }
}
