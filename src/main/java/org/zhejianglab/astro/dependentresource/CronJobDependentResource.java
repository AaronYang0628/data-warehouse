package org.zhejianglab.astro.dependentresource;

import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.batch.v1.CronJob;
import io.fabric8.kubernetes.api.model.batch.v1.CronJobBuilder;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.CRUDKubernetesDependentResource;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.KubernetesDependent;
import org.zhejianglab.astro.customresource.CronIngestTask;

@KubernetesDependent
public class CronJobDependentResource
    extends CRUDKubernetesDependentResource<CronJob, CronIngestTask> {

  public CronJobDependentResource() {
    super(CronJob.class);
  }

  @Override
  protected CronJob desired(CronIngestTask primary, Context<CronIngestTask> context) {
    return new CronJobBuilder()
        .withMetadata(
            new ObjectMetaBuilder()
                .withName(primary.getMetadata().getName())
                .withNamespace(primary.getMetadata().getNamespace())
                .build())
        .withSpec(null)
        .build();
  }
}
