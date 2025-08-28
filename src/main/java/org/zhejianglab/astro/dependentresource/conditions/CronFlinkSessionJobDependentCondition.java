package org.zhejianglab.astro.dependentresource.conditions;

import io.fabric8.kubernetes.api.model.batch.v1.CronJob;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.dependent.DependentResource;
import io.javaoperatorsdk.operator.processing.dependent.workflow.Condition;
import org.zhejianglab.astro.customresource.CronIngestTask;
import org.zhejianglab.astro.customresource.Platform;

public class CronFlinkSessionJobDependentCondition implements Condition<CronJob, CronIngestTask> {

  @Override
  public boolean isMet(
      DependentResource<CronJob, CronIngestTask> dependentResource,
      CronIngestTask primary,
      Context<CronIngestTask> context) {

    return primary.getSpec().getPlatform().equalsIgnoreCase(Platform.OSS.getProtocol())
        || primary.getSpec().getPlatform().equalsIgnoreCase(Platform.S3.getProtocol())
        || primary.getSpec().getPlatform().equalsIgnoreCase(Platform.JDBC.getProtocol());
  }
}
