package org.zhejianglab.astro.dependentresource.conditions;

import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.dependent.DependentResource;
import io.javaoperatorsdk.operator.processing.dependent.workflow.Condition;
import org.zhejianglab.astro.customresource.Platform;
import org.zhejianglab.astro.customresource.VirtualIngestTask;

public class JavaCRUDDependentCondition implements Condition<Job, VirtualIngestTask> {

  @Override
  public boolean isMet(
      DependentResource<Job, VirtualIngestTask> dependentResource,
      VirtualIngestTask primary,
      Context<VirtualIngestTask> context) {

    return primary.getSpec().getPlatform().equalsIgnoreCase(Platform.VIRTUAL.getProtocol());
  }
}
