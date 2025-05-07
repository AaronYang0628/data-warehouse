package org.zhejianglab.astro.dependentresource;

import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.dependent.DependentResource;
import io.javaoperatorsdk.operator.processing.dependent.workflow.Condition;
import org.zhejianglab.astro.customresource.MetadataIngestTask;
import org.zhejianglab.astro.customresource.Platform;

public class JavaCRUDDependentCondition implements Condition<Job, MetadataIngestTask> {

  @Override
  public boolean isMet(
      DependentResource<Job, MetadataIngestTask> dependentResource,
      MetadataIngestTask primary,
      Context<MetadataIngestTask> context) {

    return primary.getSpec().getPlatform().equalsIgnoreCase(Platform.VIRTUAL.getProtocol());
  }
}
