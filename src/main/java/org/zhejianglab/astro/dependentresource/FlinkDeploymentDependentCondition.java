package org.zhejianglab.astro.dependentresource;

import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.dependent.DependentResource;
import io.javaoperatorsdk.operator.processing.dependent.workflow.Condition;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.zhejianglab.astro.customresource.MetadataIngestTask;
import org.zhejianglab.astro.customresource.Platform;

public class FlinkDeploymentDependentCondition
    implements Condition<FlinkDeployment, MetadataIngestTask> {

  @Override
  public boolean isMet(
      DependentResource<FlinkDeployment, MetadataIngestTask> dependentResource,
      MetadataIngestTask primary,
      Context<MetadataIngestTask> context) {

    return primary.getSpec().getPlatform().equals(Platform.VIRTUAL.getProtocol());
  }
}
