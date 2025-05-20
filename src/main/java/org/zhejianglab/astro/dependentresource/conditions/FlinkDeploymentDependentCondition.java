package org.zhejianglab.astro.dependentresource.conditions;

import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.dependent.DependentResource;
import io.javaoperatorsdk.operator.processing.dependent.workflow.Condition;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.zhejianglab.astro.customresource.FlinkIngestTask;
import org.zhejianglab.astro.customresource.Platform;

public class FlinkDeploymentDependentCondition
    implements Condition<FlinkDeployment, FlinkIngestTask> {

  @Override
  public boolean isMet(
      DependentResource<FlinkDeployment, FlinkIngestTask> dependentResource,
      FlinkIngestTask primary,
      Context<FlinkIngestTask> context) {

    return primary.getSpec().getPlatform().equalsIgnoreCase(Platform.OSS.getProtocol())
        || primary.getSpec().getPlatform().equalsIgnoreCase(Platform.S3.getProtocol())
        || primary.getSpec().getPlatform().equalsIgnoreCase(Platform.JDBC.getProtocol());
  }
}
