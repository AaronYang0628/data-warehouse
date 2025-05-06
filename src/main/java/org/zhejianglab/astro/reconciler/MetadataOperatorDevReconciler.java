package org.zhejianglab.astro.reconciler;

import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import io.javaoperatorsdk.operator.api.reconciler.Workflow;
import io.javaoperatorsdk.operator.api.reconciler.dependent.Dependent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zhejianglab.astro.customresource.MetadataIngestTask;
import org.zhejianglab.astro.customresource.Platform;
import org.zhejianglab.astro.dependentresource.FlinkDeploymentDependentCondition;
import org.zhejianglab.astro.dependentresource.FlinkDeploymentDependentResource;

@Workflow(
    dependents = {
      @Dependent(
          type = FlinkDeploymentDependentResource.class,
          activationCondition = FlinkDeploymentDependentCondition.class)
    })
public class MetadataOperatorDevReconciler implements Reconciler<MetadataIngestTask> {

  private static final Logger log = LoggerFactory.getLogger(MetadataOperatorDevReconciler.class);

  public UpdateControl<MetadataIngestTask> reconcile(
      MetadataIngestTask primary, Context<MetadataIngestTask> context) {

    if (primary.getSpec().getPlatform().equals(Platform.VIRTUAL.getProtocol())) {
      log.info("Reconcile virtual platform");
      primary.getStatus().updateConditions("virtual", true);
      return UpdateControl.patchStatus(primary);
    } else {
      log.info("Reconcile other platform, create flink deployment job");
    }
    return UpdateControl.noUpdate();
  }
}
