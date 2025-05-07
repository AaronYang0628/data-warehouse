package org.zhejianglab.astro.reconciler;

import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import io.javaoperatorsdk.operator.api.reconciler.Workflow;
import io.javaoperatorsdk.operator.api.reconciler.dependent.Dependent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zhejianglab.astro.customresource.MetadataIngestTask;
import org.zhejianglab.astro.dependentresource.FlinkDeploymentDependentCondition;
import org.zhejianglab.astro.dependentresource.FlinkDeploymentDependentResource;

@Workflow(
    dependents = {
      @Dependent(
          type = FlinkDeploymentDependentResource.class,
          reconcilePrecondition = FlinkDeploymentDependentCondition.class,
          activationCondition = FlinkDeploymentDependentCondition.class)
    })
public class MetadataOperatorFlinkReconciler implements Reconciler<MetadataIngestTask> {

  private static final Logger log = LoggerFactory.getLogger(MetadataOperatorFlinkReconciler.class);

  public UpdateControl<MetadataIngestTask> reconcile(
      MetadataIngestTask primary, Context<MetadataIngestTask> context) {

    log.info("Reconcile flink platform");
    return UpdateControl.noUpdate();
  }
}
