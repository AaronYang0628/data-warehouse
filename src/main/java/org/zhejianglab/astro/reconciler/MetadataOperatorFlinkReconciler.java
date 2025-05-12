package org.zhejianglab.astro.reconciler;

import io.javaoperatorsdk.operator.api.reconciler.*;
import io.javaoperatorsdk.operator.api.reconciler.dependent.Dependent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zhejianglab.astro.customresource.MetadataIngestTask;
import org.zhejianglab.astro.dependentresource.FlinkDeploymentDependentCondition;
import org.zhejianglab.astro.dependentresource.FlinkDeploymentDependentResource;
import org.zhejianglab.astro.utils.ExceptionUtils;

@Workflow(
    explicitInvocation = true,
    dependents = {
      @Dependent(
          type = FlinkDeploymentDependentResource.class,
          reconcilePrecondition = FlinkDeploymentDependentCondition.class,
          activationCondition = FlinkDeploymentDependentCondition.class)
    })
public class MetadataOperatorFlinkReconciler
    implements Reconciler<MetadataIngestTask>, Cleaner<MetadataIngestTask> {

  private static final Logger log = LoggerFactory.getLogger(MetadataOperatorFlinkReconciler.class);

  @Override
  public ErrorStatusUpdateControl<MetadataIngestTask> updateErrorStatus(
      MetadataIngestTask primary, Context<MetadataIngestTask> context, Exception e) {
    return ExceptionUtils.handleError(primary, e);
  }

  public UpdateControl<MetadataIngestTask> reconcile(
      MetadataIngestTask primary, Context<MetadataIngestTask> context) {

    log.info("Reconcile flink platform");
    context.managedWorkflowAndDependentResourceContext().reconcileManagedWorkflow();
    if (context.isNextReconciliationImminent()) {
      // your logic, maybe return?
      log.info("Reconcile flink inner logic");
    }
    return UpdateControl.noUpdate();
  }

  public DeleteControl cleanup(MetadataIngestTask primary, Context<MetadataIngestTask> context) {
    log.info("Delete flink platform");
    context.managedWorkflowAndDependentResourceContext().reconcileManagedWorkflow();
    return DeleteControl.defaultDelete();
  }
}
