package org.zhejianglab.astro.reconciler;

import io.javaoperatorsdk.operator.api.reconciler.*;
import io.javaoperatorsdk.operator.api.reconciler.dependent.Dependent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zhejianglab.astro.customresource.MetadataIngestTask;
import org.zhejianglab.astro.dependentresource.ConfigMapDependentResource;
import org.zhejianglab.astro.dependentresource.JavaCRUDDependentCondition;

@Workflow(
    explicitInvocation = true,
    dependents = {
      @Dependent(
          type = ConfigMapDependentResource.class,
          reconcilePrecondition = JavaCRUDDependentCondition.class,
          activationCondition = JavaCRUDDependentCondition.class)
    })
@ControllerConfiguration
public class MetadataOperatorJavaReconciler
    implements Reconciler<MetadataIngestTask>, Cleaner<MetadataIngestTask> {

  private static final Logger log = LoggerFactory.getLogger(MetadataOperatorJavaReconciler.class);

  public UpdateControl<MetadataIngestTask> reconcile(
      MetadataIngestTask primary, Context<MetadataIngestTask> context) {

    log.info("Reconcile java platform");
    context.managedWorkflowAndDependentResourceContext().reconcileManagedWorkflow();
    return UpdateControl.noUpdate();
  }

  public DeleteControl cleanup(MetadataIngestTask primary, Context<MetadataIngestTask> context) {
    log.info("Delete java platform");
    context.managedWorkflowAndDependentResourceContext().reconcileManagedWorkflow();
    return DeleteControl.defaultDelete();
  }
}
