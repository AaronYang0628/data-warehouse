package org.zhejianglab.astro.reconciler;

import io.fabric8.kubernetes.client.KubernetesClientException;
import io.javaoperatorsdk.operator.api.reconciler.*;
import io.javaoperatorsdk.operator.api.reconciler.dependent.Dependent;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zhejianglab.astro.customresource.VirtualIngestTask;
import org.zhejianglab.astro.dependentresource.ConfigMapDependentResource;
import org.zhejianglab.astro.dependentresource.conditions.JavaCRUDDependentCondition;

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
    implements Reconciler<VirtualIngestTask>, Cleaner<VirtualIngestTask> {

  private static final Logger log = LoggerFactory.getLogger(MetadataOperatorJavaReconciler.class);

  public UpdateControl<VirtualIngestTask> reconcile(
      VirtualIngestTask primary, Context<VirtualIngestTask> context) {

    if (primary.getMetadata().getDeletionTimestamp() != null) {
      log.info("Resource is being deleted, skip reconciliation");
      return UpdateControl.noUpdate();
    }

    List<String> finalizers = primary.getMetadata().getFinalizers();
    if (!finalizers.contains(VirtualIngestTask.FINALIZER_NAME)) {
      finalizers.add(VirtualIngestTask.FINALIZER_NAME);
      primary.getMetadata().setFinalizers(finalizers);
      return UpdateControl.patchResource(primary);
    }

    context.managedWorkflowAndDependentResourceContext().reconcileManagedWorkflow();
    if (context.isNextReconciliationImminent()) {
      // your logic, maybe return?
      log.info("Reconcile java inner logic");
    }
    return UpdateControl.noUpdate();
  }

  public DeleteControl cleanup(VirtualIngestTask primary, Context<VirtualIngestTask> context) {

    log.info("Delete java platform");
    if (primary.getMetadata().getDeletionTimestamp() == null) {
      context.managedWorkflowAndDependentResourceContext().reconcileManagedWorkflow();
    }
    return DeleteControl.defaultDelete();
  }

  @Override
  public ErrorStatusUpdateControl<VirtualIngestTask> updateErrorStatus(
      VirtualIngestTask primary, Context<VirtualIngestTask> context, Exception e) {

    if (e instanceof KubernetesClientException
        && ((KubernetesClientException) e).getCode() == 404) {
      return ErrorStatusUpdateControl.noStatusUpdate();
    }
    return handleError(primary, e);
  }

  private static ErrorStatusUpdateControl<VirtualIngestTask> handleError(
      VirtualIngestTask primary, Exception e) {
    log.error("Error occurred while reconciling task: {}", primary.getMetadata().getName(), e);

    return ErrorStatusUpdateControl.noStatusUpdate();
  }
}
