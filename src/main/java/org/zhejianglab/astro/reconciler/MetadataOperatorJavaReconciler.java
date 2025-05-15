package org.zhejianglab.astro.reconciler;

import io.fabric8.kubernetes.client.KubernetesClientException;
import io.javaoperatorsdk.operator.api.reconciler.*;
import io.javaoperatorsdk.operator.api.reconciler.dependent.Dependent;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zhejianglab.astro.customresource.MetadataIngestTask;
import org.zhejianglab.astro.customresource.Platform;
import org.zhejianglab.astro.dependentresource.ConfigMapDependentResource;
import org.zhejianglab.astro.dependentresource.JavaCRUDDependentCondition;
import org.zhejianglab.astro.utils.ExceptionUtils;

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

    if (primary.getMetadata().getDeletionTimestamp() != null) {
      log.info("Resource is being deleted, skip reconciliation");
      return UpdateControl.noUpdate();
    }

    List<String> finalizers = primary.getMetadata().getFinalizers();
    if (!finalizers.contains(MetadataIngestTask.FINALIZER_NAME)) {
      finalizers.add(MetadataIngestTask.FINALIZER_NAME);
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

  public DeleteControl cleanup(MetadataIngestTask primary, Context<MetadataIngestTask> context) {
    if (primary.getSpec().getPlatform().equalsIgnoreCase(Platform.VIRTUAL.getProtocol())) {

      log.info("Delete java platform");
      if (primary.getMetadata().getDeletionTimestamp() == null) {
        context.managedWorkflowAndDependentResourceContext().reconcileManagedWorkflow();
      }

      List<String> finalizers = primary.getMetadata().getFinalizers();
      finalizers.remove(MetadataIngestTask.FINALIZER_NAME);
      primary.getMetadata().setFinalizers(finalizers);
      return DeleteControl.defaultDelete();
    }
    return DeleteControl.noFinalizerRemoval();
  }

  @Override
  public ErrorStatusUpdateControl<MetadataIngestTask> updateErrorStatus(
      MetadataIngestTask primary, Context<MetadataIngestTask> context, Exception e) {

    if (e instanceof KubernetesClientException
        && ((KubernetesClientException) e).getCode() == 404) {
      return ErrorStatusUpdateControl.noStatusUpdate();
    }
    return ExceptionUtils.handleError(primary, e);
  }
}
