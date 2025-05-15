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
import org.zhejianglab.astro.dependentresource.FlinkDeploymentDependentCondition;
import org.zhejianglab.astro.dependentresource.FlinkDeploymentDependentResource;
import org.zhejianglab.astro.dependentresource.JavaCRUDDependentCondition;
import org.zhejianglab.astro.utils.ExceptionUtils;

@Workflow(
    explicitInvocation = true,
    dependents = {
      @Dependent(
          type = FlinkDeploymentDependentResource.class,
          activationCondition = FlinkDeploymentDependentCondition.class),
      @Dependent(
          type = ConfigMapDependentResource.class,
          activationCondition = JavaCRUDDependentCondition.class)
    })
public class MetadataOperatorFlinkReconciler
    implements Reconciler<MetadataIngestTask>, Cleaner<MetadataIngestTask> {

  private static final Logger log = LoggerFactory.getLogger(MetadataOperatorFlinkReconciler.class);

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
      log.info("Reconcile inner logic");
    }

    if (primary.getSpec().getPlatform().equalsIgnoreCase(Platform.VIRTUAL.getProtocol())) {
      log.info("virtual reconcile");
    } else {
      log.info("other reconcile");
    }

    return UpdateControl.noUpdate();
  }

  public DeleteControl cleanup(MetadataIngestTask primary, Context<MetadataIngestTask> context) {
    if (primary.getMetadata().getDeletionTimestamp() == null) {
      context.managedWorkflowAndDependentResourceContext().reconcileManagedWorkflow();
    }
    List<String> finalizers = primary.getMetadata().getFinalizers();
    finalizers.remove(MetadataIngestTask.FINALIZER_NAME);
    primary.getMetadata().setFinalizers(finalizers);

    if (primary.getSpec().getPlatform().equalsIgnoreCase(Platform.VIRTUAL.getProtocol())) {
      log.info("Delete virtual platform");

    } else {
      log.info("Delete flink platform");
    }

    return DeleteControl.defaultDelete();
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
