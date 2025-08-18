package org.zhejianglab.astro.reconciler;

import io.fabric8.kubernetes.client.KubernetesClientException;
import io.javaoperatorsdk.operator.api.reconciler.*;
import io.javaoperatorsdk.operator.api.reconciler.dependent.Dependent;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zhejianglab.astro.customresource.FlinkIngestTask;
import org.zhejianglab.astro.customresource.Platform;
import org.zhejianglab.astro.dependentresource.FlinkSessionJobDependentResource;
import org.zhejianglab.astro.dependentresource.conditions.FlinkSessionJobDependentCondition;

@Workflow(
    explicitInvocation = true,
    dependents = {
      @Dependent(
          type = FlinkSessionJobDependentResource.class,
          reconcilePrecondition = FlinkSessionJobDependentCondition.class),
    })
public class MetadataOperatorFlinkReconciler
    implements Reconciler<FlinkIngestTask>, Cleaner<FlinkIngestTask> {

  private static final Logger log = LoggerFactory.getLogger(MetadataOperatorFlinkReconciler.class);

  public UpdateControl<FlinkIngestTask> reconcile(
      FlinkIngestTask primary, Context<FlinkIngestTask> context) {

    String namespace = primary.getMetadata().getNamespace();
    log.info("FlinkIngestTask is applied in namespace: {}", namespace);
    if (!primary.getSpec().getExtraSecret().contentEquals(".")) {
      String extraSecretName = primary.getSpec().getExtraSecret();
      primary.getSpec().setExtraSecret(namespace + "." + extraSecretName);
    }

    if (primary.getMetadata().getDeletionTimestamp() != null) {
      log.info("Resource is being deleted, skip reconciliation");
      return UpdateControl.noUpdate();
    }

    List<String> finalizers = primary.getMetadata().getFinalizers();
    if (!finalizers.contains(FlinkIngestTask.FINALIZER_NAME)) {
      finalizers.add(FlinkIngestTask.FINALIZER_NAME);
      primary.getMetadata().setFinalizers(finalizers);
      return UpdateControl.patchResource(primary);
    }

    context.managedWorkflowAndDependentResourceContext().reconcileManagedWorkflow();

    if (context.isNextReconciliationImminent()) {
      log.info("Reconcile inner logic");
    }

    if (primary.getSpec().getPlatform().equalsIgnoreCase(Platform.VIRTUAL.getProtocol())) {
      log.info("virtual reconcile");
    } else {
      log.info("other reconcile");
    }

    return UpdateControl.noUpdate();
  }

  public DeleteControl cleanup(FlinkIngestTask primary, Context<FlinkIngestTask> context) {
    if (primary.getMetadata().getDeletionTimestamp() == null) {
      context.managedWorkflowAndDependentResourceContext().reconcileManagedWorkflow();
    }

    return DeleteControl.defaultDelete();
  }

  @Override
  public ErrorStatusUpdateControl<FlinkIngestTask> updateErrorStatus(
      FlinkIngestTask primary, Context<FlinkIngestTask> context, Exception e) {

    if (e instanceof KubernetesClientException
        && ((KubernetesClientException) e).getCode() == 404) {
      return ErrorStatusUpdateControl.noStatusUpdate();
    }
    return handleError(primary, e);
  }

  private static ErrorStatusUpdateControl<FlinkIngestTask> handleError(
      FlinkIngestTask primary, Exception e) {
    log.error("Error occurred while reconciling task: {}", primary.getMetadata().getName(), e);

    return ErrorStatusUpdateControl.noStatusUpdate();
  }
}
