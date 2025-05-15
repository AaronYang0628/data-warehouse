package org.zhejianglab.astro.reconciler;

import io.fabric8.kubernetes.client.KubernetesClientException;
import io.javaoperatorsdk.operator.api.reconciler.*;
import io.javaoperatorsdk.operator.api.reconciler.dependent.Dependent;
import java.util.List;
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

  private static final String OPERATOR_NAME = "metadataingesttasks.org.zhejianglab.astro";

  private static final String FINALIZER_NAME = OPERATOR_NAME + "/" + "finalizer";

  public UpdateControl<MetadataIngestTask> reconcile(
      MetadataIngestTask primary, Context<MetadataIngestTask> context) {

    if (primary.getMetadata().getDeletionTimestamp() != null) {
      log.info("Resource is being deleted, skip reconciliation");
      return UpdateControl.noUpdate();
    }

    List<String> finalizers = primary.getMetadata().getFinalizers();
    if (!finalizers.contains(FINALIZER_NAME)) {
      finalizers.add(FINALIZER_NAME);
      primary.getMetadata().setFinalizers(finalizers);
      // 更新 CR，触发状态变更
      return UpdateControl.patchResource(primary);
    }

    context.managedWorkflowAndDependentResourceContext().reconcileManagedWorkflow();
    if (context.isNextReconciliationImminent()) {
      // your logic, maybe return?
      log.info("Reconcile flink inner logic");
    }
    return UpdateControl.noUpdate();
  }

  public DeleteControl cleanup(MetadataIngestTask primary, Context<MetadataIngestTask> context) {
    log.info("Delete flink platform");
    if (primary.getMetadata().getDeletionTimestamp() == null) {
      context.managedWorkflowAndDependentResourceContext().reconcileManagedWorkflow();
    }

    List<String> finalizers = primary.getMetadata().getFinalizers();
    finalizers.remove(FINALIZER_NAME);
    primary.getMetadata().setFinalizers(finalizers);

    return DeleteControl.defaultDelete();
  }

  @Override
  public ErrorStatusUpdateControl<MetadataIngestTask> updateErrorStatus(
      MetadataIngestTask primary, Context<MetadataIngestTask> context, Exception e) {

    // 如果资源已不存在，跳过状态更新
    if (e instanceof KubernetesClientException
        && ((KubernetesClientException) e).getCode() == 404) {
      return ErrorStatusUpdateControl.noStatusUpdate();
    }
    return ExceptionUtils.handleError(primary, e);
  }
}
