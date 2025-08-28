package org.zhejianglab.astro.reconciler;

import io.fabric8.kubernetes.api.model.batch.v1.CronJob;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.javaoperatorsdk.operator.api.reconciler.*;
import io.javaoperatorsdk.operator.api.reconciler.dependent.Dependent;
import java.util.List;
import java.util.Optional;
import org.apache.flink.kubernetes.operator.api.lifecycle.ResourceLifecycleState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zhejianglab.astro.customresource.CronIngestTask;
import org.zhejianglab.astro.customresource.FlinkIngestTask;
import org.zhejianglab.astro.customresource.cron.CronIngestTaskSpec;
import org.zhejianglab.astro.customresource.cron.CronIngestTaskStatus;
import org.zhejianglab.astro.dependentresource.CronJobDependentResource;
import org.zhejianglab.astro.dependentresource.conditions.CronFlinkSessionJobDependentCondition;

@ControllerConfiguration(
    generationAwareEventProcessing = false,
    name = "metadataoperatorcronconciler")
@Workflow(
    explicitInvocation = true,
    dependents = {
      @Dependent(
          type = CronJobDependentResource.class,
          reconcilePrecondition = CronFlinkSessionJobDependentCondition.class),
    })
public class MetadataOperatorCronReconciler
    implements Reconciler<CronIngestTask>, Cleaner<CronIngestTask> {

  private static final Logger log = LoggerFactory.getLogger(MetadataOperatorCronReconciler.class);

  public UpdateControl<CronIngestTask> reconcile(
      CronIngestTask primary, Context<CronIngestTask> context) {

    try {
      boolean primarySpecNeedUpdate = false;
      boolean primaryStatusNeedUpdate = false;

      String namespace = primary.getMetadata().getNamespace();
      log.info("A CronIngestTask is applied in namespace: {}", namespace);

      if (primary.getStatus() == null) {
        primary.setStatus(
            CronIngestTaskStatus.builder().jobStatus(ResourceLifecycleState.CREATED).build());
        primaryStatusNeedUpdate = true;
      }

      if (!validateAppliedResource(primary)) {
        updateErrorStatus(
            primary,
            context,
            new IllegalArgumentException("An Invalid FlinkIngestTask resource applied."));
        return UpdateControl.patchStatus(primary);
      }

      if (primary.getMetadata().getDeletionTimestamp() != null) {
        log.info("This FlinkIngestTask is being deleted, skip reconciliation");
        primary.getStatus().setJobStatus(ResourceLifecycleState.DELETING.name());
        return UpdateControl.patchStatus(primary);
      }

      Optional<CronJob> cronJobOptional = retrieveCronJobInfo(context.getClient(), primary);

      if (cronJobOptional.isPresent()) {
        CronJob generatedCronJob = cronJobOptional.get();
        primary.getStatus().setSchedule(generatedCronJob.getSpec().getSchedule());
        primaryStatusNeedUpdate = true;
      }

      List<String> finalizers = primary.getMetadata().getFinalizers();
      if (!finalizers.contains(FlinkIngestTask.FINALIZER_NAME)) {
        finalizers.add(FlinkIngestTask.FINALIZER_NAME);
        primary.getMetadata().setFinalizers(finalizers);
        primarySpecNeedUpdate = true;
      }

      if (primarySpecNeedUpdate) {
        primary.getMetadata().setManagedFields(null);
        log.debug("Updating FlinkIngestTask Status: {}", primary.getSpec());
      }

      context.managedWorkflowAndDependentResourceContext().reconcileManagedWorkflow();

      if (primarySpecNeedUpdate && primaryStatusNeedUpdate) {
        primary.getMetadata().setManagedFields(null);
        return UpdateControl.patchResourceAndStatus(primary);
      } else if (primarySpecNeedUpdate) {
        primary.getMetadata().setManagedFields(null);
        return UpdateControl.patchResource(primary);
      } else if (primaryStatusNeedUpdate) {
        return UpdateControl.patchStatus(primary);
      }

      return UpdateControl.noUpdate();
    } catch (KubernetesClientException e) {
      if (e.getCode() == 409 || e.getCode() == 404) {
        log.warn(
            "Resource conflict detected for {}, will retry in next reconciliation: {}",
            primary.getMetadata().getName(),
            e.getMessage());
      }
      return UpdateControl.noUpdate();
    } catch (Exception e) {
      log.error(
          "BUG!!! -> Error during reconciliation of resource {}: {}",
          primary.getMetadata().getName(),
          e.getMessage(),
          e);

      updateErrorStatus(primary, context, e);
      return UpdateControl.patchStatus(primary);
    }
  }

  @Override
  public DeleteControl cleanup(CronIngestTask primary, Context<CronIngestTask> context)
      throws Exception {
    if (primary.getMetadata().getDeletionTimestamp() == null) {
      context.managedWorkflowAndDependentResourceContext().reconcileManagedWorkflow();
    }

    primary.getMetadata().setManagedFields(null);
    log.info("CronIngestTask {} cleaned up successfully", primary.getMetadata().getName());
    return DeleteControl.defaultDelete();
  }

  @Override
  public ErrorStatusUpdateControl<CronIngestTask> updateErrorStatus(
      CronIngestTask primary, Context<CronIngestTask> context, Exception e) {

    if (e instanceof KubernetesClientException
        && ((KubernetesClientException) e).getCode() == 404) {
      return ErrorStatusUpdateControl.noStatusUpdate();
    }

    if (e instanceof KubernetesClientException
        && ((KubernetesClientException) e).getCode() == 409) {
      return ErrorStatusUpdateControl.patchStatus(primary);
    }

    return handleError(primary, e);
  }

  private boolean validateAppliedResource(CronIngestTask resource) {
    CronIngestTaskSpec spec = resource.getSpec();

    return true;
  }

  private static ErrorStatusUpdateControl<CronIngestTask> handleError(
      CronIngestTask primary, Exception e) {
    log.error("Error occurred while reconciling task: {}", primary.getMetadata().getName(), e);

    return ErrorStatusUpdateControl.noStatusUpdate();
  }

  private Optional<CronJob> retrieveCronJobInfo(
      KubernetesClient k8sClient, CronIngestTask primary) {

    try {
      String namespace = primary.getMetadata().getNamespace();
      String name = primary.getMetadata().getName();

      CronJob cronJob =
          k8sClient.resources(CronJob.class).inNamespace(namespace).withName(name).get();

      return Optional.ofNullable(cronJob);
    } catch (Exception e) {
      log.error("Failed to retrieve CronJob: {}", e.getMessage());
      return Optional.empty();
    }
  }
}
