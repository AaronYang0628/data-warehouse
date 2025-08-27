package org.zhejianglab.astro.reconciler;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.javaoperatorsdk.operator.api.reconciler.*;
import io.javaoperatorsdk.operator.api.reconciler.dependent.Dependent;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.flink.api.common.JobStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zhejianglab.astro.customresource.FlinkIngestTask;
import org.zhejianglab.astro.customresource.flink.ExtraSecret;
import org.zhejianglab.astro.customresource.flink.FlinkIngestTaskSpec;
import org.zhejianglab.astro.customresource.flink.FlinkIngestTaskStatus;
import org.zhejianglab.astro.dependentresource.FlinkSessionJobDependentResource;
import org.zhejianglab.astro.dependentresource.conditions.FlinkSessionJobDependentCondition;
import org.zhejianglab.astro.exception.BugException;
import org.zhejianglab.astro.utils.SecretConstant;

@Workflow(
    explicitInvocation = true,
    handleExceptionsInReconciler = true,
    dependents = {
      @Dependent(
          type = FlinkSessionJobDependentResource.class,
          reconcilePrecondition = FlinkSessionJobDependentCondition.class),
    })
public class MetadataOperatorFlinkReconciler
    implements Reconciler<FlinkIngestTask>, Cleaner<FlinkIngestTask> {

  private static final Logger log = LoggerFactory.getLogger(MetadataOperatorFlinkReconciler.class);

  private volatile boolean errorsFoundInReconcilerResult = false;

  private volatile boolean primaryResourceNeedUpdate = false;

  public UpdateControl<FlinkIngestTask> reconcile(
      FlinkIngestTask primary, Context<FlinkIngestTask> context) {

    context
        .managedWorkflowAndDependentResourceContext()
        .getWorkflowReconcileResult()
        .ifPresent(
            result -> {
              log.info(
                  result.erroredDependentsExist()
                      ? "Error found in dependent resource"
                      : "No error found in dependent resource");
            });

    try {

      String namespace = primary.getMetadata().getNamespace();
      log.info("A FlinkIngestTask is applied in namespace: {}", namespace);

      if (primary.getStatus() == null) {
        primary.setStatus(
            FlinkIngestTaskStatus.builder().jobStatus(JobStatus.INITIALIZING).build());
        primaryResourceNeedUpdate = true;
      }

      if (!validateAppliedResource(primary)) {
        updateErrorStatus(
            primary,
            context,
            new IllegalArgumentException("An Invalid FlinkIngestTask resource applied."));
        // return UpdateControl.patchStatus(primary);
      }

      if (primary.getSpec().getBatchId() != null) {
        primary.getStatus().setBatchId(primary.getSpec().getBatchId());
        primary.getStatus().setJobStatus(JobStatus.RECONCILING.name());
        primaryResourceNeedUpdate = true;
      }

      if (null == primary.getSpec().getExtraSecret()) {
        primary.getSpec().setExtraSecret(ExtraSecret.builder().namespace(namespace).build());
        primary.getStatus().setJobStatus(JobStatus.RUNNING.name());
        primaryResourceNeedUpdate = true;
      } else {
        ExtraSecret existingSecret = primary.getSpec().getExtraSecret();
        existingSecret = existingSecret.patchInfo(namespace);
        primary.getSpec().setExtraSecret(existingSecret);
        primary.getStatus().setJobStatus(JobStatus.RECONCILING.name());
        primaryResourceNeedUpdate = true;
      }

      ExtraSecret extraSecretWithData =
          retrieveExtraSecretInfo(context.getClient(), primary.getSpec().getExtraSecret());

      if (!extraSecretWithData.getSecretData().isEmpty()) {
        log.info("Updating FlinkIngestTask with new secret data");
        primary.getSpec().setExtraSecret(extraSecretWithData);
        primary.getStatus().setJobStatus(JobStatus.RUNNING.name());
        primaryResourceNeedUpdate = true;
      } else {
        updateErrorStatus(
            primary,
            context,
            new IllegalAccessException(
                "cannot access to the object storage, due to bad crediential."));
      }

      if (primary.getMetadata().getDeletionTimestamp() != null) {
        log.info("This FlinkIngestTask is being deleted, skip reconciliation");
        primary.getStatus().setJobStatus(JobStatus.FAILING.name());
        return UpdateControl.patchStatus(primary);
      }

      List<String> finalizers = primary.getMetadata().getFinalizers();
      if (!finalizers.contains(FlinkIngestTask.FINALIZER_NAME)) {
        finalizers.add(FlinkIngestTask.FINALIZER_NAME);
        primary.getMetadata().setFinalizers(finalizers);
        primary.getStatus().setJobStatus(JobStatus.RUNNING.name());
        primaryResourceNeedUpdate = true;
      }

      if (primaryResourceNeedUpdate) {
        primary.getMetadata().setManagedFields(null);
        log.debug("Updating FlinkIngestTask Spec: {}", primary.getSpec());
      }

      context.managedWorkflowAndDependentResourceContext().reconcileManagedWorkflow();

      return primaryResourceNeedUpdate
          ? UpdateControl.patchResourceAndStatus(primary)
          : UpdateControl.noUpdate();

    } catch (Exception e) {
      log.error(
          "BUG!!! -> Error during reconciliation of resource {}: {}",
          primary.getMetadata().getName(),
          e.getMessage(),
          e);

      updateErrorStatus(
          primary, context, new BugException("call the developers to fix the bug.", e));
      return UpdateControl.patchResourceAndStatus(primary);
    }
  }

  public DeleteControl cleanup(FlinkIngestTask primary, Context<FlinkIngestTask> context) {
    if (primary.getMetadata().getDeletionTimestamp() == null) {
      context.managedWorkflowAndDependentResourceContext().reconcileManagedWorkflow();
    }

    return DeleteControl.defaultDelete();
  }

  private boolean validateAppliedResource(FlinkIngestTask resource) {
    FlinkIngestTaskSpec spec = resource.getSpec();

    return true;
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
    primary.getStatus().setJobStatus(JobStatus.FAILED.name());
    primary.getStatus().setException(null == e ? e.getLocalizedMessage() : "Unknown bug error");
    return ErrorStatusUpdateControl.patchStatus(primary);
  }

  private ExtraSecret retrieveExtraSecretInfo(KubernetesClient k8sClient, ExtraSecret extraSecret) {
    Map<String, String> result = new ConcurrentHashMap<>();
    Secret secret =
        k8sClient
            .secrets()
            .inNamespace(extraSecret.getNamespace())
            .withName(extraSecret.getName())
            .get();

    if (secret != null && secret.getData() != null) {
      Map<String, String> secretData = secret.getData();
      log.debug(
          "Successfully read secret '{}' in namespace '{}'",
          extraSecret.getName(),
          extraSecret.getNamespace());

      String accessKey = secretData.get(extraSecret.getAccessKeyName());
      if (accessKey != null) {
        String decodedAccessKey = new String(Base64.getDecoder().decode(accessKey));
        log.debug("Access Key from secret: {}", decodedAccessKey);
        result.put(SecretConstant.ACCESS_KEY_UP_NAME, decodedAccessKey);
      } else {
        log.warn("Access key '{}' not found in secret", extraSecret.getAccessKeyName());
      }
      String secretKey = secretData.get(extraSecret.getSecretKeyName());
      if (secretKey != null) {
        String decodedSecretKey = new String(Base64.getDecoder().decode(secretKey));
        log.debug("Secret Key from secret: {}", decodedSecretKey);
        result.put(SecretConstant.SECRET_KEY_UP_NAME, decodedSecretKey);
      } else {
        log.warn("Secret key '{}' not found in secret", extraSecret.getSecretKeyName());
      }
      String endpointKey = secretData.get(extraSecret.getEndpointKeyName());
      if (endpointKey != null) {
        String decodedEndpoint = new String(Base64.getDecoder().decode(endpointKey));
        log.debug("Endpoint from secret: {}", decodedEndpoint);
        result.put(SecretConstant.ENDPOINT_KEY_UP_NAME, decodedEndpoint);
      } else {
        log.warn("Endpoint key '{}' not found in secret", extraSecret.getEndpointKeyName());
      }
    } else {
      log.warn(
          "Secret '{}' not found in namespace '{}'",
          extraSecret.getName(),
          extraSecret.getNamespace());
    }

    extraSecret.setSecretData(result);
    log.info("Extra secret information retrieved: {}", extraSecret.getSecretData());

    return extraSecret;
  }
}
