package org.zhejianglab.astro.reconciler;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.javaoperatorsdk.operator.api.reconciler.*;
import io.javaoperatorsdk.operator.api.reconciler.dependent.Dependent;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.kubernetes.operator.api.FlinkSessionJob;
import org.apache.flink.kubernetes.operator.api.spec.JobState;
import org.apache.flink.kubernetes.operator.api.status.FlinkSessionJobStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zhejianglab.astro.customresource.FlinkIngestTask;
import org.zhejianglab.astro.customresource.enums.IngestStatus;
import org.zhejianglab.astro.customresource.flink.ExtraSecret;
import org.zhejianglab.astro.customresource.flink.FlinkIngestTaskSpec;
import org.zhejianglab.astro.customresource.flink.FlinkIngestTaskStatus;
import org.zhejianglab.astro.dependentresource.FinishedAuditJobDependentResource;
import org.zhejianglab.astro.dependentresource.FlinkSessionJobDependentResource;
import org.zhejianglab.astro.dependentresource.conditions.FlinkSessionJobDependentCondition;
import org.zhejianglab.astro.utils.SecretConstant;

@ControllerConfiguration(
    generationAwareEventProcessing = false,
    name = "metadataoperatorflinkreconciler")
@Workflow(
    explicitInvocation = true,
    dependents = {
      @Dependent(
          type = FlinkSessionJobDependentResource.class,
          reconcilePrecondition = FlinkSessionJobDependentCondition.class),
      @Dependent(type = FinishedAuditJobDependentResource.class)
    })
public class MetadataOperatorFlinkReconciler
    implements Reconciler<FlinkIngestTask>, Cleaner<FlinkIngestTask> {

  private static final Logger log = LoggerFactory.getLogger(MetadataOperatorFlinkReconciler.class);

  public UpdateControl<FlinkIngestTask> reconcile(
      FlinkIngestTask primary, Context<FlinkIngestTask> context) {

    try {
      boolean primarySpecNeedUpdate = false;
      boolean primaryStatusNeedUpdate = false;

      String namespace = primary.getMetadata().getNamespace();
      log.info(
          "A FlinkIngestTask is applied in namespace: {}, and content is {}",
          namespace,
          primary.getSpec().toString());

      if (primary.getStatus() == null) {
        primary.setStatus(
            FlinkIngestTaskStatus.builder()
                .jobStatus(JobStatus.INITIALIZING)
                .ingestStatus(IngestStatus.INGESTING)
                .build());
        primaryStatusNeedUpdate = true;
      } else {
        primary.getStatus().setException("");
        primaryStatusNeedUpdate = true;
      }

      if (!validateAppliedResource(primary)) {
        updateErrorStatus(
            primary,
            context,
            new IllegalArgumentException("An Invalid FlinkIngestTask resource applied."));
        return UpdateControl.patchStatus(primary);
      }

      if (primary.getSpec().getBatchId() != null
          && !primary.getSpec().getBatchId().equals(primary.getStatus().getBatchId())) {
        primary.getStatus().setBatchId(primary.getSpec().getBatchId());
        primary.getStatus().setJobStatus(JobStatus.INITIALIZING.name());
        primaryStatusNeedUpdate = true;
      }

      if (null == primary.getSpec().getExtraSecret()) {
        primary.getSpec().setExtraSecret(ExtraSecret.builder().namespace(namespace).build());
        primarySpecNeedUpdate = true;
      } else {
        ExtraSecret existingSecret = primary.getSpec().getExtraSecret();
        existingSecret = existingSecret.patchInfo(namespace);
        primary.getSpec().setExtraSecret(existingSecret);
        primarySpecNeedUpdate = true;
      }

      ExtraSecret extraSecretWithData =
          retrieveExtraSecretInfo(context.getClient(), primary.getSpec().getExtraSecret());

      if (!extraSecretWithData.getSecretData().isEmpty()) {
        log.debug("Updating FlinkIngestTask with new secret data");
        primary.getSpec().setExtraSecret(extraSecretWithData);
        primarySpecNeedUpdate = true;
      }

      if (null == primary.getSpec().getExtraEnvs()) {
        primary
            .getSpec()
            .setExtraEnvs(org.zhejianglab.astro.customresource.flink.ExtraEnvs.builder().build());
        primarySpecNeedUpdate = true;
      } else {
        primary
            .getSpec()
            .setExtraEnvs(org.zhejianglab.astro.customresource.flink.ExtraEnvs.builder().build());
        primary.getSpec().setExtraEnvs(primary.getSpec().getExtraEnvs());
        primarySpecNeedUpdate = true;
      }

      if (primary.getMetadata().getDeletionTimestamp() != null) {
        log.info("This FlinkIngestTask is being deleted, skip reconciliation");
        primary.getStatus().setJobStatus(JobStatus.CANCELLING.name());
        return UpdateControl.patchStatus(primary);
      }

      Optional<FlinkSessionJob> flinkSessionJobOptional =
          retrieveFlinkSessionJobInfo(context.getClient(), primary);

      if (flinkSessionJobOptional.isPresent()) {
        FlinkSessionJob flinkSessionJob = flinkSessionJobOptional.get();
        FlinkSessionJobStatus flinkSessionJobStatus = flinkSessionJob.getStatus();

        if (flinkSessionJobStatus != null) {
          log.debug("Got Corresponding FlinkSessionJob status: {}", flinkSessionJobStatus);

          if (flinkSessionJobStatus.getJobStatus() != null) {
            if (flinkSessionJobStatus.getJobStatus().getState() != null) {
              String jobState = flinkSessionJobStatus.getJobStatus().getState().name();
              primary.getStatus().setJobStatus(jobState);
              if (primary.getStatus().getIngestStatus() != IngestStatus.FINISHED) {
                primary.getStatus().setIngestStatus(IngestStatus.INGESTING);
              }

              suspendFlinkSessionJobIfFinished(
                  context.getClient(), primary, flinkSessionJob, jobState);

              primaryStatusNeedUpdate = true;
            } else {
              primary.getStatus().setJobStatus(JobStatus.INITIALIZING.name());
            }
            primary.getStatus().setException(flinkSessionJobStatus.getError());
            primaryStatusNeedUpdate = true;
          }
        }
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

  public DeleteControl cleanup(FlinkIngestTask primary, Context<FlinkIngestTask> context) {
    if (primary.getMetadata().getDeletionTimestamp() == null) {
      context.managedWorkflowAndDependentResourceContext().reconcileManagedWorkflow();
    }

    primary.getMetadata().setManagedFields(null);
    log.info("FlinkIngestTask {} cleaned up successfully", primary.getMetadata().getName());
    return DeleteControl.defaultDelete();
  }

  @Override
  public ErrorStatusUpdateControl<FlinkIngestTask> updateErrorStatus(
      FlinkIngestTask primary, Context<FlinkIngestTask> context, Exception e) {

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

  private void suspendFlinkSessionJobIfFinished(
      KubernetesClient k8sClient,
      FlinkIngestTask primary,
      FlinkSessionJob flinkSessionJob,
      String jobState) {

    if ("FINISHED".equals(jobState)) {
      log.info(
          "Flink job {} has finished. Suspending FlinkSessionJob.",
          primary.getMetadata().getName());

      primary.getStatus().setIngestStatus(IngestStatus.FINISHED);

      if (flinkSessionJob.getSpec().getJob().getState() != JobState.SUSPENDED) {
        flinkSessionJob.getSpec().getJob().setState(JobState.SUSPENDED);

        try {
          k8sClient
              .resources(FlinkSessionJob.class)
              .inNamespace(primary.getMetadata().getNamespace())
              .withName(primary.getMetadata().getName())
              .patch(flinkSessionJob);

          log.info("Successfully suspended FlinkSessionJob {}", primary.getMetadata().getName());
        } catch (KubernetesClientException e) {
          if (e.getCode() == 409) {
            log.warn(
                "Conflict when suspending FlinkSessionJob {}, will retry in next reconciliation",
                primary.getMetadata().getName());
          } else {
            log.error(
                "Failed to suspend FlinkSessionJob {}: {}",
                primary.getMetadata().getName(),
                e.getMessage(),
                e);
          }
        } catch (Exception e) {
          log.error(
              "Unexpected error when suspending FlinkSessionJob {}: {}",
              primary.getMetadata().getName(),
              e.getMessage(),
              e);
        }
      } else {
        log.debug("FlinkSessionJob {} is already suspended", primary.getMetadata().getName());
      }
    }
  }

  private boolean validateAppliedResource(FlinkIngestTask resource) {
    FlinkIngestTaskSpec spec = resource.getSpec();

    return true;
  }

  private static ErrorStatusUpdateControl<FlinkIngestTask> handleError(
      FlinkIngestTask primary, Exception e) {
    log.error("Error occurred while reconciling task: {}", primary.getMetadata().getName(), e);

    return ErrorStatusUpdateControl.noStatusUpdate();
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
    log.debug("Extra secret information retrieved: {}", extraSecret.getSecretData());

    return extraSecret;
  }

  private Optional<FlinkSessionJob> retrieveFlinkSessionJobInfo(
      KubernetesClient k8sClient, FlinkIngestTask primary) {

    try {
      String namespace = primary.getMetadata().getNamespace();
      String name = primary.getMetadata().getName();

      FlinkSessionJob sessionJob =
          k8sClient.resources(FlinkSessionJob.class).inNamespace(namespace).withName(name).get();

      return Optional.ofNullable(sessionJob);
    } catch (Exception e) {
      log.error("Failed to retrieve FlinkSessionJob: {}", e.getMessage());
      return Optional.empty();
    }
  }
}
