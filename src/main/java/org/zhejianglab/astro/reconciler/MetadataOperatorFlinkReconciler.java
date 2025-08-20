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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zhejianglab.astro.customresource.FlinkIngestTask;
import org.zhejianglab.astro.customresource.flink.ExtraSecret;
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
    log.info("A FlinkIngestTask is applied in namespace: {}", namespace);

    if (null == primary.getSpec().getExtraSecret()) {
      primary.getSpec().setExtraSecret(ExtraSecret.builder().namespace(namespace).build());
    } else {
      ExtraSecret existingSecret = primary.getSpec().getExtraSecret();
      existingSecret = existingSecret.patchInfo(namespace);
      primary.getSpec().setExtraSecret(existingSecret);
    }

    Map<String, String> result =
        retrieveExtraSecretInfo(context.getClient(), primary.getSpec().getExtraSecret());
    log.info("retrieveExtraSecretInfo -> {}", result);

    if (primary.getMetadata().getDeletionTimestamp() != null) {
      log.info("This FlinkIngestTask is being deleted, skip reconciliation");
      return UpdateControl.noUpdate();
    }

    List<String> finalizers = primary.getMetadata().getFinalizers();
    if (!finalizers.contains(FlinkIngestTask.FINALIZER_NAME)) {
      finalizers.add(FlinkIngestTask.FINALIZER_NAME);
      primary.getMetadata().setFinalizers(finalizers);
      return UpdateControl.patchResource(primary);
    }

    context.managedWorkflowAndDependentResourceContext().reconcileManagedWorkflow();
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

  private Map<String, String> retrieveExtraSecretInfo(
      KubernetesClient k8sClient, ExtraSecret extraSecret) {
    Map<String, String> result = new ConcurrentHashMap<>();
    Secret secret =
        k8sClient
            .secrets()
            .inNamespace(extraSecret.getNamespace())
            .withName(extraSecret.getName())
            .get();

    if (secret != null && secret.getData() != null) {
      Map<String, String> secretData = secret.getData();
      log.info(
          "Successfully read secret '{}' in namespace '{}'",
          extraSecret.getName(),
          extraSecret.getNamespace());

      // 打印 access key
      String accessKey = secretData.get(extraSecret.getAccessKeyName());
      if (accessKey != null) {
        String decodedAccessKey = new String(Base64.getDecoder().decode(accessKey));
        log.info("Access Key from secret: {}", decodedAccessKey);
      } else {
        log.warn("Access key '{}' not found in secret", extraSecret.getAccessKeyName());
      }

      // 打印 secret key
      String secretKey = secretData.get(extraSecret.getSecretKeyName());
      if (secretKey != null) {
        String decodedSecretKey = new String(Base64.getDecoder().decode(secretKey));
        log.info("Secret Key from secret: {}", decodedSecretKey);
      } else {
        log.warn("Secret key '{}' not found in secret", extraSecret.getSecretKeyName());
      }

      // 打印 endpoint
      String endpointKey = secretData.get(extraSecret.getEndpointKeyName());
      if (endpointKey != null) {
        String decodedEndpoint = new String(Base64.getDecoder().decode(endpointKey));
        log.info("Endpoint from secret: {}", decodedEndpoint);
      } else {
        log.warn("Endpoint key '{}' not found in secret", extraSecret.getEndpointKeyName());
      }

    } else {
      log.warn(
          "Secret '{}' not found in namespace '{}'",
          extraSecret.getName(),
          extraSecret.getNamespace());
    }
    return result;
  }
}
