package org.zhejianglab.astro.dependentresource;

import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.PodSpecBuilder;
import io.fabric8.kubernetes.api.model.PodTemplateSpecBuilder;
import io.fabric8.kubernetes.api.model.batch.v1.CronJob;
import io.fabric8.kubernetes.api.model.batch.v1.CronJobBuilder;
import io.fabric8.kubernetes.api.model.batch.v1.CronJobSpecBuilder;
import io.fabric8.kubernetes.api.model.batch.v1.JobSpecBuilder;
import io.fabric8.kubernetes.api.model.batch.v1.JobTemplateSpecBuilder;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.CRUDKubernetesDependentResource;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.KubernetesDependent;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zhejianglab.astro.customresource.CronIngestTask;
import org.zhejianglab.astro.customresource.FlinkIngestTask;
import org.zhejianglab.astro.utils.StringUtils;

@KubernetesDependent
public class CronJobDependentResource
    extends CRUDKubernetesDependentResource<CronJob, CronIngestTask> {

  private static final Logger log = LoggerFactory.getLogger(CronJobDependentResource.class);

  private static final String CRON_JOB_SA_NAME = "metadata-ingest-flink-sa";

  private static final String CRON_JOB_IMAGE = "docker.io/bitnami/kubectl:1.28-debian-11";

  public CronJobDependentResource() {
    super(CronJob.class);
  }

  @Override
  protected CronJob desired(CronIngestTask primary, Context<CronIngestTask> context) {
    return new CronJobBuilder()
        .withMetadata(
            new ObjectMetaBuilder()
                .withName(primary.getMetadata().getName())
                .withNamespace(primary.getMetadata().getNamespace())
                .build())
        .withSpec(
            new CronJobSpecBuilder()
                .withSchedule(primary.getSpec().getCron())
                .withSuspend(primary.getSpec().getSuspend())
                .withConcurrencyPolicy("Forbid")
                .withFailedJobsHistoryLimit(1)
                .withSuccessfulJobsHistoryLimit(3)
                .withStartingDeadlineSeconds(primary.getSpec().getDelay())
                .withJobTemplate(
                    new JobTemplateSpecBuilder()
                        .withSpec(
                            new JobSpecBuilder()
                                .withTemplate(
                                    new PodTemplateSpecBuilder()
                                        .withSpec(
                                            new PodSpecBuilder()
                                                .withServiceAccountName(CRON_JOB_SA_NAME)
                                                .withRestartPolicy("Never")
                                                .withContainers(
                                                    List.of(
                                                        new ContainerBuilder()
                                                            .withName("submit-flink-ingest-task")
                                                            .withImage(
                                                                primary.getSpec().getImageMirror()
                                                                        != null
                                                                    ? primary
                                                                            .getSpec()
                                                                            .getImageMirror()
                                                                        + CRON_JOB_IMAGE
                                                                    : CRON_JOB_IMAGE)
                                                            .withImagePullPolicy("IfNotPresent")
                                                            .withCommand(
                                                                List.of(
                                                                    "/bin/sh",
                                                                    "-c",
                                                                    "|",
                                                                    "cat <<EOF | kubectl apply -f -",
                                                                    "apiVersion: astro.zhejianglab.org/v1",
                                                                    "kind:" + FlinkIngestTask.KIND,
                                                                    "metadata:",
                                                                    "  name: "
                                                                        + primary
                                                                            .getMetadata()
                                                                            .getName()
                                                                        + "-$(date +%s)",
                                                                    "spec:",
                                                                    "  path: "
                                                                        + primary
                                                                            .getSpec()
                                                                            .getPath(),
                                                                    "  platform: "
                                                                        + primary
                                                                            .getSpec()
                                                                            .getPlatform(),
                                                                    "  jobParallelism: "
                                                                        + primary
                                                                            .getSpec()
                                                                            .getJobParallelism(),
                                                                    "  s3TableName: "
                                                                        + primary
                                                                            .getSpec()
                                                                            .getS3TableName(),
                                                                    "  allowedSuffixes: "
                                                                        + StringUtils
                                                                            .listToYamlString(
                                                                                primary
                                                                                    .getSpec()
                                                                                    .getAllowedSuffixes()),
                                                                    "  tags: "
                                                                        + StringUtils
                                                                            .listToYamlString(
                                                                                primary
                                                                                    .getSpec()
                                                                                    .getTags()),
                                                                    "  userProperties: "
                                                                        + StringUtils
                                                                            .mapToYamlString(
                                                                                primary
                                                                                    .getSpec()
                                                                                    .getUserProperties()),
                                                                    "  pathPatterns: "
                                                                        + primary
                                                                            .getSpec()
                                                                            .getPathPatterns(),
                                                                    "  extraSecret: ",
                                                                    "    name: "
                                                                        + primary
                                                                            .getSpec()
                                                                            .getExtraSecret()
                                                                            .getName(),
                                                                    "    namespace: "
                                                                        + primary
                                                                            .getSpec()
                                                                            .getExtraSecret()
                                                                            .getNamespace(),
                                                                    "   accessKeyName: "
                                                                        + primary
                                                                            .getSpec()
                                                                            .getExtraSecret()
                                                                            .getAccessKeyName(),
                                                                    "   secretKeyName: "
                                                                        + primary
                                                                            .getSpec()
                                                                            .getExtraSecret()
                                                                            .getSecretKeyName(),
                                                                    "   endpointKeyName: "
                                                                        + primary
                                                                            .getSpec()
                                                                            .getExtraSecret()
                                                                            .getEndpointKeyName(),
                                                                    "EOF"))
                                                            .build()))
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build())
        .build();
  }
}
