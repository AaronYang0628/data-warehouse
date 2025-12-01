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
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zhejianglab.astro.customresource.CronIngestTask;
import org.zhejianglab.astro.customresource.FlinkIngestTask;
import org.zhejianglab.astro.utils.StringUtils;

@KubernetesDependent
public class CronJobDependentResource
    extends CRUDKubernetesDependentResource<CronJob, CronIngestTask> {

  private static final Logger log = LoggerFactory.getLogger(CronJobDependentResource.class);

  private static final String CRON_JOB_SA_NAME = "metadata-ingest-operator-sa";

  private static final String CRON_JOB_IMAGE =
      "crpi-wixjy6gci86ms14e.cn-hongkong.personal.cr.aliyuncs.com/ay-mirror/kubectl:1.28-debian-11";

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
                                                                        + "/"
                                                                        + CRON_JOB_IMAGE
                                                                    : CRON_JOB_IMAGE)
                                                            .withImagePullPolicy("IfNotPresent")
                                                            .withCommand(
                                                                List.of(
                                                                    "/bin/sh",
                                                                    "-c",
                                                                    "cat <<'EOF' | kubectl apply -f -\n"
                                                                        + "apiVersion: org.zhejianglab.astro.metadata/v1\n"
                                                                        + "kind: "
                                                                        + FlinkIngestTask.KIND
                                                                        + "\n"
                                                                        + "metadata: \n"
                                                                        + "  name: "
                                                                        + primary
                                                                            .getMetadata()
                                                                            .getName()
                                                                        + "-"
                                                                        + System.currentTimeMillis()
                                                                        + "\n"
                                                                        + "spec: \n"
                                                                        + "  paths: "
                                                                        + primary
                                                                            .getSpec()
                                                                            .getPaths()
                                                                            .stream()
                                                                            .collect(
                                                                                Collectors.joining(
                                                                                    ","))
                                                                        + "\n"
                                                                        + "  platform: "
                                                                        + primary
                                                                            .getSpec()
                                                                            .getPlatform()
                                                                        + "\n"
                                                                        + "  jobParallelism: "
                                                                        + primary
                                                                            .getSpec()
                                                                            .getJobParallelism()
                                                                        + "\n"
                                                                        + "  allowedSuffixes: "
                                                                        + StringUtils
                                                                            .listToYamlString(
                                                                                primary
                                                                                    .getSpec()
                                                                                    .getAllowedSuffixes())
                                                                        + "\n"
                                                                        + "  tags: "
                                                                        + StringUtils
                                                                            .listToYamlString(
                                                                                primary
                                                                                    .getSpec()
                                                                                    .getTags())
                                                                        + "\n"
                                                                        + "  userProperties: "
                                                                        + StringUtils
                                                                            .mapToYamlString(
                                                                                primary
                                                                                    .getSpec()
                                                                                    .getUserProperties())
                                                                        + "\n"
                                                                        + "  extraSecret: \n"
                                                                        + "    name: "
                                                                        + primary
                                                                            .getSpec()
                                                                            .getExtraSecret()
                                                                            .getName()
                                                                        + "\n"
                                                                        + "    namespace: "
                                                                        + primary
                                                                            .getSpec()
                                                                            .getExtraSecret()
                                                                            .getNamespace()
                                                                        + "\n"
                                                                        + "    accessKeyName: "
                                                                        + primary
                                                                            .getSpec()
                                                                            .getExtraSecret()
                                                                            .getAccessKeyName()
                                                                        + "\n"
                                                                        + "    secretKeyName: "
                                                                        + primary
                                                                            .getSpec()
                                                                            .getExtraSecret()
                                                                            .getSecretKeyName()
                                                                        + "\n"
                                                                        + "    endpointKeyName: "
                                                                        + primary
                                                                            .getSpec()
                                                                            .getExtraSecret()
                                                                            .getEndpointKeyName()
                                                                        + "\n"
                                                                        + "EOF"))
                                                            .build()))
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build())
        .build();
  }
}
