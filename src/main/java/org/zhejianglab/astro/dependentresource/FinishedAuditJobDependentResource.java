package org.zhejianglab.astro.dependentresource;

import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobBuilder;
import io.fabric8.kubernetes.api.model.batch.v1.JobSpecBuilder;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.CRUDKubernetesDependentResource;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.KubernetesDependent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zhejianglab.astro.customresource.FlinkIngestTask;

@KubernetesDependent
public class FinishedAuditJobDependentResource
    extends CRUDKubernetesDependentResource<Job, FlinkIngestTask> {

  private static final Logger log =
      LoggerFactory.getLogger(FinishedAuditJobDependentResource.class);

  public static final String AUDIT_JOB_NAME_SUFFIX = "-finished-audit-job";

  private static final String CRON_JOB_SA_NAME = "metadata-ingest-operator-sa";
  private static final String KAFKA_ES_IMAGE =
      "m.daocloud.io/docker.io/bitnami/kubectl:1.28-debian-11";

  public FinishedAuditJobDependentResource() {
    super(Job.class);
  }

  @Override
  protected Job desired(FlinkIngestTask primary, Context<FlinkIngestTask> context) {

    String jobName = primary.getMetadata().getName() + AUDIT_JOB_NAME_SUFFIX;

    log.info("Creating audit job {} for finished task", jobName);

    return new JobBuilder()
        .withMetadata(
            new ObjectMetaBuilder()
                .withName(jobName)
                .withNamespace(primary.getMetadata().getNamespace())
                .build())
        .withSpec(
            new JobSpecBuilder()
                .withBackoffLimit(3)
                .withTemplate(
                    new io.fabric8.kubernetes.api.model.PodTemplateSpecBuilder()
                        .withNewMetadata()
                        .withName(jobName + "-template")
                        .endMetadata()
                        .withNewSpec()
                        .withRestartPolicy("OnFailure")
                        .withServiceAccountName(CRON_JOB_SA_NAME)
                        .addNewContainer()
                        .withName("audit")
                        .withImage(KAFKA_ES_IMAGE)
                        .withCommand("sh", "-c", buildKafkaEsCommand(primary))
                        .endContainer()
                        .endSpec()
                        .build())
                .build())
        .build();
  }

  private String buildKafkaEsCommand(FlinkIngestTask primary) {
    StringBuilder command = new StringBuilder();

    command
        .append("echo 'Audit job started for ")
        .append(primary.getMetadata().getName())
        .append("' && ");

    command
        .append("echo 'Sending completion message to Kafka...' && ")
        .append("echo '{\"jobName\":\"")
        .append(primary.getMetadata().getName())
        .append("\",\"batchId\":\"")
        .append(primary.getSpec().getBatchId())
        .append("\",\"timestamp\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\",\"status\":\"FINISHED\"}' | ")
        .append("kubectl -n ")
        .append(primary.getMetadata().getNamespace())
        .append(" exec -i $(kubectl -n ")
        .append(primary.getMetadata().getNamespace())
        .append(
            " get pods -l app.kubernetes.io/name=kafka -o jsonpath='{.items[0].metadata.name}') -- ")
        .append("kafka-console-producer.sh ")
        .append("--bootstrap-server localhost:9092 ")
        .append("--topic ingest-to-es && ");

    command
        .append("echo 'Polling Elasticsearch for results...' && ")
        .append("for i in $(seq 1 10); do ")
        .append("  echo \"Polling attempt $i\" && ")
        .append("  kubectl -n ")
        .append(primary.getMetadata().getNamespace())
        .append(" exec $(kubectl -n ")
        .append(primary.getMetadata().getNamespace())
        .append(
            " get pods -l app.kubernetes.io/name=elasticsearch -l app.kubernetes.io/component=master ")
        .append("-o jsonpath='{.items[0].metadata.name}') -- ")
        .append("curl -s -X GET 'http://localhost:9200/")
        .append(primary.getSpec().getExtraEnvs().getDatasetIndex())
        .append("/_search?q=batchId:")
        .append(primary.getSpec().getBatchId())
        .append("' && ")
        .append("  sleep 30; ")
        .append("done && ")
        .append("echo 'Audit completed successfully'");

    return command.toString();
  }
}
