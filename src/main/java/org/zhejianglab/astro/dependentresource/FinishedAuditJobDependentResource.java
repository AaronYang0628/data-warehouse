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
import org.zhejianglab.astro.customresource.enums.IngestStatus;

@KubernetesDependent
public class FinishedAuditJobDependentResource
    extends CRUDKubernetesDependentResource<Job, FlinkIngestTask> {

  private static final Logger log =
      LoggerFactory.getLogger(FinishedAuditJobDependentResource.class);

  private static final String CRON_JOB_SA_NAME = "metadata-ingest-operator-sa";
  private static final String KAFKA_ES_IMAGE = "docker.io/bitnami/kubectl:1.28-debian-11";

  public FinishedAuditJobDependentResource() {
    super(Job.class);
  }

  @Override
  protected Job desired(FlinkIngestTask primary, Context<FlinkIngestTask> context) {
    // Check if the ingest status is FINISHED before creating the job
    if (primary.getStatus() == null
        || primary.getStatus().getIngestStatus() != IngestStatus.FINISHED) {
      log.info(
          "FlinkIngestTask {} is not in FINISHED state, skipping job creation",
          primary.getMetadata().getName());
      return null;
    }

    String jobName = primary.getMetadata().getName() + "-finished-audit-job";

    return new JobBuilder()
        .withMetadata(
            new ObjectMetaBuilder()
                .withName(jobName)
                .withNamespace(primary.getMetadata().getNamespace())
                .build())
        .withSpec(
            new JobSpecBuilder()
                .withBackoffLimit(1)
                .withSuspend(false)
                .withBackoffLimit(1)
                .withTemplate(
                    new io.fabric8.kubernetes.api.model.PodTemplateSpecBuilder()
                        .withNewMetadata()
                        .withName(jobName + "-template")
                        .endMetadata()
                        .withNewSpec()
                        .withRestartPolicy("OnFailure")
                        .addNewContainer()
                        .withName("kubectl")
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
        .append("echo 'Sending message to Kafka for job ")
        .append(primary.getMetadata().getName())
        .append("' && ");

    command
        .append("echo '{\"jobName\":\"")
        .append(primary.getMetadata().getName())
        .append("\",\"timestamp\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\",\"status\":\"FINISHED\"}' && ")
        .append("echo {\"topic\":" + primary.getSpec().getBatchId() + "\"} | ")
        .append("kubectl -n " + primary.getMetadata().getNamespace())
        .append(" exec -i $(kubectl")
        .append(" -n " + primary.getMetadata().getNamespace())
        .append(
            " get pods -l app.kubernetes.io/name=kafka  -o jsonpath='{.items[0].metadata.name}') -- kafka-console-producer.sh ")
        .append("--bootstrap-server localhost:9092 ")
        .append("--topic ingest-to-es ");

    command.append("echo 'Polling Elasticsearch...' && ");
    command
        .append(
            "kubectl -n "
                + primary.getMetadata().getNamespace()
                + " exec -it $(kubectl "
                + primary.getMetadata().getNamespace()
                + " get pods -l app.kubernetes.io/name=elasticsearch -l app.kubernetes.io/component=master -o jsonpath='{.items[0].metadata.name}') -- curl -X GET ")
        .append("\"http://localhost:9200/")
        .append(primary.getMetadata().getName())
        .append("/_search?q=jobName:")
        .append(primary.getMetadata().getName())
        .append("\" && ");

    command
        .append("for i in $(seq 1 10); do ")
        .append("echo \"Polling attempt $i\" && ")
        .append("sleep 30 && ")
        .append(
            "kubectl "
                + primary.getMetadata().getNamespace()
                + " exec -it $(kubectl "
                + primary.getMetadata().getNamespace()
                + " get pods -l app.kubernetes.io/name=elasticsearch -l app.kubernetes.io/component=master -o jsonpath='{.items[0].metadata.name}') -- curl -X GET ")
        .append("\"http://localhost:9200/")
        .append(primary.getMetadata().getName())
        .append("/_search?q=jobName:")
        .append(primary.getMetadata().getName())
        .append("\" || true; ")
        .append("done");

    return command.toString();
  }
}
