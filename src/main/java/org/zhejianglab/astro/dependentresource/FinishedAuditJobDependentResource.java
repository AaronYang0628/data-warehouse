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
      "m.lab.zverse.space/docker.io/bitnami/kubectl:1.28-debian-11";

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
        .append("echo 'Sending completion messages to Kafka...' && ")
        .append("KAFKA_POD=$(kubectl -n ")
        .append(primary.getMetadata().getNamespace())
        .append(
            " get pods -l app.kubernetes.io/name=kafka -o jsonpath='{.items[0].metadata.name}') && ")
        .append("echo \"Querying partition number from Kafka topic...\" && ")
        .append("partition_num=$(kubectl -n ")
        .append(primary.getMetadata().getNamespace())
        .append(" exec $KAFKA_POD -- ")
        .append(
            "kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic ingest-to-es | ")
        .append("grep -oP 'PartitionCount: \\K[0-9]+') && ")
        .append("echo \"Found partition_num=$partition_num\" && ")
        .append("TIMESTAMP_MS=$(date +%s%3N) && ")
        .append("for i in $(seq 0 $((partition_num - 1))); do ")
        .append("  echo \"Sending message for partition $i to Kafka partition $i\" && ")
        .append("  echo '{\"type\":3,\"jobid\":\"")
        .append(primary.getSpec().getBatchId())
        .append(
            "\",\"partitionId\":\"'\"$i\"'\",\"partitionNum\":'\"$partition_num\"',\"finishedTime\":'\"$TIMESTAMP_MS\"'}' | ")
        .append("  kubectl -n ")
        .append(primary.getMetadata().getNamespace())
        .append(" exec -i $KAFKA_POD -- ")
        .append("  kafka-console-producer.sh ")
        .append("  --bootstrap-server localhost:9092 ")
        .append("  --topic ingest-to-es ")
        .append("  --property \"parse.key=true\" ")
        .append("  --property \"key.separator=:\" ")
        .append("  --property \"key=$i\"; ")
        .append("done && ")
        .append("echo \"Sent $partition_num messages to Kafka\" && ");

    command
        .append("echo 'Polling Elasticsearch for results...' && ")
        .append("COUNTER=0 && ")
        .append("while true; do ")
        .append("  COUNTER=$((COUNTER+1)) && ")
        .append("  echo \"Polling attempt $COUNTER at $(date)\" && ")
        .append("  ES_POD=$(kubectl -n ")
        .append(primary.getMetadata().getNamespace())
        .append(
            " get pods -l app.kubernetes.io/name=elasticsearch -l app.kubernetes.io/component=master ")
        .append("-o jsonpath='{.items[0].metadata.name}') && ")
        .append("  echo \"Using ES pod: $ES_POD\" && ")
        .append("  result=$(kubectl -n ")
        .append(primary.getMetadata().getNamespace())
        .append(" exec $ES_POD -- ")
        .append("curl -s -X GET 'http://localhost:9200")
        .append("/ingestjob_info/_search' -H 'Content-Type: application/json' ")
        .append("-d '{\"query\":{\"term\":{\"jobId\":\"")
        .append(primary.getSpec().getBatchId())
        .append("\"}}}') && ")
        .append("  echo \"ES Response: $result\" && ")
        .append(
            "  hits_count=$(echo \"$result\" | sed -n 's/.*\"total\":{\"value\":\\([0-9]*\\).*/\\1/p') && ")
        .append(
            "  partition_num=$(echo \"$result\" | sed -n 's/.*\"partitionNum\":\"\\([0-9]*\\)\".*/\\1/p' | head -1) && ")
        .append("  echo \"hits_count=$hits_count\" && ")
        .append("  echo \"partition_num=$partition_num\" && ")
        .append(
            "  if [ -n \"$hits_count\" ] && [ -n \"$partition_num\" ] && [ \"$hits_count\" -eq \"$partition_num\" ] && [ \"$hits_count\" -ne 0 ]; then ")
        .append("    echo 'All partitions completed successfully!' && ")
        .append("    break; ")
        .append("  fi && ")
        .append("  echo 'Waiting 10 seconds before next poll...' && ")
        .append("  sleep 10; ")
        .append("done && ")
        .append("echo 'Audit completed successfully'");

    return command.toString();
  }
}
