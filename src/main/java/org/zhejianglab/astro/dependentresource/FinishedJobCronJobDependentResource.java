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
public class FinishedJobCronJobDependentResource
    extends CRUDKubernetesDependentResource<Job, FlinkIngestTask> {

  private static final Logger log =
      LoggerFactory.getLogger(FinishedJobCronJobDependentResource.class);

  private static final String CRON_JOB_SA_NAME = "metadata-ingest-operator-sa";
  private static final String KAFKA_ES_IMAGE = "docker.io/bitnami/kubectl:1.28-debian-11";

  public FinishedJobCronJobDependentResource() {
    super(Job.class);
  }

  @Override
  protected Job desired(FlinkIngestTask primary, Context<FlinkIngestTask> context) {
    String jobName = primary.getMetadata().getName() + "-finished-cronjob";

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
    // 构建向Kafka发送消息并轮询ES的命令
    StringBuilder command = new StringBuilder();

    // 1. 向Kafka发送消息
    command
        .append("echo 'Sending message to Kafka for job ")
        .append(primary.getMetadata().getName())
        .append("' && ");

    command
        .append("echo '{\"jobName\":\"")
        .append(primary.getMetadata().getName())
        .append("\",\"timestamp\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\",\"status\":\"FINISHED\"}' | ")
        .append(
            "kubectl exec -it $(kubectl get pods -l app=kafka -o jsonpath='{.items[0].metadata.name}') -- kafka-console-producer.sh ")
        .append("--bootstrap-server localhost:9092 ")
        .append("--topic ingest-to-es && ");

    // 2. 轮询ES
    command.append("echo 'Polling Elasticsearch...' && ");
    command
        .append(
            "kubectl exec -it $(kubectl get pods -l app=elasticsearch -o jsonpath='{.items[0].metadata.name}') -- curl -X GET ")
        .append("\"http://localhost:9200/")
        .append(primary.getMetadata().getName())
        .append("/_search?q=jobName:")
        .append(primary.getMetadata().getName())
        .append("\" && ");

    // 3. 添加循环轮询逻辑
    command
        .append("for i in $(seq 1 10); do ")
        .append("echo \"Polling attempt $i\" && ")
        .append("sleep 30 && ") // 每30秒轮询一次
        .append(
            "kubectl exec -it $(kubectl get pods -l app=elasticsearch -o jsonpath='{.items[0].metadata.name}') -- curl -X GET ")
        .append("\"http://localhost:9200/")
        .append(primary.getMetadata().getName())
        .append("/_search?q=jobName:")
        .append(primary.getMetadata().getName())
        .append("\" || true; ")
        .append("done");

    return command.toString();
  }
}
