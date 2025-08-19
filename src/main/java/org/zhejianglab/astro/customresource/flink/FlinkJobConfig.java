package org.zhejianglab.astro.customresource.flink;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.fabric8.kubernetes.api.model.PodTemplateSpec;
import java.util.List;
import java.util.Map;
import lombok.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.kubernetes.operator.api.spec.*;
import org.apache.logging.log4j.core.util.UuidUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
public class FlinkJobConfig {

  private static final Logger log = LoggerFactory.getLogger(FlinkJobConfig.class);

  private static final ObjectMapper objectMapper = new ObjectMapper();

  public static final String FLINK_SERVICE_ACCOUNT = "metadata-ingest-flink-sa";

  private static final String FLINK_TASKMANAGER_NUMBER_OF_TASK_SLOTS =
      "taskmanager.numberOfTaskSlots";

  @Builder.Default private String repository = "docker.io/library";

  private String image;

  @Builder.Default private String serviceAccount = FLINK_SERVICE_ACCOUNT;

  @Builder.Default private FlinkVersion flinkVersion = FlinkVersion.v1_20;

  private IngressSpec ingress;

  private PodTemplateSpec podTemplate;

  private JobManagerSpec jobManager;

  private FlinkIngestTaskManagerSpec taskManager;

  private JobSpec job;

  private Map<String, String> flinkConfiguration;

  @Builder.Default private KubernetesDeploymentMode mode = KubernetesDeploymentMode.NATIVE;

  public FlinkJobConfig getSessionJobDefaultConfig(FlinkIngestTaskSpec primarSpec) {

    return FlinkJobConfig.builder()
        .job(
            JobSpec.builder()
                .jarURI(
                    "http://data-and-computing.oss-cn-hangzhou-zjy-d01-a.res.cloud.zhejianglab.com/projects%2Fslurm-on-k8s%2Fintel-mpi-libs%2Fflink-es-ingest-job-1.0.0-all.jar")
                .parallelism(primarSpec.getJobParallelism())
                .upgradeMode(UpgradeMode.STATELESS)
                .entryClass("com.zhejianglab.astronomy.metadata.Main")
                .args(
                    new String[] {
                      "BATCH_ID=" + UuidUtil.getTimeBasedUuid().toString(),
                      "SCAN_CONFIG="
                          + generateScanConfig(
                              primarSpec.getUserProperties(),
                              primarSpec.getPathPatterns(),
                              primarSpec.getTags(),
                              primarSpec.getAllowedSuffixes()),
                      "PLATFORM=" + primarSpec.getPlatform(),
                      "SCAN_PATH=" + primarSpec.getPath(),
                      "KAFKA_BOOTSTRAP_SERVER=metadata-kafka.metadata.sve.cluster.local:9092",
                      "S3_ENDPOINT=" + "http://oss-cn-hangzhou-zjy-d01-a.ops.cloud.zhejianglab.com",
                      "S3_ACCESS_KEY=" + "dHhEJoLjXS7BI7tG",
                      "S3_ACCESS_SECRET=" + "OIGQCkaQiLNymxXdhDb1v7kU7O6kfT",
                      "S3_TABLE_NAME=" + primarSpec.getS3TableName()
                    })
                .build())
        .build();
  }

  public void updateJobParallelism(Integer jobParallelism) {
    JobSpec jobSpec = this.getJob();
    if (jobSpec == null) {
      throw new IllegalStateException("JobSpec is null, cannot update parallelism.");
    }
    jobSpec.setParallelism(jobParallelism);
    this.setJob(jobSpec);
  }

  public void updateTaskSlots(Integer taskSlots) {
    this.getFlinkConfiguration().put(FLINK_TASKMANAGER_NUMBER_OF_TASK_SLOTS, taskSlots.toString());
  }

  private String generateScanConfig(
      Map<String, String> userProperties,
      Map<String, String> pathPatterns,
      List<String> tags,
      List<String> allowedSuffixes) {
    ObjectNode scanConfig = objectMapper.createObjectNode();

    scanConfig.set("userProperties", objectMapper.valueToTree(userProperties));

    scanConfig.set("pathPatterns", objectMapper.valueToTree(pathPatterns));

    ArrayNode tagsNode = scanConfig.putArray("tags");
    tags.forEach(tagsNode::add);

    ArrayNode suffixesNode = scanConfig.putArray("allowedSuffixes");
    allowedSuffixes.forEach(suffixesNode::add);
    return scanConfig.toString();
  }
}
