package org.zhejianglab.astro.customresource.flink;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.fabric8.kubernetes.api.model.PodTemplateSpec;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
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

  private String image;

  @Builder.Default private String serviceAccount = FLINK_SERVICE_ACCOUNT;

  @Builder.Default private FlinkVersion flinkVersion = FlinkVersion.v1_20;

  private IngressSpec ingress;

  private PodTemplateSpec podTemplate;

  private JobManagerSpec jobManager;

  private FlinkIngestTaskManagerSpec taskManager;

  private JobSpec job;

  @Builder.Default @JsonIgnore private Map<String, Object> jobArgsMap = new ConcurrentHashMap<>();

  private Map<String, String> flinkConfiguration;

  @Builder.Default private KubernetesDeploymentMode mode = KubernetesDeploymentMode.NATIVE;

  public FlinkJobConfig initSessionJobDefaultConfig(FlinkIngestTaskSpec primarSpec) {
    log.info("Generating default Flink job config for session job with spec: {}", primarSpec);
    this.jobArgsMap.put("BATCH_ID", UuidUtil.getTimeBasedUuid().toString());
    this.jobArgsMap.put(
        "SCAN_CONFIG",
        generateScanConfig(
            primarSpec.getUserProperties(),
            primarSpec.getPathPatterns(),
            primarSpec.getTags(),
            primarSpec.getAllowedSuffixes()));

    this.jobArgsMap.put("PLATFORM", primarSpec.getPlatform());
    this.jobArgsMap.put("SCAN_PATH", primarSpec.getPath());
    this.jobArgsMap.put("S3_TABLE_NAME", primarSpec.getS3TableName());
    this.jobArgsMap.putAll(primarSpec.getExtraSecret().getSecretData());
    return FlinkJobConfig.builder()
        .job(
            JobSpec.builder()
                .jarURI(
                    "http://data-and-computing.oss-cn-hangzhou-zjy-d01-a.res.cloud.zhejianglab.com/projects%2Fslurm-on-k8s%2Fintel-mpi-libs%2Fflink-es-ingest-job-1.0.0-all.jar")
                .parallelism(primarSpec.getJobParallelism())
                .upgradeMode(UpgradeMode.STATELESS)
                .entryClass("com.zhejianglab.astronomy.metadata.Main")
                .args(mapToStringArray(this.getJobArgsMap()))
                .build())
        .build();
  }

  public void updateJobArgsMap(Map<String, ?> updatedMap) {
    this.jobArgsMap.putAll(updatedMap);
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

  private static String[] mapToStringArray(Map<String, Object> map) {
    if (map == null || map.isEmpty()) {
      return new String[0];
    }
    return map.entrySet().stream()
        .map(entry -> entry.getKey() + "=" + entry.getValue().toString())
        .toArray(String[]::new);
  }
}
