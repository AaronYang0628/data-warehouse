package org.zhejianglab.astro.customresource.flink;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.fabric8.kubernetes.api.model.PodTemplateSpec;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import lombok.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.kubernetes.operator.api.spec.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder(toBuilder = true)
@JsonIgnoreProperties(ignoreUnknown = true)
public class FlinkJobConfig {

  private static final Logger log = LoggerFactory.getLogger(FlinkJobConfig.class);

  private static final ObjectMapper objectMapper = new ObjectMapper();

  public static final String FLINK_SERVICE_ACCOUNT = "metadata-ingest-flink-sa";

  private static final String DEFAULT_KAFKA_BOOTSTRAP_SERVER =
      "warehouse-kafka.warehouse.svc.cluster.local:9092";

  private String image;

  @Builder.Default private String serviceAccount = FLINK_SERVICE_ACCOUNT;

  @Builder.Default private FlinkVersion flinkVersion = FlinkVersion.v1_20;

  private IngressSpec ingress;

  private PodTemplateSpec podTemplate;

  private JobManagerSpec jobManager;

  private FlinkIngestTaskManagerSpec taskManager;

  private JobSpec job;

  @Builder.Default @JsonIgnore private Map<String, Object> jobArgsMap = new ConcurrentHashMap<>();

  @Builder.Default private Map<String, String> flinkConfiguration = new ConcurrentHashMap<>();

  @Builder.Default private KubernetesDeploymentMode mode = KubernetesDeploymentMode.NATIVE;

  public FlinkJobConfig initSessionJobDefaultConfig(
      Integer parallelism,
      String bacthId,
      String platform,
      List<String> scanPaths,
      Map<String, String> userProperties,
      List<String> activatedHandlers,
      List<String> tags,
      Map<String, String> pathPatterns,
      List<String> allowedSuffixes,
      ExtraEnvs extraEnvs,
      ExtraSecret extraSecret) {
    if (null != bacthId && !bacthId.isEmpty()) {
      this.jobArgsMap.put("BATCH_ID", bacthId);
    }

    this.jobArgsMap.put(
        "SCAN_CONFIG",
        generateScanConfig(userProperties, pathPatterns, tags, allowedSuffixes, activatedHandlers));

    this.jobArgsMap.put("PLATFORM", platform);
    this.jobArgsMap.put("SCAN_PATHS", scanPaths.stream().collect(Collectors.joining(",")));
    if (null != extraSecret) {
      this.jobArgsMap.putAll(extraSecret.getSecretData());
    }

    this.jobArgsMap.put(
        "KAFKA_BOOTSTRAP_SERVER",
        System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVER", DEFAULT_KAFKA_BOOTSTRAP_SERVER));

    this.jobArgsMap.put("ES_HOST", extraEnvs.getEsHost());
    this.jobArgsMap.put("ES_PORT", extraEnvs.getEsPort());
    this.jobArgsMap.put("ES_DATASET_INDEX", extraEnvs.getDatasetIndex());
    this.jobArgsMap.putAll(extraEnvs.getOthers());

    return FlinkJobConfig.builder()
        .job(
            JobSpec.builder()
                .jarURI(
                    "http://data-and-computing.oss-cn-hangzhou-zjy-d01-a.res.cloud.zhejianglab.com/projects/slurm-on-k8s/intel-mpi-libs/flink-es-ingest-job-1.0.0-all.jar")
                .parallelism(parallelism)
                .upgradeMode(UpgradeMode.STATELESS)
                .entryClass("com.zhejianglab.astronomy.metadata.file.MetadataExtractorJob")
                .args(mapToStringArray(this.getJobArgsMap()))
                .build())
        .build();
  }

  public FlinkJobConfig updateJobArgsMap(
      Integer parallelism,
      String bacthId,
      String platform,
      List<String> scanPaths,
      Map<String, String> userProperties,
      List<String> activatedHandlers,
      List<String> tags,
      Map<String, String> pathPatterns,
      List<String> allowedSuffixes,
      ExtraEnvs extraEnvs,
      ExtraSecret extraSecret) {
    if (null != bacthId && !bacthId.isEmpty()) {
      this.jobArgsMap.put("BATCH_ID", bacthId);
    }
    this.jobArgsMap.put(
        "SCAN_CONFIG",
        generateScanConfig(userProperties, pathPatterns, tags, allowedSuffixes, activatedHandlers));

    this.jobArgsMap.put("PLATFORM", platform);
    this.jobArgsMap.put("SCAN_PATH", scanPaths.stream().collect(Collectors.joining(",")));
    if (null != extraSecret) {
      this.jobArgsMap.putAll(extraSecret.getSecretData());
    }

    this.jobArgsMap.put("ES_HOST", extraEnvs.getEsHost());
    this.jobArgsMap.put("ES_PORT", extraEnvs.getEsPort());
    this.jobArgsMap.put("ES_DATASET_INDEX", extraEnvs.getDatasetIndex());

    this.jobArgsMap.put(
        "KAFKA_BOOTSTRAP_SERVER",
        System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVER", DEFAULT_KAFKA_BOOTSTRAP_SERVER));

    return this.toBuilder()
        .job(
            JobSpec.builder()
                .jarURI(this.getJob().getJarURI())
                .parallelism(this.getJob().getParallelism())
                .upgradeMode(this.getJob().getUpgradeMode())
                .entryClass(this.getJob().getEntryClass())
                .args(mapToStringArray(this.getJobArgsMap()))
                .build())
        .build();
  }

  private String generateScanConfig(
      Map<String, String> userProperties,
      Map<String, String> pathPatterns,
      List<String> tags,
      List<String> allowedSuffixes,
      List<String> activatedHandlers) {
    ObjectNode scanConfig = objectMapper.createObjectNode();

    scanConfig.set(
        "userProperties",
        objectMapper.valueToTree(userProperties != null ? userProperties : new HashMap<>()));

    scanConfig.set(
        "pathPatterns",
        objectMapper.valueToTree(pathPatterns != null ? pathPatterns : new HashMap<>()));

    ArrayNode tagsNode = scanConfig.putArray("tags");
    if (tags != null) {
      tags.forEach(tagsNode::add);
    }

    ArrayNode suffixesNode = scanConfig.putArray("allowedSuffixes");
    if (allowedSuffixes != null) {
      allowedSuffixes.forEach(suffixesNode::add);
    }

    ArrayNode handlersNode = scanConfig.putArray("activatedHandlers");
    if (activatedHandlers != null && !activatedHandlers.isEmpty()) {
      activatedHandlers.forEach(handlersNode::add);
    }
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
