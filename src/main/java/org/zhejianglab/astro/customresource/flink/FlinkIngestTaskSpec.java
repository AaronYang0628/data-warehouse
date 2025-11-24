package org.zhejianglab.astro.customresource.flink;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.*;
import lombok.extern.jackson.Jacksonized;
import org.zhejianglab.astro.customresource.abs.AbstractIngestTaskSpec;
import org.zhejianglab.astro.utils.StringUtils;

@Data
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class FlinkIngestTaskSpec extends AbstractIngestTaskSpec {

  private static final ObjectMapper objectMapper = new ObjectMapper();

  @Nullable private ExtraSecret extraSecret;
  @Nullable private ExtraEnvs extraEnvs;
  @Nullable private Map<String, String> pathPatterns;
  @Nullable private List<String> allowedSuffixes;
  @Nullable private List<String> activatedHandlers;

  @Nullable private String batchId;

  /** job parallelism which will be used to override the flinkJobConfig.JobSpc.parallelism */
  @Nullable private Integer jobParallelism;

  @Nullable private FlinkJobConfig flinkJobConfig;

  @Builder
  @Jacksonized
  public FlinkIngestTaskSpec(
      List<String> paths,
      String platform,
      @Nullable String s3TableName,
      @Nullable Integer jobParallelism,
      @Nullable List<String> allowedSuffixes,
      @Nullable Integer timeout,
      @Nullable List<String> tags,
      @Nullable ExtraSecret extraSecret,
      @Nullable ExtraEnvs extraEnvs,
      @Nullable List<String> activatedHandlers,
      @Nullable Map<String, String> userProperties,
      @Nullable Map<String, String> pathPatterns,
      @Nullable String batchId,
      @Nullable FlinkJobConfig flinkJobConfig) {
    this.setPaths(paths);
    this.setPlatform(platform);

    this.setJobParallelism(null != jobParallelism ? jobParallelism : 1);
    this.setAllowedSuffixes(null != allowedSuffixes ? allowedSuffixes : List.of("*"));

    this.setTimeout(null != timeout ? timeout : 20);
    this.setTags(null != tags ? tags : List.of());
    this.setUserProperties(null != userProperties ? userProperties : Map.of());
    this.setActivatedHandlers(null != activatedHandlers ? activatedHandlers : List.of());

    this.setExtraEnvs(extraEnvs);
    this.setExtraSecret(null != extraSecret ? extraSecret : ExtraSecret.builder().build());
    this.setPathPatterns(null != pathPatterns ? pathPatterns : Map.of());

    try {
      this.batchId =
          null != batchId
              ? batchId
              : StringUtils.generateMD5(objectMapper.writeValueAsString(this));
    } catch (JsonProcessingException e) {
      this.setBatchId(StringUtils.EMPTY);
    }

    if (null == flinkJobConfig) {
      this.flinkJobConfig =
          FlinkJobConfig.builder()
              .build()
              .initSessionJobDefaultConfig(
                  this.getJobParallelism(),
                  this.getBatchId(),
                  this.getPlatform(),
                  this.getPaths(),
                  this.getUserProperties(),
                  this.getActivatedHandlers(),
                  this.getTags(),
                  this.getPathPatterns(),
                  this.getAllowedSuffixes(),
                  this.getExtraEnvs(),
                  this.getExtraSecret());
      this.flinkJobConfig.getJob().setParallelism(this.jobParallelism);
    } else {
      this.setFlinkJobConfig(flinkJobConfig);
    }
  }
}
