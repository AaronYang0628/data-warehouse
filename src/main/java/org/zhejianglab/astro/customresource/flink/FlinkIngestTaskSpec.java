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

  private ExtraSecret extraSecret;
  private Map<String, String> pathPatterns;
  private List<String> allowedSuffixes;
  @Nullable private String batchId;

  /** job parallelism which will be used to override the flinkJobConfig.JobSpc.parallelism */
  @Nullable private Integer jobParallelism;

  @Nullable private FlinkJobConfig flinkJobConfig;

  @Nullable private String s3TableName;

  @Builder
  @Jacksonized
  public FlinkIngestTaskSpec(
      @NonNull String path,
      @NonNull String platform,
      ExtraSecret extraSecret,
      List<String> tags,
      Map<String, String> userProperties,
      Map<String, String> pathPatterns,
      List<String> allowedSuffixes,
      @Nullable String batchId,
      @Nullable Integer timeout,
      @Nullable Integer jobParallelism,
      @Nullable String s3TableName,
      @Nullable FlinkJobConfig flinkJobConfig) {
    this.setPath(path);
    this.setPlatform(platform);
    this.setTimeout(null != timeout ? timeout : 20);
    this.setTags(tags);
    this.setUserProperties(userProperties);
    this.extraSecret = extraSecret;
    this.pathPatterns = pathPatterns;
    this.allowedSuffixes = allowedSuffixes;

    this.jobParallelism = null != jobParallelism ? jobParallelism : 1;

    this.s3TableName = null != s3TableName ? s3TableName : StringUtils.EMPTY;

    try {
      this.setBatchId(
          null != batchId
              ? batchId
              : StringUtils.generateMD5(objectMapper.writeValueAsString(this)));
    } catch (JsonProcessingException e) {
      this.setBatchId("");
    }

    if (null == flinkJobConfig) {
      this.flinkJobConfig =
          FlinkJobConfig.builder()
              .build()
              .initSessionJobDefaultConfig(
                  this.getJobParallelism(),
                  this.getBatchId(),
                  this.getPlatform(),
                  this.getPath(),
                  this.getS3TableName(),
                  this.getUserProperties(),
                  this.getTags(),
                  this.getPathPatterns(),
                  this.getAllowedSuffixes(),
                  this.getExtraSecret());
      this.flinkJobConfig.getJob().setParallelism(this.jobParallelism);
      this.flinkJobConfig.getJobArgsMap().put("S3_TABLE_NAME", this.s3TableName);
    } else {
      this.flinkJobConfig = flinkJobConfig;
    }
  }
}
