package org.zhejianglab.astro.customresource.flink;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.*;
import lombok.extern.jackson.Jacksonized;
import org.apache.commons.lang3.StringUtils;
import org.zhejianglab.astro.customresource.abs.AbstractIngestTaskSpec;

@Data
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class FlinkIngestTaskSpec extends AbstractIngestTaskSpec {

  private ExtraSecret extraSecret;
  private Map<String, String> pathPatterns;
  private List<String> allowedSuffixes;

  /** job parallelism which will be used to override the flinkJobConfig.JobSpc.parallelism */
  @Nullable private Integer jobParallelism;

  @Nullable private FlinkJobConfig flinkJobConfig;

  @Nullable private String s3TableName;

  @Builder
  @Jacksonized
  public FlinkIngestTaskSpec(
      String path,
      String platform,
      ExtraSecret extraSecret,
      List<String> tags,
      Map<String, String> userProperties,
      Map<String, String> pathPatterns,
      List<String> allowedSuffixes,
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

    this.jobParallelism = null != jobParallelism ? jobParallelism : 5;

    this.s3TableName = null != s3TableName ? s3TableName : StringUtils.EMPTY;
    if (null == flinkJobConfig) {
      this.flinkJobConfig = FlinkJobConfig.builder().build().getSessionJobDefaultConfig(this);
      this.flinkJobConfig.updateJobParallelism(this.jobParallelism);
    } else {
      this.flinkJobConfig = flinkJobConfig;
    }
  }
}
