package org.zhejianglab.astro.customresource.flink;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.*;
import lombok.extern.jackson.Jacksonized;
import org.zhejianglab.astro.customresource.abs.AbstractIngestTaskSpec;

@Data
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class FlinkIngestTaskSpec extends AbstractIngestTaskSpec {

  private String extraSecret;
  private Map<String, String> pathPatterns;
  private List<String> allowedSuffixes;

  /** job parallelism which will be used to override the flinkJobConfig.JobSpc.parallelism */
  @Nullable private Integer jobParallelism;

  /**
   * task slots which will be used to override the
   * flinkJobConfig.FlinkConfiguration.taskmanager.numberofTaskSlots
   */
  @Nullable private Integer taskSlots;

  @Nullable private FlinkJobConfig flinkJobConfig;

  @Builder
  @Jacksonized
  public FlinkIngestTaskSpec(
      String path,
      String platform,
      Integer timeout,
      String extraSecret,
      List<String> tags,
      Map<String, String> userProperties,
      Map<String, String> pathPatterns,
      List<String> allowedSuffixes,
      Integer jobParallelism,
      Integer taskSlots,
      FlinkJobConfig flinkJobConfig) {
    this.setPath(path);
    this.setPlatform(platform);
    this.setTimeout(timeout);
    this.setTags(tags);
    this.setUserProperties(userProperties);
    this.extraSecret = extraSecret;
    this.pathPatterns = pathPatterns;
    this.allowedSuffixes = allowedSuffixes;
    if (null == flinkJobConfig) {
      this.flinkJobConfig = FlinkJobConfig.getDefaultConfig();
    } else {
      this.flinkJobConfig = flinkJobConfig;
    }
    this.jobParallelism = jobParallelism;
    if (null != this.jobParallelism) {
      this.flinkJobConfig.updateJobParallelism(this.jobParallelism);
    }
    this.taskSlots = taskSlots;
    if (null != this.taskSlots) {
      this.flinkJobConfig.updateTaskSlots(this.taskSlots);
    }
  }
}
