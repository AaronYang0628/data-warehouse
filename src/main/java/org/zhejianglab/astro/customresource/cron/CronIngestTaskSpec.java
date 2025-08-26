package org.zhejianglab.astro.customresource.cron;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.zhejianglab.astro.customresource.abs.AbstractIngestTaskSpec;
import org.zhejianglab.astro.customresource.flink.ExtraSecret;
import org.zhejianglab.astro.customresource.flink.FlinkJobConfig;

@Data
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class CronIngestTaskSpec extends AbstractIngestTaskSpec {

  private ExtraSecret extraSecret;

  private Map<String, String> pathPatterns;

  private List<String> allowedSuffixes;

  private String cron;

  @Nullable private Integer jobParallelism;

  @Nullable private FlinkJobConfig flinkJobConfig;

  @Nullable private String s3TableName;

  @Nullable private String imageMirror;

  public CronIngestTaskSpec(
      String path,
      String cron,
      String platform,
      ExtraSecret extraSecret,
      List<String> tags,
      Map<String, String> userProperties,
      Map<String, String> pathPatterns,
      List<String> allowedSuffixes,
      @Nullable String batchId,
      @Nullable String imageMirror,
      @Nullable Integer timeout,
      @Nullable Integer jobParallelism,
      @Nullable FlinkJobConfig flinkJobConfig) {

    this.setCron(cron);
    this.setPath(path);
    this.setPlatform(platform);
    this.setTimeout(null != timeout ? timeout : 20);
    this.setTags(tags);
    this.setUserProperties(userProperties);
    this.setExtraSecret(extraSecret);
    this.setPathPatterns(pathPatterns);
    this.setAllowedSuffixes(allowedSuffixes);

    this.jobParallelism = null != jobParallelism ? jobParallelism : 1;

    this.s3TableName = null != s3TableName ? s3TableName : StringUtils.EMPTY;

    this.imageMirror = null != imageMirror ? imageMirror : StringUtils.EMPTY;

    if (null == flinkJobConfig) {
      this.flinkJobConfig =
          FlinkJobConfig.builder()
              .build()
              .initSessionJobDefaultConfig(
                  this.getJobParallelism(),
                  StringUtils.EMPTY,
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
