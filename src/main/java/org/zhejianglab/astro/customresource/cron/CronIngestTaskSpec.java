package org.zhejianglab.astro.customresource.cron;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.fabric8.generator.annotation.Required;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.jackson.Jacksonized;
import org.apache.commons.lang3.StringUtils;
import org.zhejianglab.astro.customresource.abs.AbstractIngestTaskSpec;
import org.zhejianglab.astro.customresource.flink.ExtraSecret;
import org.zhejianglab.astro.customresource.flink.FlinkJobConfig;

@Data
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class CronIngestTaskSpec extends AbstractIngestTaskSpec {
  @Required private String cron;

  @Nullable private Boolean suspend;

  @Nullable private Long delay;

  @Nullable private ExtraSecret extraSecret;

  @Nullable private Map<String, String> pathPatterns;

  @Nullable private List<String> allowedSuffixes;

  @Nullable private List<String> activatedHandlers;

  @Nullable private Integer jobParallelism;

  @Nullable private FlinkJobConfig flinkJobConfig;

  @Nullable private String imageMirror;

  @Builder
  @Jacksonized
  public CronIngestTaskSpec(
      String path,
      String cron,
      String platform,
      @Nullable Boolean suspend,
      @Nullable Long delay,
      @Nullable Integer jobParallelism,
      @Nullable List<String> allowedSuffixes,
      @Nullable Integer timeout,
      @Nullable List<String> tags,
      @Nullable List<String> activatedHandlers,
      @Nullable ExtraSecret extraSecret,
      @Nullable Map<String, String> userProperties,
      @Nullable Map<String, String> pathPatterns,
      @Nullable String imageMirror,
      @Nullable FlinkJobConfig flinkJobConfig) {

    this.setCron(cron);
    this.setPath(path);
    this.setPlatform(platform);

    this.setSuspend(null != suspend ? suspend : false);
    this.setDelay(null != delay ? delay : 600L);
    this.setJobParallelism(null != jobParallelism ? jobParallelism : 1);
    this.setAllowedSuffixes(null != allowedSuffixes ? allowedSuffixes : List.of("*"));

    this.setTimeout(null != timeout ? timeout : 20);
    this.setTags(null != tags ? tags : List.of());
    this.setUserProperties(null != userProperties ? userProperties : Map.of());
    this.setActivatedHandlers(null != activatedHandlers ? activatedHandlers : List.of());

    this.setExtraSecret(null != extraSecret ? extraSecret : ExtraSecret.builder().build());
    this.setPathPatterns(null != pathPatterns ? pathPatterns : Map.of());
    this.setImageMirror(null != imageMirror ? imageMirror : "m.daocloud.io");
    if (null == flinkJobConfig) {
      this.flinkJobConfig =
          FlinkJobConfig.builder()
              .build()
              .initSessionJobDefaultConfig(
                  this.getJobParallelism(),
                  StringUtils.EMPTY,
                  this.getPlatform(),
                  this.getPath(),
                  this.getUserProperties(),
                  this.getActivatedHandlers(),
                  this.getTags(),
                  this.getPathPatterns(),
                  this.getAllowedSuffixes(),
                  this.getExtraSecret());
      this.flinkJobConfig.getJob().setParallelism(this.jobParallelism);
    } else {
      this.setFlinkJobConfig(flinkJobConfig);
    }
  }
}
