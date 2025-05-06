package org.zhejianglab.astro.customresource;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.*;
import lombok.extern.jackson.Jacksonized;

@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class MetadataIngestTaskSpec {

  private String path;
  private String platform;
  private Integer timeout;
  private String extraSecret;
  private ScanConfig scanConfig;
  private FlinkJobConfig flinkJobConfig;

  @Builder
  @Jacksonized
  public MetadataIngestTaskSpec(
      String path,
      Integer timeout,
      String platform,
      String extraSecret,
      ScanConfig scanConfig,
      FlinkJobConfig flinkJobConfig) {
    this.timeout = timeout;
    this.path = path;
    this.platform = platform;
    this.extraSecret = extraSecret;
    this.scanConfig = scanConfig;
    this.flinkJobConfig = flinkJobConfig;
  }
}
