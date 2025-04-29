package org.zhejianglab.astro.customresource;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.jackson.Jacksonized;

@Getter
@Setter
@EqualsAndHashCode
@ToString
public class MetadataIngestTaskSpec {

  private String path;
  private String platform;
  private Integer timeout;
  private String extraSecret;
  private ScanConfig scanConfig;

  @Builder
  @Jacksonized
  public MetadataIngestTaskSpec(
      String path, Integer timeout, String platform, String extraSecret, ScanConfig scanConfig) {
    this.timeout = timeout;
    this.path = path;
    this.platform = platform;
    this.extraSecret = extraSecret;
    this.scanConfig = scanConfig;
  }
}
