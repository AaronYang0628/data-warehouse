package org.zhejianglab.astro.customresource.flink;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.jackson.Jacksonized;

@Data
@NoArgsConstructor
public class ExtraEnvs {

  private String esHost;
  private Integer esPort;
  private String datasetIndex;
  private Map<String, String> others;

  @Builder
  @Jacksonized
  public ExtraEnvs(String esHost, Integer esPort, String datasetIndex, Map<String, String> others) {
    this.setDatasetIndex(null != datasetIndex ? datasetIndex : "");
    this.setEsHost(null != esHost ? esHost : "");
    this.setEsPort(null != esPort ? esPort : 5044);
    this.setOthers(null != others ? others : new ConcurrentHashMap<>());
  }
}
