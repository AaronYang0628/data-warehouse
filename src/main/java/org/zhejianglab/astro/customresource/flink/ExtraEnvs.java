package org.zhejianglab.astro.customresource.flink;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;
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
  public ExtraEnvs(
      @Nullable String esHost,
      @Nullable Integer esPort,
      @Nullable String datasetIndex,
      @Nullable Map<String, String> others) {
    this.setDatasetIndex(null != datasetIndex ? datasetIndex : "datasetIndex");
    this.setEsHost(null != esHost ? esHost : "elasticsearch");
    this.setEsPort(null != esPort ? esPort : 9200);
    this.setOthers(null != others ? others : new ConcurrentHashMap<>());
  }
}
