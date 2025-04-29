package org.zhejianglab.astro.customresource;

import java.util.List;
import java.util.Map;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.jackson.Jacksonized;

@Getter
@Setter
@EqualsAndHashCode
@Builder
@ToString
@Jacksonized
public class ScanConfig {

  private List<String> tags;
  private Map<String, String> userProperties;
  private Map<String, String> pathPatterns;
  private List<String> allowedSuffixes;
}
