package org.zhejianglab.astro.customresource;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;
import java.util.Map;
import lombok.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
public class ScanConfig {

  private List<String> tags;
  private Map<String, String> userProperties;
  private Map<String, String> pathPatterns;
  private List<String> allowedSuffixes;
}
