package org.zhejianglab.astro.customresource.java;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;
import java.util.Map;
import lombok.*;
import lombok.extern.jackson.Jacksonized;
import org.zhejianglab.astro.customresource.abs.AbstractIngestTaskSpec;

@Data
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class VirtualIngestTaskSpec extends AbstractIngestTaskSpec {

  private String extraSecret;
  private Map<String, String> pathPatterns;
  private List<String> allowedSuffixes;

  @Builder
  @Jacksonized
  public VirtualIngestTaskSpec(
      String path,
      String platform,
      Integer timeout,
      String extraSecret,
      List<String> tags,
      Map<String, String> userProperties,
      Map<String, String> pathPatterns,
      List<String> allowedSuffixes) {
    this.setPath(path);
    this.setPlatform(platform);
    this.setTimeout(timeout);
    this.setTags(tags);
    this.setUserProperties(userProperties);
    this.extraSecret = extraSecret;
    this.pathPatterns = pathPatterns;
    this.allowedSuffixes = allowedSuffixes;
  }
}
