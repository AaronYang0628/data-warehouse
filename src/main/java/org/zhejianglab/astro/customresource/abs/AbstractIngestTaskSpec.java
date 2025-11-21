package org.zhejianglab.astro.customresource.abs;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.fabric8.generator.annotation.Required;
import java.util.List;
import java.util.Map;
import lombok.*;

@Data
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class AbstractIngestTaskSpec {

  @Required private List<String> paths;

  @Required private String platform;

  private Integer timeout;
  private List<String> tags;
  private Map<String, String> userProperties;
}
