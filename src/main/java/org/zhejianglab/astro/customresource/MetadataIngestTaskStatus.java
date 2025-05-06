package org.zhejianglab.astro.customresource;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
public class MetadataIngestTaskStatus {

  private Map<String, Object> conditions = new ConcurrentHashMap<>();

  public void updateConditions(String key, Object value) {
    this.conditions.put(key, value);
  }
}
