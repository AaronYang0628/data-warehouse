package org.zhejianglab.astro.customresource.abs;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class AbstractIngestTaskStatus {

  private String exception;
  private String status;

  private Map<String, Object> conditions = new ConcurrentHashMap<>();

  public void updateConditions(String key, Object value) {
    this.conditions.put(key, value);
  }
}
