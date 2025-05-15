package org.zhejianglab.astro.customresource.abs;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class AbstractIngestTaskStatus {

  private Map<String, Object> conditions = new ConcurrentHashMap<>();

  public void updateConditions(String key, Object value) {
    this.conditions.put(key, value);
  }
}
