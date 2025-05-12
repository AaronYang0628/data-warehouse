package org.zhejianglab.astro.utils;

import java.util.Objects;

public final class EnvUtil {

  private EnvUtil() {
    throw new UnsupportedOperationException("cannot init");
  }

  public static String getEnv(String key, String defaultValue) {
    Objects.requireNonNull(key, "cannot null");
    String value = System.getenv(key);
    return value != null ? value : defaultValue;
  }

  public static String getEnv(String key) {
    return getEnv(key, "");
  }
}
