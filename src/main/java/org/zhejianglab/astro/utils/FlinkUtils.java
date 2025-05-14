package org.zhejianglab.astro.utils;

import org.apache.flink.kubernetes.operator.api.spec.FlinkVersion;

public class FlinkUtils {

  private static final String FLINK = "flink";

  public static String getImageVersion(FlinkVersion version) {
    return FLINK + ":" + version.name().substring(1).replace('_', '.');
  }
}
