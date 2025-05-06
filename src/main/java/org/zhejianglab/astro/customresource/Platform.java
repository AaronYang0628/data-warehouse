package org.zhejianglab.astro.customresource;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum Platform {
  VIRTUAL("virtual"),
  S3("s3"),
  OSS("oss"),
  JDBC("jdbc"),
  FILE("file");

  final String protocol;
}
