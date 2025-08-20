package org.zhejianglab.astro.customresource.flink;

import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.jackson.Jacksonized;

@Data
@NoArgsConstructor
public class ExtraSecret {

  private String name;
  private String namespace;
  private String accessKeyName;
  private String secretKeyName;
  private String endpointKeyName;

  @Builder
  @Jacksonized
  public ExtraSecret(
      @Nullable String namespace,
      @Nullable String name,
      @Nullable String accessKeyName,
      @Nullable String secretKeyName,
      @Nullable String endpointKeyName) {

    this.setNamespace(null != namespace ? namespace : "");
    this.setName(null != name ? name : "metadata-minio-secret");
    this.setAccessKeyName(null != accessKeyName ? accessKeyName : "s3-access-key");
    this.setSecretKeyName(null != secretKeyName ? secretKeyName : "s3-s3-access-secret");
    this.setEndpointKeyName(null != endpointKeyName ? endpointKeyName : "s3-endpoint");
  }

  public ExtraSecret patchInfo(String namespace) {
    if (this.getNamespace().isBlank()) {
      this.setNamespace(namespace);
    }

    return this;
  }
}
