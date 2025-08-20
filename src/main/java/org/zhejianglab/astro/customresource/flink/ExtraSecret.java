package org.zhejianglab.astro.customresource.flink;

import lombok.Builder;
import lombok.extern.jackson.Jacksonized;

@Data
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class ExtraSecret {

  private String namespace;
  @Builder.Default private String name = "metadata-minio-secret";
  @Builder.Default private String accessKeyName = "s3-access-key";
  @Builder.Default private String secretKeyName = "s3-access-secret";
  @Builder.Default private String endpointName = "s3-endpoint";

  @Builder
  @Jacksonized
  public ExtraSecret(
    String name,
    String namespace,
    String accessKeyName,

  ) {
    this.set
  }
}
