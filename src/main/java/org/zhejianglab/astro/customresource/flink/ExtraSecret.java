package org.zhejianglab.astro.customresource.flink;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.jackson.Jacksonized;
import org.zhejianglab.astro.utils.SecretConstant;

@Data
@NoArgsConstructor
public class ExtraSecret {

  private String name;
  private String namespace;
  private String accessKeyName;
  private String secretKeyName;
  private String endpointKeyName;

  @JsonIgnore private Map<String, String> secretData;

  @Builder
  @Jacksonized
  public ExtraSecret(
      @Nullable String namespace,
      @Nullable String name,
      @Nullable String accessKeyName,
      @Nullable String secretKeyName,
      @Nullable String endpointKeyName,
      @Nullable Map<String, String> secretData) {

    this.setNamespace(null != namespace ? namespace : "");
    this.setName(null != name ? name : "metadata-minio-secret");
    this.setAccessKeyName(
        null != accessKeyName ? accessKeyName : SecretConstant.ACCESS_KEY_LOW_NAME);
    this.setSecretKeyName(
        null != secretKeyName ? secretKeyName : SecretConstant.SECRET_KEY_LOW_NAME);
    this.setEndpointKeyName(
        null != endpointKeyName ? endpointKeyName : SecretConstant.ENDPOINT_KEY_LOW_NAME);
    this.setSecretData(new ConcurrentHashMap<>());
  }

  public ExtraSecret patchInfo(String namespace) {
    if (this.getNamespace().isBlank()) {
      this.setNamespace(namespace);
    }
    return this;
  }
}
