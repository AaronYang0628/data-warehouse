package org.zhejianglab.astro.customresource;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.fabric8.kubernetes.api.model.PodTemplateSpec;
import java.util.Map;
import lombok.*;
import org.apache.flink.kubernetes.operator.api.spec.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
public class FlinkJobConfig {

  private String image;

  private String imagePullPolicy;

  private String serviceAccount;

  private String flinkVersion;

  private IngressSpec ingress;

  private PodTemplateSpec podTemplate;

  private JobManagerSpec jobManager;

  private MetadataIngestTaskManagerSpec taskManager;

  private JobSpec job;

  private Map<String, String> flinkConfiguration;

  public FlinkVersion getFlinkVersion() {
    return FlinkVersion.valueOf(flinkVersion);
  }
}
