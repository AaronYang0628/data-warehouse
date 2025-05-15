package org.zhejianglab.astro.customresource.flink;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.fabric8.kubernetes.api.model.PodTemplateSpec;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.kubernetes.operator.api.spec.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
public class FlinkIngestTaskManagerSpec {
  private Resource resource;

  private Integer replicas;

  private PodTemplateSpec podTemplate;
}
