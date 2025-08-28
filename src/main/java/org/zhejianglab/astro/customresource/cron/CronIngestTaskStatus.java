package org.zhejianglab.astro.customresource.cron;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.*;
import lombok.extern.jackson.Jacksonized;
import org.apache.flink.kubernetes.operator.api.lifecycle.ResourceLifecycleState;
import org.zhejianglab.astro.customresource.abs.AbstractIngestTaskStatus;

@Data
@AllArgsConstructor
@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
public class CronIngestTaskStatus extends AbstractIngestTaskStatus {

  @Builder
  @Jacksonized
  public CronIngestTaskStatus(ResourceLifecycleState jobStatus) {
    if (null == jobStatus) {
      this.setJobStatus(ResourceLifecycleState.FAILED.name());
    } else {
      this.setJobStatus(jobStatus.name());
    }
  }
}
