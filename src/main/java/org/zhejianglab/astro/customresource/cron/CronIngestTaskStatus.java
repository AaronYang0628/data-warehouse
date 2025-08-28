package org.zhejianglab.astro.customresource.cron;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.fabric8.crd.generator.annotation.PrinterColumn;
import lombok.*;
import lombok.extern.jackson.Jacksonized;
import org.apache.flink.kubernetes.operator.api.lifecycle.ResourceLifecycleState;
import org.zhejianglab.astro.customresource.abs.AbstractIngestTaskStatus;

@Data
@AllArgsConstructor
@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
public class CronIngestTaskStatus extends AbstractIngestTaskStatus {

  @PrinterColumn(name = "SCHEDULE", priority = 5)
  private String schedule;

  @Builder
  @Jacksonized
  public CronIngestTaskStatus(String schedule, ResourceLifecycleState jobStatus) {
    this.setSchedule(schedule);
    if (null == jobStatus) {
      this.setJobStatus(ResourceLifecycleState.FAILED.name());
    } else {
      this.setJobStatus(jobStatus.name());
    }
  }
}
