package org.zhejianglab.astro.customresource.cron;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.fabric8.crd.generator.annotation.PrinterColumn;
import lombok.*;
import lombok.extern.jackson.Jacksonized;
import org.apache.flink.api.common.JobStatus;
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
  public CronIngestTaskStatus(String schedule, JobStatus jobStatus) {
    this.setSchedule(schedule);
    if (null == jobStatus) {
      this.setJobStatus(JobStatus.INITIALIZING.name());
    } else {
      this.setJobStatus(jobStatus.name());
    }
  }
}
