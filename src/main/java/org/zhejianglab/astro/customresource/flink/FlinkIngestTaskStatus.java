package org.zhejianglab.astro.customresource.flink;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.*;
import lombok.extern.jackson.Jacksonized;
import org.apache.flink.api.common.JobStatus;
import org.zhejianglab.astro.customresource.abs.AbstractIngestTaskStatus;

@Data
@AllArgsConstructor
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class FlinkIngestTaskStatus extends AbstractIngestTaskStatus {
  private String batchId;

  @Builder
  @Jacksonized
  public FlinkIngestTaskStatus(String batchId, Exception exception, JobStatus status) {
    this.setBatchId(batchId);
    if (null != exception) {
      this.setException(exception.getLocalizedMessage());
    } else {
      this.setException("");
    }
    this.setStatus(status.name());
  }
}
