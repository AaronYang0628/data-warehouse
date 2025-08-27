package org.zhejianglab.astro.customresource.flink;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.fabric8.crd.generator.annotation.PrinterColumn;
import lombok.*;
import lombok.extern.jackson.Jacksonized;
import org.apache.flink.api.common.JobStatus;
import org.zhejianglab.astro.customresource.abs.AbstractIngestTaskStatus;

@Data
@AllArgsConstructor
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class FlinkIngestTaskStatus extends AbstractIngestTaskStatus {

  @PrinterColumn(name = "BATCHID", priority = 5)
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
