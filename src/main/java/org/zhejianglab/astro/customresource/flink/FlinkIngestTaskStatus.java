package org.zhejianglab.astro.customresource.flink;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.fabric8.crd.generator.annotation.PrinterColumn;
import lombok.*;
import lombok.extern.jackson.Jacksonized;
import org.apache.flink.api.common.JobStatus;
import org.zhejianglab.astro.customresource.abs.AbstractIngestTaskStatus;
import org.zhejianglab.astro.customresource.enums.IngestStatus;

@Data
@AllArgsConstructor
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class FlinkIngestTaskStatus extends AbstractIngestTaskStatus {

  @PrinterColumn(name = "BATCHID", priority = 5)
  private String batchId;

  @PrinterColumn(name = "INGEST STATUS", priority = 6)
  private IngestStatus ingestStatus;

  @Builder
  @Jacksonized
  public FlinkIngestTaskStatus(
      String batchId, IngestStatus ingestStatus, Exception exception, JobStatus jobStatus) {
    this.setBatchId(batchId);
    if (null != exception) {
      this.setException(exception.getLocalizedMessage());
    } else {
      this.setException("NONE");
    }
    if (null == jobStatus) {
      this.setJobStatus(JobStatus.INITIALIZING.name());
    } else {
      this.setJobStatus(jobStatus.name());
    }
    if (null == ingestStatus) {
      this.setIngestStatus(IngestStatus.INGESTING);
    }
  }
}
