package org.zhejianglab.astro.customresource.abs;

import io.fabric8.crd.generator.annotation.PrinterColumn;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class AbstractIngestTaskStatus {

  @PrinterColumn(name = "STATUS", priority = 3)
  private String status;

  @PrinterColumn(name = "EXCEPTION", priority = 4)
  private String exception;
}
