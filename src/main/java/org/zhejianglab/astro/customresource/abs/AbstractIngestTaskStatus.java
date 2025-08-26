package org.zhejianglab.astro.customresource.abs;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class AbstractIngestTaskStatus {

  private String exception;
  private String status;
}
