package org.zhejianglab.astro.utils;

import io.javaoperatorsdk.operator.api.reconciler.ErrorStatusUpdateControl;
import org.zhejianglab.astro.customresource.MetadataIngestTask;

public class ExceptionUtils {

  public static ErrorStatusUpdateControl<MetadataIngestTask> handleError(
      MetadataIngestTask primary, Exception e) {
    //        return ErrorStatusUpdateControl.builder()
    //                .withStatusMessage(e.getMessage())
    //                .build();
    return null;
  }
}
