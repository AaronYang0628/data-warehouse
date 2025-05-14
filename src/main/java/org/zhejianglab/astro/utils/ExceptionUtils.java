package org.zhejianglab.astro.utils;

import io.javaoperatorsdk.operator.api.reconciler.ErrorStatusUpdateControl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zhejianglab.astro.customresource.MetadataIngestTask;

public class ExceptionUtils {

  private static final Logger log = LoggerFactory.getLogger(ExceptionUtils.class);

  public static ErrorStatusUpdateControl<MetadataIngestTask> handleError(
      MetadataIngestTask primary, Exception e) {
    log.error("Error occurred while reconciling task: {}", primary.getMetadata().getName(), e);

    // Return appropriate control directive, e.g., update status with error message
    return ErrorStatusUpdateControl.defaultErrorProcessing().noStatusUpdate();
  }
}
