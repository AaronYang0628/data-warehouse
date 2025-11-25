package org.zhejianglab.astro.dependentresource.conditions;

import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.dependent.DependentResource;
import io.javaoperatorsdk.operator.processing.dependent.workflow.Condition;
import org.apache.flink.kubernetes.operator.api.FlinkSessionJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zhejianglab.astro.customresource.FlinkIngestTask;
import org.zhejianglab.astro.customresource.Platform;
import org.zhejianglab.astro.customresource.enums.IngestStatus;

public class FlinkSessionJobDependentCondition
    implements Condition<FlinkSessionJob, FlinkIngestTask> {

  private static final Logger log =
      LoggerFactory.getLogger(FlinkSessionJobDependentCondition.class);

  @Override
  public boolean isMet(
      DependentResource<FlinkSessionJob, FlinkIngestTask> dependentResource,
      FlinkIngestTask primary,
      Context<FlinkIngestTask> context) {

    if (primary.getStatus() != null
        && primary.getStatus().getIngestStatus() == IngestStatus.FINISHED) {
      log.info(
          "FlinkIngestTask {} is already finished, condition not met",
          primary.getMetadata().getName());
      return false;
    }

    boolean platformMatches =
        primary.getSpec().getPlatform().equalsIgnoreCase(Platform.OSS.getProtocol())
            || primary.getSpec().getPlatform().equalsIgnoreCase(Platform.S3.getProtocol())
            || primary.getSpec().getPlatform().equalsIgnoreCase(Platform.JDBC.getProtocol());

    if (!platformMatches) {
      log.debug("Platform {} does not match, condition not met", primary.getSpec().getPlatform());
    }

    return platformMatches;
  }
}
