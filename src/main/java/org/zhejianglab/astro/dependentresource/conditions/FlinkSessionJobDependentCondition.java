package org.zhejianglab.astro.dependentresource.conditions;

import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.dependent.DependentResource;
import io.javaoperatorsdk.operator.processing.dependent.workflow.Condition;
import org.apache.flink.kubernetes.operator.api.FlinkSessionJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zhejianglab.astro.customresource.FlinkIngestTask;
import org.zhejianglab.astro.customresource.Platform;

public class FlinkSessionJobDependentCondition
    implements Condition<FlinkSessionJob, FlinkIngestTask> {

  private static final Logger log =
      LoggerFactory.getLogger(FlinkSessionJobDependentCondition.class);

  @Override
  public boolean isMet(
      DependentResource<FlinkSessionJob, FlinkIngestTask> dependentResource,
      FlinkIngestTask primary,
      Context<FlinkIngestTask> context) {

    // 如果作业已完成，则不需要重新创建FlinkSessionJob
    if (primary.getStatus() != null && "FINISHED".equals(primary.getStatus().getJobStatus())) {
      log.info(
          "Flink job {} is already finished, skipping FlinkSessionJob creation",
          primary.getMetadata().getName());
      return false;
    }

    return primary.getSpec().getPlatform().equalsIgnoreCase(Platform.OSS.getProtocol())
        || primary.getSpec().getPlatform().equalsIgnoreCase(Platform.S3.getProtocol())
        || primary.getSpec().getPlatform().equalsIgnoreCase(Platform.JDBC.getProtocol());
  }
}
