package org.zhejianglab.astro.dependentresource.conditions;

import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.dependent.DependentResource;
import io.javaoperatorsdk.operator.processing.dependent.workflow.Condition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zhejianglab.astro.customresource.FlinkIngestTask;
import org.zhejianglab.astro.customresource.enums.IngestStatus;

public class FinishedAuditJobDependentCondition implements Condition<Job, FlinkIngestTask> {

  private static final Logger log =
      LoggerFactory.getLogger(FinishedAuditJobDependentCondition.class);

  @Override
  public boolean isMet(
      DependentResource<Job, FlinkIngestTask> dependentResource,
      FlinkIngestTask primary,
      Context<FlinkIngestTask> context) {

    if (primary.getStatus() == null
        || primary.getStatus().getIngestStatus() != IngestStatus.FINISHED) {
      log.debug(
          "FlinkIngestTask {} is not in FINISHED state, condition not met",
          primary.getMetadata().getName());
      return false;
    }

    String jobName = primary.getMetadata().getName() + "-finished-audit-job";
    Job existingJob =
        context
            .getClient()
            .batch()
            .v1()
            .jobs()
            .inNamespace(primary.getMetadata().getNamespace())
            .withName(jobName)
            .get();

    if (existingJob != null) {
      log.info("Audit job {} already exists, condition not met", jobName);
      return false;
    }

    log.info("Task is finished and audit job does not exist, condition met");
    return true;
  }
}
