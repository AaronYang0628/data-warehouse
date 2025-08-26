package org.zhejianglab.astro.reconciler;

import io.javaoperatorsdk.operator.api.reconciler.*;
import io.javaoperatorsdk.operator.api.reconciler.dependent.Dependent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zhejianglab.astro.customresource.CronIngestTask;
import org.zhejianglab.astro.dependentresource.CronJobDependentResource;
import org.zhejianglab.astro.dependentresource.conditions.CornFlinkSessionJobDependentCondition;

@Workflow(
    explicitInvocation = true,
    dependents = {
      @Dependent(
          type = CronJobDependentResource.class,
          reconcilePrecondition = CornFlinkSessionJobDependentCondition.class),
    })
public class MetadataOperatorCornReconciler
    implements Reconciler<CronIngestTask>, Cleaner<CronIngestTask> {

  private static final Logger log = LoggerFactory.getLogger(MetadataOperatorCornReconciler.class);

  public UpdateControl<CronIngestTask> reconcile(
      CronIngestTask primary, Context<CronIngestTask> context) {
    return UpdateControl.noUpdate();
  }

  @Override
  public DeleteControl cleanup(CronIngestTask resource, Context<CronIngestTask> context)
      throws Exception {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("Unimplemented method 'cleanup'");
  }
}
