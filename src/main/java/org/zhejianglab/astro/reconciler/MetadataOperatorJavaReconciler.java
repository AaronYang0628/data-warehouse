package org.zhejianglab.astro.reconciler;

import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import io.javaoperatorsdk.operator.api.reconciler.Workflow;
import io.javaoperatorsdk.operator.api.reconciler.dependent.Dependent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zhejianglab.astro.customresource.MetadataIngestTask;
import org.zhejianglab.astro.dependentresource.ConfigMapDependentResource;
import org.zhejianglab.astro.dependentresource.JavaCRUDDependentCondition;

@Workflow(
    dependents = {
      @Dependent(
          type = ConfigMapDependentResource.class,
          reconcilePrecondition = JavaCRUDDependentCondition.class,
          activationCondition = JavaCRUDDependentCondition.class)
    })
public class MetadataOperatorJavaReconciler implements Reconciler<MetadataIngestTask> {

  private static final Logger log = LoggerFactory.getLogger(MetadataOperatorJavaReconciler.class);

  public UpdateControl<MetadataIngestTask> reconcile(
      MetadataIngestTask primary, Context<MetadataIngestTask> context) {

    log.info("Reconcile java platform");
    return UpdateControl.noUpdate();
  }
}
