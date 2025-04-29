package org.zhejianglab.astro;

import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import io.javaoperatorsdk.operator.api.reconciler.Workflow;
import io.javaoperatorsdk.operator.api.reconciler.dependent.Dependent;
import org.zhejianglab.astro.customresource.MetadataIngestTask;
import org.zhejianglab.astro.dependentresource.ConfigMapDependentResource;

@Workflow(dependents = {@Dependent(type = ConfigMapDependentResource.class)})
public class MetadataOperatorReconciler implements Reconciler<MetadataIngestTask> {

  public UpdateControl<MetadataIngestTask> reconcile(
      MetadataIngestTask primary, Context<MetadataIngestTask> context) {

    return UpdateControl.noUpdate();
  }
}
