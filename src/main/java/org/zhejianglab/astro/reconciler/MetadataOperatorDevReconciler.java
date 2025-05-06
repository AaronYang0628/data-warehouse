package org.zhejianglab.astro.reconciler;

import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import io.javaoperatorsdk.operator.api.reconciler.Workflow;
import io.javaoperatorsdk.operator.api.reconciler.dependent.Dependent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zhejianglab.astro.customresource.MetadataIngestTask;
import org.zhejianglab.astro.customresource.MetadataIngestTaskSpec;
import org.zhejianglab.astro.dependentresource.FlinkDeploymentDependentResource;

@Workflow(dependents = {@Dependent(type = FlinkDeploymentDependentResource.class)})
public class MetadataOperatorDevReconciler implements Reconciler<MetadataIngestTask> {

  private static final Logger log = LoggerFactory.getLogger(MetadataOperatorDevReconciler.class);

  public UpdateControl<MetadataIngestTask> reconcile(
      MetadataIngestTask primary, Context<MetadataIngestTask> context) {

    MetadataIngestTaskSpec spec = primary.getSpec();

    log.info("current go spec {}", spec);
    return UpdateControl.noUpdate();
  }
}
