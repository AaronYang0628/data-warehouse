package org.zhejianglab.astro;

import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import io.javaoperatorsdk.operator.api.reconciler.Workflow;
import io.javaoperatorsdk.operator.api.reconciler.dependent.Dependent;
import org.zhejianglab.astro.customresource.MetadataOperator;

@Workflow(dependents = {@Dependent(type = ConfigMapDependentResource.class)})
public class MetadataOperatorReconciler implements Reconciler<MetadataOperator> {

  public UpdateControl<MetadataOperator> reconcile(
      MetadataOperator primary, Context<MetadataOperator> context) {

    return UpdateControl.noUpdate();
  }
}
