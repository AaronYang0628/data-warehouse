package org.acme;

import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.dependent.Dependent;
import io.javaoperatorsdk.operator.api.reconciler.Workflow;

import java.util.Map;
import java.util.Optional;

@Workflow(dependents = {@Dependent(type = ConfigMapDependentResource.class)})
public class MetadataOperatorReconciler implements Reconciler<MetadataOperatorCustomResource> {

    public UpdateControl<MetadataOperatorCustomResource> reconcile(MetadataOperatorCustomResource primary,
                                                     Context<MetadataOperatorCustomResource> context) {

        return UpdateControl.noUpdate();
    }
}
