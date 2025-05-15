package org.zhejianglab.astro.dependentresource;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.CRUDKubernetesDependentResource;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.KubernetesDependent;
import java.util.Map;
import org.zhejianglab.astro.customresource.VirtualIngestTask;

@KubernetesDependent
public class ConfigMapDependentResource
    extends CRUDKubernetesDependentResource<ConfigMap, VirtualIngestTask> {

  public static final String KEY = "key";

  public ConfigMapDependentResource() {
    super(ConfigMap.class);
  }

  @Override
  protected ConfigMap desired(VirtualIngestTask primary, Context<VirtualIngestTask> context) {
    return new ConfigMapBuilder()
        .withMetadata(
            new ObjectMetaBuilder()
                .withName(primary.getMetadata().getName())
                .withNamespace(primary.getMetadata().getNamespace())
                .build())
        .withData(Map.of("A", "B"))
        .build();
  }
}
