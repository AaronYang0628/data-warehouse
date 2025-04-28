package org.zhejianglab.astro;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.CRUDKubernetesDependentResource;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.KubernetesDependent;
import java.util.Map;
import org.zhejianglab.astro.customresource.MetadataOperator;

@KubernetesDependent
public class ConfigMapDependentResource
    extends CRUDKubernetesDependentResource<ConfigMap, MetadataOperator> {

  public static final String KEY = "key";

  public ConfigMapDependentResource() {
    super(ConfigMap.class);
  }

  @Override
  protected ConfigMap desired(MetadataOperator primary, Context<MetadataOperator> context) {
    return new ConfigMapBuilder()
        .withMetadata(
            new ObjectMetaBuilder()
                .withName(primary.getMetadata().getName())
                .withNamespace(primary.getMetadata().getNamespace())
                .build())
        .withData(Map.of(KEY, primary.getSpec().getValue()))
        .build();
  }
}
