package org.zhejianglab.astro;

import io.fabric8.kubernetes.api.model.Namespaced;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.model.annotation.Group;
import io.fabric8.kubernetes.model.annotation.Version;

@Group("org.zhejianglab.astro")
@Version("v1")
public class MetadataOperatorCustomResource
    extends CustomResource<MetadataOperatorSpec, MetadataOperatorStatus> implements Namespaced {}
