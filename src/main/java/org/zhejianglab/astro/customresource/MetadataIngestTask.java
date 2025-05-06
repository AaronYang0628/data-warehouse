package org.zhejianglab.astro.customresource;

import io.fabric8.kubernetes.api.model.Namespaced;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.model.annotation.Group;
import io.fabric8.kubernetes.model.annotation.ShortNames;
import io.fabric8.kubernetes.model.annotation.Version;

@Group("org.zhejianglab.astro")
@Version("v1")
@ShortNames("ingest")
public class MetadataIngestTask
    extends CustomResource<MetadataIngestTaskSpec, MetadataIngestTaskStatus> implements Namespaced {

  private static final long serialVersionUID = 1L;
  public static final String KIND = "MetadataIngestTask";
  public static final String PLURAL = "metadataingesttasks";

  @Override
  public String toString() {
    return super.toString();
  }
}
