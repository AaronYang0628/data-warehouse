package org.zhejianglab.astro.customresource;

import io.fabric8.kubernetes.api.model.Namespaced;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.model.annotation.Group;
import io.fabric8.kubernetes.model.annotation.ShortNames;
import io.fabric8.kubernetes.model.annotation.Version;
import org.zhejianglab.astro.customresource.flink.FlinkIngestTaskSpec;
import org.zhejianglab.astro.customresource.flink.FlinkIngestTaskStatus;

@Group("org.zhejianglab.astro.metadata")
@Version("v1")
@ShortNames("flinkingest")
public class FlinkIngestTask extends CustomResource<FlinkIngestTaskSpec, FlinkIngestTaskStatus>
    implements Namespaced {

  private static final long serialVersionUID = 1L;

  public static final String KIND = "FlinkIngestTask";

  public static final String PLURAL = "flinkingesttasks";

  public static final String OPERATOR_NAME = PLURAL + ".org.zhejianglab.astro.metadata";

  public static final String FINALIZER_NAME = OPERATOR_NAME + "/" + "finalizer";

  @Override
  public String toString() {
    return super.toString();
  }
}
