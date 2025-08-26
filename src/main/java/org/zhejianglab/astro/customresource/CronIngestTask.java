package org.zhejianglab.astro.customresource;

import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.model.annotation.Group;
import io.fabric8.kubernetes.model.annotation.ShortNames;
import io.fabric8.kubernetes.model.annotation.Version;
import org.zhejianglab.astro.customresource.cron.CronIngestTaskSpec;
import org.zhejianglab.astro.customresource.cron.CronIngestTaskStatus;

@Group("org.zhejianglab.astro.metadata")
@Version("v1")
@ShortNames("croningest")
public class CronIngestTask extends CustomResource<CronIngestTaskSpec, CronIngestTaskStatus> {

  private static final long serialVersionUID = 3L;

  public static final String KIND = "CronIngestTask";

  public static final String PLURAL = "croningesttasks";

  public static final String OPERATOR_NAME = PLURAL + ".org.zhejianglab.astro.metadata";

  public static final String FINALIZER_NAME = OPERATOR_NAME + "/" + "finalizer";

  @Override
  public String toString() {
    return super.toString();
  }
}
