package org.zhejianglab.astro.customresource.flink;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.*;
import org.zhejianglab.astro.customresource.abs.AbstractIngestTaskStatus;

@Data
@AllArgsConstructor
@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
public class FlinkIngestTaskStatus extends AbstractIngestTaskStatus {}
