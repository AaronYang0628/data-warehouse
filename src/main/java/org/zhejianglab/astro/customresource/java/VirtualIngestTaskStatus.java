package org.zhejianglab.astro.customresource.java;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import org.zhejianglab.astro.customresource.abs.AbstractIngestTaskStatus;

@Data
@AllArgsConstructor
@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
public class VirtualIngestTaskStatus extends AbstractIngestTaskStatus {}
