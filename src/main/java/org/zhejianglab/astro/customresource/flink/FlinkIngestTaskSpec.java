package org.zhejianglab.astro.customresource.flink;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.*;
import lombok.extern.jackson.Jacksonized;
import org.apache.commons.lang3.StringUtils;
import org.zhejianglab.astro.customresource.abs.AbstractIngestTaskSpec;

@Data
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class FlinkIngestTaskSpec extends AbstractIngestTaskSpec {

  private static final ObjectMapper objectMapper = new ObjectMapper();

  private ExtraSecret extraSecret;
  private Map<String, String> pathPatterns;
  private List<String> allowedSuffixes;
  @Nullable private String batchId;

  /** job parallelism which will be used to override the flinkJobConfig.JobSpc.parallelism */
  @Nullable private Integer jobParallelism;

  @Nullable private FlinkJobConfig flinkJobConfig;

  @Nullable private String s3TableName;

  @Builder
  @Jacksonized
  public FlinkIngestTaskSpec(
      String path,
      String platform,
      ExtraSecret extraSecret,
      List<String> tags,
      Map<String, String> userProperties,
      Map<String, String> pathPatterns,
      List<String> allowedSuffixes,
      @Nullable String batchId,
      @Nullable Integer timeout,
      @Nullable Integer jobParallelism,
      @Nullable String s3TableName,
      @Nullable FlinkJobConfig flinkJobConfig) {
    this.setPath(path);
    this.setPlatform(platform);
    this.setTimeout(null != timeout ? timeout : 20);
    this.setBatchId(null != batchId ? batchId : generateMD5());
    this.setTags(tags);
    this.setUserProperties(userProperties);
    this.extraSecret = extraSecret;
    this.pathPatterns = pathPatterns;
    this.allowedSuffixes = allowedSuffixes;

    this.jobParallelism = null != jobParallelism ? jobParallelism : 5;

    this.s3TableName = null != s3TableName ? s3TableName : StringUtils.EMPTY;

    if (null == flinkJobConfig) {
      this.flinkJobConfig = FlinkJobConfig.builder().build().initSessionJobDefaultConfig(this);
      this.flinkJobConfig.getJob().setParallelism(this.jobParallelism);
      this.flinkJobConfig.getJobArgsMap().put("S3_TABLE_NAME", this.s3TableName);
    } else {
      this.flinkJobConfig = flinkJobConfig;
    }
  }

  private String generateMD5() {
    try {
      String jsonSpec = objectMapper.writeValueAsString(this);

      MessageDigest md = MessageDigest.getInstance("MD5");
      byte[] messageDigest = md.digest(jsonSpec.getBytes());

      BigInteger no = new BigInteger(1, messageDigest);
      String hashtext = no.toString(16);

      while (hashtext.length() < 32) {
        hashtext = "0" + hashtext;
      }

      return hashtext.substring(0, 20);
    } catch (JsonProcessingException | NoSuchAlgorithmException e) {
      return "00000000000000000000";
    }
  }
}
