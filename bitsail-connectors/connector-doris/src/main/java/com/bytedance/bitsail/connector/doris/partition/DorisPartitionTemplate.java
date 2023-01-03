package com.bytedance.bitsail.connector.doris.partition;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class DorisPartitionTemplate {

  @JsonProperty(value = "prefix", required = false, defaultValue = "p")
  private String prefix;

  @JsonProperty(value = "start_range", required = true)
  private String startRange;

  @JsonProperty(value = "end_range", required = true)
  private String endRange;

  @JsonProperty(value = "pattern", required = false, defaultValue = "yyyy-MM-dd")
  private String pattern;

}
