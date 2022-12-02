package com.bytedance.bitsail.connector.fake.source.generate;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

import java.sql.Timestamp;

@Builder
@Getter
@AllArgsConstructor
public class ColumnConfig {

  private Integer taskId;
  private final long upper;
  private final long lower;
  private final transient Timestamp fromTimestamp;
  private final transient Timestamp toTimestamp;

}
