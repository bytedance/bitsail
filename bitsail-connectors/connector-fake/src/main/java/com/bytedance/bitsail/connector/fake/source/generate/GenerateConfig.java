package com.bytedance.bitsail.connector.fake.source.generate;

import java.time.ZoneId;
import java.util.concurrent.atomic.AtomicLong;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

import java.sql.Timestamp;

@Builder
@Getter
@AllArgsConstructor
public class GenerateConfig {

  private Integer taskId;
  private final transient AtomicLong rowId;
  private final long upper;
  private final long lower;
  private final transient Timestamp fromTimestamp;
  private final transient Timestamp toTimestamp;
  private final ZoneId zoneId = ZoneId.systemDefault();

}
