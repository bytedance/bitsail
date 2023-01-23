package com.bytedance.bitsail.connector.hbase.source.split;

import com.bytedance.bitsail.base.connector.reader.v1.SourceSplit;

import lombok.Setter;

@Setter
public class HBaseSourceSplit implements SourceSplit {
  public static final String HBASE_SOURCE_SPLIT_PREFIX = "hbase_source_split_";
  private final String splitId;

  /**
   * Read whole table or not
   */
  private boolean readTable;

  public HBaseSourceSplit(int splitId) {
    this.splitId = HBASE_SOURCE_SPLIT_PREFIX + splitId;
  }

  @Override
  public String uniqSplitId() {
    return splitId;
  }

  @Override
  public boolean equals(Object obj) {
    return (obj instanceof HBaseSourceSplit) && (splitId.equals(((HBaseSourceSplit) obj).splitId));
  }

  @Override
  public String toString() {
    return String.format(
        "{\"split_id\":\"%s\", \"readTable\":%s}",
        splitId, readTable);
  }
}
