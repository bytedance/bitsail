package com.bytedance.bitsail.connector.hbase.source.split.strategy;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.connector.hbase.option.HBaseReaderOptions;
import com.bytedance.bitsail.connector.hbase.source.split.HBaseSourceSplit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SimpleDivideSplitConstructor {
  private static final Logger LOG = LoggerFactory.getLogger(SimpleDivideSplitConstructor.class);
  protected final BitSailConfiguration jobConf;
  protected final String tableName;
  //private SplitConfiguration splitConf;

  public SimpleDivideSplitConstructor(BitSailConfiguration jobConfig) throws IOException {
    this.jobConf = jobConfig;
    this.tableName = jobConf.get(HBaseReaderOptions.TABLE_NAME);

    // TODO
    // get SplitConfig
  }

  public List<HBaseSourceSplit> construct() throws IOException {
    // TODO
    // Allow user to specify split num in the split config?
    return new ArrayList<>();
  }
}
