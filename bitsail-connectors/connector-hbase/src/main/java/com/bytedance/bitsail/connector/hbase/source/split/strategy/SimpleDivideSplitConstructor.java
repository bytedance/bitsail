package com.bytedance.bitsail.connector.hbase.source.split.strategy;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.model.ColumnInfo;
import com.bytedance.bitsail.common.util.Preconditions;
import com.bytedance.bitsail.connector.hbase.HBaseHelper;
import com.bytedance.bitsail.connector.hbase.error.HBasePluginErrorCode;
import com.bytedance.bitsail.connector.hbase.option.HBaseReaderOptions;
import com.bytedance.bitsail.connector.hbase.source.split.HBaseSourceSplit;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class SimpleDivideSplitConstructor {
  private static final Logger LOG = LoggerFactory.getLogger(SimpleDivideSplitConstructor.class);
  protected final BitSailConfiguration jobConf;
  protected final String tableName;
  private Map<String, Object> hbaseConfig;
  private static final String ROW_KEY = "rowkey";
  private final transient Connection connection;
  private final List<String> columnNames;
  private Set<String> columnFamilies;

  public SimpleDivideSplitConstructor(BitSailConfiguration jobConfig) throws IOException {
    this.jobConf = jobConfig;
    this.hbaseConfig = jobConf.get(HBaseReaderOptions.HBASE_CONF);
    this.tableName = jobConf.get(HBaseReaderOptions.TABLE);

    this.columnFamilies = new LinkedHashSet<>();
    List<ColumnInfo> columnInfos = jobConf.getNecessaryOption(
            HBaseReaderOptions.COLUMNS, HBasePluginErrorCode.REQUIRED_VALUE);
    this.columnNames = columnInfos.stream().map(ColumnInfo::getName).collect(Collectors.toList());
    // Check if input column names are in format: [ columnFamily:column ].
    this.columnNames.stream().peek(column -> Preconditions.checkArgument(
                    (column.contains(":") && column.split(":").length == 2) ||
                            ROW_KEY.equalsIgnoreCase(column),
                    "Invalid column names, it should be [ColumnFamily:Column] format"))
            .forEach(column -> this.columnFamilies.add(column.split(":")[0]));

    HBaseHelper hbaseHelper = new HBaseHelper();
    this.connection = hbaseHelper.getHbaseConnection(hbaseConfig);
  }

  public List<HBaseSourceSplit> construct() throws IOException {
    List<HBaseSourceSplit> splits = new ArrayList<>();

    RegionLocator regionLocator = connection.getRegionLocator(TableName.valueOf(tableName));
    byte[][] startKeys = regionLocator.getStartKeys();
    byte[][] endKeys = regionLocator.getEndKeys();
    if (startKeys.length != endKeys.length) {
      throw new IOException("Failed to create Splits for HBase table {}. HBase start keys and end keys not equal." + tableName);
    }

    int i = 0;
    while (i < startKeys.length) {
      splits.add(new HBaseSourceSplit(i, startKeys[i], endKeys[i]));
      i++;
    }
    return splits;
  }
}
