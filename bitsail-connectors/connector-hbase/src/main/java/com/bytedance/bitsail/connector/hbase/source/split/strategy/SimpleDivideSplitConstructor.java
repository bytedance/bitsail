package com.bytedance.bitsail.connector.hbase.source.split.strategy;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.model.ColumnInfo;
import com.bytedance.bitsail.common.util.Preconditions;
import com.bytedance.bitsail.connector.hbase.HBaseHelper;
import com.bytedance.bitsail.connector.hbase.error.HBasePluginErrorCode;
import com.bytedance.bitsail.connector.hbase.option.HBaseReaderOptions;
import com.bytedance.bitsail.connector.hbase.source.split.HBaseSourceSplit;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
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
  private List<String> columnNames;
  private Set<String> columnFamilies;
  private byte[] minRowKey;
  private byte[] maxRowKey;

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

    this.connection = HBaseHelper.getHbaseConnection(hbaseConfig);

    getSplitRanges();

    // TODO
    // get SplitConfig
  }

  public List<HBaseSourceSplit> construct() throws IOException {
    // TODO
    // Allow user to specify split num in the split config?

    List<HBaseSourceSplit> splits = new ArrayList<>();
    HBaseSourceSplit split = new HBaseSourceSplit(0, this.minRowKey, this.maxRowKey);
    splits.add(split);
    return splits;
  }

  private void getSplitRanges() throws IOException {
    Scan scan = new Scan();
    this.columnFamilies.forEach(cf -> scan.addFamily(Bytes.toBytes(cf)));
    ResultScanner resultScanner = this.connection.getTable(TableName.valueOf(this.tableName)).getScanner(scan);

    try {
      Result curResult = resultScanner.next();
      Result lastResult = null;
      this.minRowKey = new byte[0];
      this.maxRowKey = new byte[0];
      int count = 1;
      while (curResult != null) {
        if (count == 1) {
          this.minRowKey = curResult.getRow();
        }
        lastResult = curResult;
        curResult = resultScanner.next();
        count++;
      }
      this.maxRowKey = lastResult.getRow();

    } catch (Exception e) {
      throw new IOException("Failed to get row key from table " + tableName, e);
    } finally {
      resultScanner.close();
    }
  }
}
