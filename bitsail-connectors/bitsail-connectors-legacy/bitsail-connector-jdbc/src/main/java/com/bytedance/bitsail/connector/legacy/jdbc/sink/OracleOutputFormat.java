package com.bytedance.bitsail.connector.legacy.jdbc.sink;

import com.bytedance.dts.base.constants.WriteModeProxy;
import com.bytedance.dts.batch.jdbc.error.JDBCPluginErrorCode;
import com.bytedance.dts.batch.jdbc.util.JDBCUpsertUtil;
import com.bytedance.dts.batch.jdbc.util.OracleUpsertUtil;
import com.bytedance.dts.common.configuration.WriterOptions;
import com.bytedance.dts.common.model.ColumnInfo;
import com.bytedance.dts.common.util.OracleUtil;
import com.bytedance.dts.common.util.TypeConvertUtil.StorageEngine;
import com.google.common.collect.ImmutableSet;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class OracleOutputFormat extends JDBCOutputFormat {

  protected String primaryKey;
  protected String tableSchema;
  protected String tableWithSchema;
  protected Set<String> supportedOraclePartitionTypeSets01 = ImmutableSet.of(
          "varchar", "varchar2", "int", "bigint", "number", "numeric");

  @Override
  public void initPlugin() throws IOException {
    primaryKey = outputSliceConfig.getNecessaryOption(WriterOptions.OracleWriterOptions.PRIMARY_KEY, JDBCPluginErrorCode.REQUIRED_VALUE);
    tableSchema = outputSliceConfig.getNecessaryOption(WriterOptions.OracleWriterOptions.DB_NAME, JDBCPluginErrorCode.REQUIRED_VALUE);
    table = outputSliceConfig.getNecessaryOption(WriterOptions.OracleWriterOptions.TABLE_NAME, JDBCPluginErrorCode.REQUIRED_VALUE);
    tableWithSchema = tableSchema + "." + table;
    super.initPlugin();
  }

  /*
   * 覆盖写模式，获取可能有冲突的Unique索引列表
   */
  @Override
  protected Map<String, List<String>> initUniqueIndexColumnsMap() throws IOException {
    OracleUtil oracleUtil = new OracleUtil();
    try {
      return oracleUtil.getIndexColumnsMap(dbURL, username, password, null, tableSchema, table, true);
    } catch (Exception e) {
      throw new IOException("unable to get unique indexes info, Error: " + e.toString());
    }
  }

  @Override
  protected JDBCUpsertUtil initUpsertUtils () {
    return new OracleUpsertUtil(this, shardKeys, upsertKeys);
  }

  @Override
  public String getDriverName() {
    return OracleUtil.DRIVER_NAME;
  }

  @Override
  public StorageEngine getStorageEngine() {
    return StorageEngine.oracle;
  }

  @Override
  public String getFieldQuote() {
    return OracleUtil.DB_QUOTE;
  }

  @Override
  public String getValueQuote() {
    return OracleUtil.VALUE_QUOTE;
  }

  @Override
  public String getType() {
    return "Oracle";
  }

  @Override
  public Set<String> getSupportedPartitionType01() {
    return supportedOraclePartitionTypeSets01;
  }
  /*
   * 生成定量删除数据delete语句，oracle不支持delete limit语句，替换为delete from table where rownum < threshold
   */
  @Override
  public String genClearQuery(String partitionValue, String compare, String extraPartitionsSql) {
    final String tableWithQuote = getQuoteTable(tableWithSchema);
    final String primaryKeyWithQuote = getQuoteColumn(primaryKey);
    String selectQuery;
    //整形
    if (partitionPatternFormat.equals("yyyyMMdd")) {
      selectQuery = "select " + primaryKeyWithQuote + " from " + tableWithQuote + " where " + getQuoteColumn(partitionName) +
              compare + wrapPartitionValueWithQuota(partitionValue) + extraPartitionsSql + " and rownum < " + deleteThresHold;
    } else {
      //date类型
      selectQuery = "select " + primaryKeyWithQuote + " from " + tableWithQuote + " where " + getQuoteColumn(partitionName) +
              compare + getQuoteValue(partitionValue) + extraPartitionsSql + " and rownum < " + deleteThresHold;
    }
    return "delete from " + tableWithQuote + " where " + primaryKeyWithQuote + " in (" + selectQuery + ")";
  }

  /*
   * 生成oracle insert语句需携带schema信息
   */
  @Override
  String genInsertQuery(String table, List<ColumnInfo> columns, WriteModeProxy.WriteMode writeMode) {
    return super.genInsertQuery(tableWithSchema, columns, writeMode);
  }

}
