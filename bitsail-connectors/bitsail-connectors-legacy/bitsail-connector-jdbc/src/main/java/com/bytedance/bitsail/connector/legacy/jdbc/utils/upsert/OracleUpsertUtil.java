package com.bytedance.bitsail.connector.legacy.jdbc.utils.upsert;

import com.bytedance.dts.batch.jdbc.JDBCOutputFormat;
import org.apache.commons.collections.CollectionUtils;

import java.util.List;
import java.util.Map;

public class OracleUpsertUtil extends JDBCUpsertUtil{
  public OracleUpsertUtil(JDBCOutputFormat jdbcOutputFormat, String[] shardKeys, Map<String, List<String>> upsertKeys) {
    super(jdbcOutputFormat, shardKeys, upsertKeys);
  }

  @Override
  public String genUpsertTemplate(String table, List<String> columns, String targetUniqueKey) {
    if(upsertKeys == null || upsertKeys.isEmpty()) {
      return getInsertStatement(columns, table);
    }

    List<String> updateColumns = getUpdateColumns(columns, upsertKeys);
    if(CollectionUtils.isEmpty(updateColumns)){
      //出现唯一性约束冲突时，没有要更新的列
      return "MERGE INTO " + quoteTable(table) + " T1 USING "
              + "(" + makeValues(columns) + ") T2 ON ("
              + equalUpsertKeysSql(upsertKeys) + ") WHEN NOT MATCHED THEN "
              + "INSERT (" + quoteColumns(columns) + ") VALUES ("
              + quoteColumns(columns, "T2") + ")";
    } else {
      return "MERGE INTO " + quoteTable(table) + " T1 USING "
              + "(" + makeValues(columns) + ") T2 ON ("
              + equalUpsertKeysSql(upsertKeys) + ") WHEN MATCHED THEN UPDATE SET "
              + getUpdateSql(updateColumns, "T1", "T2") + " WHEN NOT MATCHED THEN "
              + "INSERT (" + quoteColumns(columns) + ") VALUES ("
              + quoteColumns(columns, "T2") + ")";
    }
  }

  @Override
  protected String makeValues(List<String> column) {
    StringBuilder sb = new StringBuilder("SELECT ");
    for(int i = 0; i < column.size(); ++i) {
      if(i != 0) {
        sb.append(",");
      }
      sb.append("? " + quoteColumn(column.get(i)));
    }
    sb.append(" FROM DUAL");
    return sb.toString();
  }
}
