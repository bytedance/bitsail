package com.bytedance.bitsail.connector.legacy.jdbc.sink;

import com.bytedance.bitsail.common.model.ColumnInfo;
import com.bytedance.bitsail.connector.legacy.jdbc.constants.WriteModeProxy;
import com.bytedance.bitsail.connector.legacy.jdbc.utils.upsert.OracleUpsertUtil;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class OracleOutputFormatTest extends OracleOutputFormat {
  @Before
  public void init() {
    this.partitionPatternFormat = "yyyyMMdd";
    this.deleteThreshold = 1000;
    this.partitionName = "date";
    this.tableSchema = "bitsail";
    this.table = "bitsail_test";
    this.tableWithSchema = "bitsail.bitsail_test";
    this.primaryKey = "id";
    this.upsertKeys = new HashMap<>();
    this.extraPartitions = new JDBCOutputExtraPartitions(getDriverName(), getFieldQuote(), getValueQuote());;
    List<String> cols = new ArrayList<>();
    cols.add("pk");
    String[] shardKeys = new String[]{"pk"};
    this.upsertKeys.put("primary_key", cols);
    this.jdbcUpsertUtil = new OracleUpsertUtil(this, shardKeys, this.upsertKeys);
  }

  @Test
  public void testGenClearQuery() {
    String expectClearQuery = "delete from \"bitsail\".\"bitsail_test\" where \"id\" in (select \"id\" from \"bitsail\"." +
            "\"bitsail_test\" where \"date\"=20201229 and rownum < 1000)";
    Assert.assertEquals(expectClearQuery, this.genClearQuery("20201229", "=", ""));
  }

  @Test
  public void testGenClearQueryWithStringPartition() {
    this.partitionType = "varchar";
    String expectClearQuery = "delete from \"bitsail\".\"bitsail_test\" where \"id\" in (select \"id\" from \"bitsail\"." +
            "\"bitsail_test\" where \"date\"='20201229' and rownum < 1000)";
    Assert.assertEquals(expectClearQuery, this.genClearQuery("20201229", "=", ""));
  }

  @Test
  public void testGenInsertQuery() {
    List<ColumnInfo> columns = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      columns.add(new ColumnInfo("col" + i, null));
    }

    // Test Insert query
    String expectedInsertQuery = "INSERT INTO bitsail.bitsail_test (\"col0\",\"col1\",\"date\")\n VALUES (?,?,?) ";
    Assert.assertEquals(expectedInsertQuery, this.genInsertQuery("bitsail_test", columns, WriteModeProxy.WriteMode.insert));
    // Test overwrite query MERGE INTO
    String expectedOverwriteQuery = "MERGE INTO \"bitsail\".\"bitsail_test\" T1 USING (SELECT ? \"col0\",? \"col1\" FROM DUAL) T2 ON (T1.\"pk\"=T2.\"pk\") " +
            "WHEN MATCHED THEN UPDATE SET \"T1\".col0=\"T2\".col0,\"T1\".col1=\"T2\".col1 " +
            "WHEN NOT MATCHED THEN INSERT (\"col0\",\"col1\") VALUES (\"T2\".\"col0\",\"T2\".\"col1\")";
    Assert.assertEquals(expectedOverwriteQuery, this.genInsertQuery("bitsail.bitsail_test", columns, WriteModeProxy.WriteMode.overwrite));
  }
}