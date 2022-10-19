package com.bytedance.bitsail.connector.legacy.jdbc.sink;

import com.bytedance.dts.base.constants.WriteModeProxy;
import com.bytedance.dts.batch.jdbc.util.OracleUpsertUtil;
import com.bytedance.dts.common.model.ColumnInfo;
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
    this.deleteThresHold = 1000;
    this.partitionName = "date";
    this.tableSchema = "dts";
    this.table = "dts_test";
    this.tableWithSchema = "dts.dts_test";
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
    String expectClearQuery = "delete from \"dts\".\"dts_test\" where \"id\" in (select \"id\" from \"dts\".\"dts_test\" where \"date\"=20201229 and rownum < 1000)";
    Assert.assertEquals(expectClearQuery, this.genClearQuery("20201229", "=", ""));
  }

  @Test
  public void testGenClearQueryWithStringPartition() {
    this.partitionType = "varchar";
    String expectClearQuery = "delete from \"dts\".\"dts_test\" where \"id\" in (select \"id\" from \"dts\".\"dts_test\" where \"date\"='20201229' and rownum < 1000)";
    Assert.assertEquals(expectClearQuery, this.genClearQuery("20201229", "=", ""));
  }

  @Test
  public void testGenInsertQuery() {
    List<ColumnInfo> columns = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      columns.add(new ColumnInfo("col" + i, null));
    }

    //测试静态写入生成的insert语句
    String expectedInsertQuery = "INSERT INTO dts.dts_test (\"col0\",\"col1\",\"date\")\n VALUES (?,?,?) ";
    Assert.assertEquals(expectedInsertQuery, this.genInsertQuery("dts_test", columns, WriteModeProxy.WriteMode.insert));
    //测试覆盖写入生成的merge into语句
    String expectedOverwriteQuery = "MERGE INTO \"dts\".\"dts_test\" T1 USING (SELECT ? \"col0\",? \"col1\" FROM DUAL) T2 ON (T1.\"pk\"=T2.\"pk\") " +
            "WHEN MATCHED THEN UPDATE SET \"T1\".col0=\"T2\".col0,\"T1\".col1=\"T2\".col1 " +
            "WHEN NOT MATCHED THEN INSERT (\"col0\",\"col1\") VALUES (\"T2\".\"col0\",\"T2\".\"col1\")";
    Assert.assertEquals(expectedOverwriteQuery, this.genInsertQuery("dts.dts_test", columns, WriteModeProxy.WriteMode.overwrite));
    //测试动态写入生成的insert语句
    String expectedDynamicQuery = "INSERT INTO dts.dts_test (\"col0\",\"col1\")\n VALUES (?,?) ";
    Assert.assertEquals(expectedDynamicQuery, this.genInsertQuery("dts.dts_test", columns, WriteModeProxy.WriteMode.dynamic));
  }
}