package com.bytedance.bitsail.connector.legacy.jdbc.utils;

import com.bytedance.dts.batch.jdbc.OracleOutputFormat;
import junit.framework.TestCase;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OracleUpsertUtilTest extends TestCase {
  @Test
  public void testGenUpsertTemplate() {
    List<String> columns = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      columns.add("col" + i);
    }

    String[] shardKeys = new String[]{"byte"};
    Map<String, List<String>> upsertKeys = new HashMap<>();
    List<String> cols = new ArrayList<>();
    cols.add("pk");
    upsertKeys.put("primary_key", cols);
    OracleUpsertUtil upsertUtil = new OracleUpsertUtil(new OracleOutputFormat(), shardKeys, upsertKeys);
    //测试覆盖写入生成的merge into语句
    String expectedOverwriteQuery = "MERGE INTO \"dts_test\" T1 USING (SELECT ? \"col0\",? \"col1\" FROM DUAL) T2 ON (T1.\"pk\"=T2.\"pk\") " +
            "WHEN MATCHED THEN UPDATE SET \"T1\".col0=\"T2\".col0,\"T1\".col1=\"T2\".col1 " +
            "WHEN NOT MATCHED THEN INSERT (\"col0\",\"col1\") VALUES (\"T2\".\"col0\",\"T2\".\"col1\")";
    Assert.assertEquals(expectedOverwriteQuery, upsertUtil.genUpsertTemplate("dts_test", columns, ""));
  }
}