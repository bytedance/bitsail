package com.bytedance.bitsail.connector.legacy.jdbc.utils;

import com.bytedance.bitsail.connector.legacy.jdbc.sink.OracleOutputFormat;
import com.bytedance.bitsail.connector.legacy.jdbc.utils.upsert.OracleUpsertUtil;

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
      columns.add("COL" + i);
    }

    String[] shardKeys = new String[]{"byte"};
    Map<String, List<String>> upsertKeys = new HashMap<>();
    List<String> cols = new ArrayList<>();
    cols.add("PK");
    upsertKeys.put("primary_key", cols);
    OracleUpsertUtil upsertUtil = new OracleUpsertUtil(new OracleOutputFormat(), shardKeys, upsertKeys);
    // Test overwriting query MERGE INTO
    String expectedOverwriteQuery = "MERGE INTO \"BITSAIL_TEST\" T1 USING (SELECT ? \"COL0\",? \"COL1\" FROM DUAL) T2 ON (T1.\"PK\"=T2.\"PK\") " +
            "WHEN MATCHED THEN UPDATE SET \"T1\".COL0=\"T2\".COL0,\"T1\".COL1=\"T2\".COL1 " +
            "WHEN NOT MATCHED THEN INSERT (\"COL0\",\"COL1\") VALUES (\"T2\".\"COL0\",\"T2\".\"COL1\")";
    Assert.assertEquals(expectedOverwriteQuery, upsertUtil.genUpsertTemplate("BITSAIL_TEST", columns, ""));
  }
}