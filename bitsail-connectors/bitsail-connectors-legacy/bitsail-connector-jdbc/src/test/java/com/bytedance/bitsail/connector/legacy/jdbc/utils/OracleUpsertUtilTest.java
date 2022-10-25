/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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