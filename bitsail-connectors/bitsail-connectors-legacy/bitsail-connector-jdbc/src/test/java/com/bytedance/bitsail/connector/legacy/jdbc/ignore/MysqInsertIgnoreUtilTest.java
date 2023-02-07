/*
 * Copyright 2022-2023 Bytedance Ltd. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.bytedance.bitsail.connector.legacy.jdbc.ignore;

import com.bytedance.bitsail.connector.legacy.jdbc.sink.JDBCOutputFormat;
import com.bytedance.bitsail.connector.legacy.jdbc.utils.ignore.MysqlInsertIgnoreUtil;

import junit.framework.TestCase;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class MysqInsertIgnoreUtilTest extends TestCase {

  @Test
  public void testGenUpsertTemplateWithNullUpsertKeys() {
    String[] shardKeys = new String[]{"byte"};
    MysqlInsertIgnoreUtil upsertUtil = new MysqlInsertIgnoreUtil(new JDBCOutputFormat(), shardKeys);
    // Test overwriting query MERGE INTO
    String expectedOverwriteQuery = "INSERT IGNORE INTO BITSAIL_TEST (`COL0`,`COL1`) VALUES (?,?)";
    Assert.assertEquals(expectedOverwriteQuery, upsertUtil.genInsertIgnoreTemplate("BITSAIL_TEST", provideColumns(2)));
  }

  private List<String> provideColumns(final int size) {
    final List<String> columns = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      columns.add("COL" + i);
    }
    return columns;
  }
}