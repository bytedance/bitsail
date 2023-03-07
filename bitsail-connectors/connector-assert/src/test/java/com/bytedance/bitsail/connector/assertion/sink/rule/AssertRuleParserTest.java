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

package com.bytedance.bitsail.connector.assertion.sink.rule;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.model.ColumnInfo;
import com.bytedance.bitsail.common.option.WriterOptions;
import com.bytedance.bitsail.common.typeinfo.BasicTypeInfo;
import com.bytedance.bitsail.connector.assertion.sink.constants.AssertRuleType;
import com.bytedance.bitsail.connector.assertion.sink.option.AssertWriterOptions;

import org.junit.Test;

import java.io.File;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class AssertRuleParserTest {
  private final AssertRuleParser assertRuleParser = new AssertRuleParser();

  private static final String COLUMN_NAME = "name";
  private static final String COLUMN_PRICE = "price";
  private static final double DELTA = 1e-9;

  @Test
  public void testParseRowRules() throws Exception {
    BitSailConfiguration jobConf = BitSailConfiguration.from(new File(
        Paths.get(getClass().getClassLoader().getResource("assert_sink.json").toURI()).toString()));

    Map<String, Object> rowRules = jobConf.get(AssertWriterOptions.ROW_RULES);
    assertNotNull(rowRules);
    List<AssertSimpleRule> assertRowRules = assertRuleParser.parseRowRules(rowRules);
    assertEquals(2, assertRowRules.size());

    assertEquals(assertRowRules.get(0).getRuleType(), AssertRuleType.MIN_ROW);
    assertEquals(assertRowRules.get(0).getRuleValue(), 10, DELTA);
    assertEquals(assertRowRules.get(1).getRuleType(), AssertRuleType.MAX_ROW);
    assertEquals(assertRowRules.get(1).getRuleValue(), 20, DELTA);
  }

  @Test
  public void testParseColumnRules() throws Exception {
    BitSailConfiguration jobConf = BitSailConfiguration.from(new File(
        Paths.get(getClass().getClassLoader().getResource("assert_sink.json").toURI()).toString()));

    Map<String, Map<String, Object>> columnRules = jobConf.get(AssertWriterOptions.COLUMN_RULES);
    assertNotNull(columnRules);
    List<ColumnInfo> columnInfos = jobConf.get(WriterOptions.BaseWriterOptions.COLUMNS);
    assertNotNull(columnInfos);

    Map<String, AssertColumnRule> assertColumnRules = assertRuleParser.parseColumnRules(columnRules, columnInfos);
    assertNotNull(assertColumnRules);
    assertEquals(assertColumnRules.size(), 2);
    assertEquals(assertColumnRules.get(COLUMN_NAME).getColumnName(), "name");
    assertEquals(assertColumnRules.get(COLUMN_PRICE).getColumnType(), new BasicTypeInfo<>(Double.class));

    List<AssertSimpleRule> nameRules = assertColumnRules.get(COLUMN_NAME).getColumnRules();
    assertNotNull(nameRules);
    assertEquals(nameRules.size(), 3);
    assertEquals(nameRules.get(0).getRuleType(), AssertRuleType.NOT_NULL);
    assertNull(nameRules.get(0).getRuleValue());
    assertEquals(nameRules.get(1).getRuleType(), AssertRuleType.MIN_LEN);
    assertEquals(nameRules.get(1).getRuleValue(), 1.0, DELTA);
    assertEquals(nameRules.get(2).getRuleType(), AssertRuleType.MAX_LEN);
    assertEquals(nameRules.get(2).getRuleValue(), 1000.0, DELTA);

    List<AssertSimpleRule> priceRules = assertColumnRules.get(COLUMN_PRICE).getColumnRules();
    assertNotNull(priceRules);
    assertEquals(priceRules.size(), 3);
    assertEquals(priceRules.get(0).getRuleType(), AssertRuleType.MIN);
    assertEquals(priceRules.get(0).getRuleValue(), 2.0, DELTA);
    assertEquals(priceRules.get(1).getRuleType(), AssertRuleType.MAX);
    assertEquals(priceRules.get(1).getRuleValue(), 180, DELTA);
  }
}
