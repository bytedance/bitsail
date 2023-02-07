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

package com.bytedance.bitsail.connector.assertion.sink.executor;

import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.common.typeinfo.BasicTypeInfo;
import com.bytedance.bitsail.connector.assertion.sink.constants.AssertRuleType;
import com.bytedance.bitsail.connector.assertion.sink.rule.AssertColumnRule;
import com.bytedance.bitsail.connector.assertion.sink.rule.AssertSimpleRule;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class AssertRuleExecutorTest {

  private final AssertRuleExecutor assertRuleExecutor = new AssertRuleExecutor();

  private static final String COLUMN_NAME = "name";
  private static final String COLUMN_PRICE = "price";

  private final Row row = new Row(new Object[]{"book", 72.0});

  @Test
  public void testCheckValue() {
    AssertColumnRule rule = commonCheck(3.0, 5.0, 50.0, 100.0);
    assertNull(rule);
  }

  @Test
  public void testCheckValueMinLenFail() {
    AssertColumnRule rule = commonCheck(10.0, 20.0, 50.0, 100.0);
    assertNotNull(rule);
  }

  @Test
  public void testCheckValueMaxLenFail() {
    AssertColumnRule rule = commonCheck(1.0, 2.0, 50.0, 100.0);
    assertNotNull(rule);
  }

  @Test
  public void testCheckValueMinFail() {
    AssertColumnRule rule = commonCheck(3.0, 5.0, 80.0, 100.0);
    assertNotNull(rule);
  }

  @Test
  public void testCheckValueMaxFail() {
    AssertColumnRule rule = commonCheck(3.0, 5.0, 50.0, 70.0);
    assertNotNull(rule);
  }

  @Test
  public void testCheckTypeFail() {
    AssertColumnRule colRule = new AssertColumnRule();
    colRule.setColumnName("name");
    colRule.setColumnType(new BasicTypeInfo<>(Integer.class));

    Map<String, AssertColumnRule> map = new HashMap<>();
    map.put(COLUMN_NAME, colRule);
    AssertColumnRule typeCheckRule = assertRuleExecutor.check(row, map, Arrays.asList(COLUMN_NAME));
    assertNotNull(typeCheckRule);
  }

  private AssertColumnRule commonCheck(double minLength, double maxLength, double min, double max) {
    AssertColumnRule nameColumnRule = setColumnRuleForName(minLength, maxLength);
    AssertColumnRule priceColumnRule = setColumnRuleForPrice(min, max);
    Map<String, AssertColumnRule> map = new HashMap<>();
    map.put(COLUMN_NAME, nameColumnRule);
    map.put(COLUMN_PRICE, priceColumnRule);
    List<String> columnNames = Arrays.asList(COLUMN_NAME, COLUMN_PRICE);
    AssertColumnRule valCheck = assertRuleExecutor.check(row, map, columnNames);
    return valCheck;
  }

  private AssertColumnRule setColumnRuleForName(double minLength, double maxLength) {
    AssertColumnRule rule = new AssertColumnRule();
    rule.setColumnName(COLUMN_NAME);
    rule.setColumnType(new BasicTypeInfo<>(String.class));
    List<AssertSimpleRule> simpleRules = new ArrayList<>();

    AssertSimpleRule columnRule1 = new AssertSimpleRule();
    columnRule1.setRuleType(AssertRuleType.NOT_NULL);
    AssertSimpleRule columnRule2 = new AssertSimpleRule();
    columnRule2.setRuleType(AssertRuleType.MIN_LEN);
    columnRule2.setRuleValue(minLength);
    AssertSimpleRule columnRule3 = new AssertSimpleRule();
    columnRule3.setRuleType(AssertRuleType.MAX_LEN);
    columnRule3.setRuleValue(maxLength);

    simpleRules.add(columnRule1);
    simpleRules.add(columnRule2);
    simpleRules.add(columnRule3);
    rule.setColumnRules(simpleRules);
    return rule;
  }

  private AssertColumnRule setColumnRuleForPrice(double min, double max) {
    AssertColumnRule rule = new AssertColumnRule();
    rule.setColumnName(COLUMN_PRICE);
    rule.setColumnType(new BasicTypeInfo<>(Double.class));
    List<AssertSimpleRule> simpleRules = new ArrayList<>();

    AssertSimpleRule columnRule1 = new AssertSimpleRule();
    columnRule1.setRuleType(AssertRuleType.NOT_NULL);
    AssertSimpleRule columnRule2 = new AssertSimpleRule();
    columnRule2.setRuleType(AssertRuleType.MIN);
    columnRule2.setRuleValue(min);
    AssertSimpleRule columnRule3 = new AssertSimpleRule();
    columnRule3.setRuleType(AssertRuleType.MAX);
    columnRule3.setRuleValue(max);

    simpleRules.add(columnRule1);
    simpleRules.add(columnRule2);
    simpleRules.add(columnRule3);
    rule.setColumnRules(simpleRules);
    return rule;
  }
}
