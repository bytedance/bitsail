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
import com.bytedance.bitsail.common.typeinfo.TypeInfo;
import com.bytedance.bitsail.connector.assertion.sink.rule.AssertColumnRule;
import com.bytedance.bitsail.connector.assertion.sink.rule.AssertSimpleRule;

import java.util.List;
import java.util.Map;

public class AssertRuleExecutor {

  public AssertColumnRule check(Row element, Map<String, AssertColumnRule> columnRules, List<String> columnNames) {
    for (String name : columnRules.keySet()) {
      AssertColumnRule assertColumnRule = columnRules.get(name);
      if (!check(element, assertColumnRule, columnNames)) {
        return assertColumnRule;
      }
    }
    return null;
  }

  private boolean check(Row element, AssertColumnRule columnRule, List<String> columnNames) {
    if (element == null) {
      return false;
    }
    int idx = indexOfName(columnRule.getColumnName(), columnNames);
    if (idx < 0) {
      return false;
    }

    Object value = element.getField(idx);
    if (value == null) {
      return false;
    }

    boolean typeChecked = checkType(value, columnRule.getColumnType());
    if (!typeChecked) {
      return false;
    }

    boolean valueChecked = checkValue(value, columnRule.getColumnRules());
    if (!valueChecked) {
      return false;
    }

    return true;
  }

  private boolean checkValue(Object value, List<AssertSimpleRule> columnRules) {
    for (AssertSimpleRule simpleRule : columnRules) {
      if (!checkSingleRule(value, simpleRule)) {
        return false;
      }
    }
    return true;
  }

  private boolean checkType(Object value, TypeInfo<?> columnType) {
    return value.getClass().equals(columnType.getTypeClass());
  }

  private boolean checkSingleRule(Object value, AssertSimpleRule simpleRule) {
    String str = value == null ? "" : String.valueOf(value);
    switch (simpleRule.getRuleType()) {
      case MIN:
        return value instanceof Number && ((Number) value).doubleValue() >= simpleRule.getRuleValue();
      case MAX:
        return value instanceof Number && ((Number) value).doubleValue() <= simpleRule.getRuleValue();
      case MIN_LEN:
        return str.length() >= simpleRule.getRuleValue();
      case MAX_LEN:
        return str.length() <= simpleRule.getRuleValue();
      case NOT_NULL:
        return value != null;
      default:
        return false;
    }
  }

  private int indexOfName(String columnName, List<String> columnNames) {
    for (int i = 0; i < columnNames.size(); i++) {
      if (columnNames.get(i).equals(columnName)) {
        return i;
      }
    }
    return -1;
  }
}
