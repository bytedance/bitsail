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

import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.model.ColumnInfo;
import com.bytedance.bitsail.common.typeinfo.TypeInfo;
import com.bytedance.bitsail.common.typeinfo.TypeInfoBridge;
import com.bytedance.bitsail.connector.assertion.sink.constants.AssertRuleType;
import com.bytedance.bitsail.connector.assertion.sink.error.AssertErrorCode;

import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AssertRuleParser implements Serializable {
  public List<AssertSimpleRule> parseRowRules(Map<String, Object> rowRulesMap) {
    return parseSimpleRules(rowRulesMap);
  }

  public Map<String, AssertColumnRule> parseColumnRules(Map<String, Map<String, Object>> columnRulesMap, List<ColumnInfo> columnInfos) {
    Map<String, AssertColumnRule> columnSimpleRuleMap = new HashMap<>();
    for (String columnName : columnRulesMap.keySet()) {
      AssertColumnRule columnRule = new AssertColumnRule();
      columnRule.setColumnName(columnName);
      TypeInfo<?> type = findColumnType(columnName, columnInfos);
      columnRule.setColumnType(type);
      Map<String, Object> assertSimpleRules = columnRulesMap.get(columnName);
      List<AssertSimpleRule> ruleList = parseSimpleRules(assertSimpleRules);
      columnRule.setColumnRules(ruleList);
      columnSimpleRuleMap.put(columnName, columnRule);
    }
    return columnSimpleRuleMap;
  }

  private TypeInfo<?> findColumnType(String columnName, List<ColumnInfo> columnInfos) {
    for (ColumnInfo columnInfo : columnInfos) {
      if (columnName.equals(columnInfo.getName())) {
        return TypeInfoBridge.bridgeTypeInfo(StringUtils.upperCase(columnInfo.getType()));
      }
    }
    return null;
  }

  private List<AssertSimpleRule> parseSimpleRules(Map<String, Object> simpleRulesMap) {
    List<AssertSimpleRule> rules = new ArrayList<>();
    for (String key : simpleRulesMap.keySet()) {
      String upperKey = key.toUpperCase();
      AssertSimpleRule assertSimpleRule = new AssertSimpleRule();
      switch (AssertRuleType.valueOf(upperKey)) {
        case MIN_ROW:
          assertSimpleRule.setRuleType(AssertRuleType.MIN_ROW);
          assertSimpleRule.setRuleValue(asDouble(simpleRulesMap.get(key)));
          break;
        case MAX_ROW:
          assertSimpleRule.setRuleType(AssertRuleType.MAX_ROW);
          assertSimpleRule.setRuleValue(asDouble(simpleRulesMap.get(key)));
          break;
        case MIN:
          assertSimpleRule.setRuleType(AssertRuleType.MIN);
          assertSimpleRule.setRuleValue(asDouble(simpleRulesMap.get(key)));
          break;
        case MAX:
          assertSimpleRule.setRuleType(AssertRuleType.MAX);
          assertSimpleRule.setRuleValue(asDouble(simpleRulesMap.get(key)));
          break;
        case MIN_LEN:
          assertSimpleRule.setRuleType(AssertRuleType.MIN_LEN);
          assertSimpleRule.setRuleValue(asDouble(simpleRulesMap.get(key)));
          break;
        case MAX_LEN:
          assertSimpleRule.setRuleType(AssertRuleType.MAX_LEN);
          assertSimpleRule.setRuleValue(asDouble(simpleRulesMap.get(key)));
          break;
        case NOT_NULL:
          if ((Boolean) simpleRulesMap.get(key)) {
            assertSimpleRule.setRuleType(AssertRuleType.NOT_NULL);
          }
          break;
        default:
          throw BitSailException.asBitSailException(
              AssertErrorCode.RULE_NOT_SUPPORT, "Rule: " + upperKey + " is not include now.");
      }
      if (assertSimpleRule.getRuleType() != null) {
        rules.add(assertSimpleRule);
      }
    }
    return rules;
  }

  private Double asDouble(Object o) {
    Double val = null;
    if (o instanceof Number) {
      val = ((Number) o).doubleValue();
    } else {
      throw BitSailException.asBitSailException(
          AssertErrorCode.DATATYPE_CAST_ERROR, "Data: " + o + " is not a number.");
    }
    return val;
  }
}
