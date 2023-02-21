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

package com.bytedance.bitsail.connector.assertion.sink;

import com.bytedance.bitsail.base.connector.writer.v1.Writer;
import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.connector.assertion.sink.error.AssertErrorCode;
import com.bytedance.bitsail.connector.assertion.sink.executor.AssertRuleExecutor;
import com.bytedance.bitsail.connector.assertion.sink.rule.AssertColumnRule;
import com.bytedance.bitsail.connector.assertion.sink.rule.AssertSimpleRule;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class AssertWriter implements Writer<Row, String, Integer> {

  private final List<AssertSimpleRule> rowRules;

  private final Map<String, AssertColumnRule> columnRules;

  private final List<String> columnNames;

  private final AtomicLong atomicLong = new AtomicLong(0);

  private final AssertRuleExecutor assertRuleExecutor = new AssertRuleExecutor();

  public AssertWriter(List<AssertSimpleRule> rowRules, Map<String, AssertColumnRule> columnRules, List<String> columnNames) {
    this.rowRules = rowRules;
    this.columnRules = columnRules;
    this.columnNames = columnNames;
  }

  @Override
  public void write(Row element) throws IOException {
    atomicLong.getAndIncrement();
    if (columnRules != null) {
      AssertColumnRule rule = assertRuleExecutor.check(element, columnRules, columnNames);
      if (rule != null) {
        throw BitSailException.asBitSailException(
            AssertErrorCode.RULE_CHECK_FAILED, "Row : " + element + " check failed rule: " + rule);
      }
    }
  }

  @Override
  public void flush(boolean endOfInput) throws IOException {

  }

  @Override
  public void close() throws IOException {
    if (rowRules != null) {
      rowRules.stream().filter(rowRule -> {
        switch (rowRule.getRuleType()) {
          case MIN_ROW:
            return atomicLong.get() < rowRule.getRuleValue();
          case MAX_ROW:
            return atomicLong.get() > rowRule.getRuleValue();
          default:
            return false;
        }
      })
          .findFirst()
          .ifPresent(failRule -> {
            throw BitSailException.asBitSailException(
                AssertErrorCode.RULE_CHECK_FAILED, "Row Num: " + atomicLong.get() + " check failed rule: " + failRule);
          });
    }
  }

  @Override
  public List<String> prepareCommit() throws IOException {
    return Collections.emptyList();
  }
}
