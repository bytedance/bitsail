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

import com.bytedance.bitsail.base.connector.writer.v1.Sink;
import com.bytedance.bitsail.base.connector.writer.v1.Writer;
import com.bytedance.bitsail.base.extension.ParallelismComputable;
import com.bytedance.bitsail.base.parallelism.ParallelismAdvice;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.model.ColumnInfo;
import com.bytedance.bitsail.common.option.WriterOptions;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.connector.assertion.sink.option.AssertWriterOptions;
import com.bytedance.bitsail.connector.assertion.sink.rule.AssertColumnRule;
import com.bytedance.bitsail.connector.assertion.sink.rule.AssertRuleParser;
import com.bytedance.bitsail.connector.assertion.sink.rule.AssertSimpleRule;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.bytedance.bitsail.connector.assertion.sink.constants.AssertConstants.DEFAULT_PARALLELISM;

public class AssertSink implements Sink<Row, String, Integer>, ParallelismComputable {
  private static final Logger LOG = LoggerFactory.getLogger(AssertSink.class);

  private List<AssertSimpleRule> rowRules;

  private Map<String, AssertColumnRule> columnRules;

  private List<ColumnInfo> columnInfos;

  private final AssertRuleParser assertRuleParser = new AssertRuleParser();

  @Override
  public String getWriterName() {
    return "Assert";
  }

  @Override
  public void configure(BitSailConfiguration commonConfiguration, BitSailConfiguration writerConfiguration) throws Exception {
    Map<String, Object> rowRulesMap = writerConfiguration.get(AssertWriterOptions.ROW_RULES);
    Map<String, Map<String, Object>> columnRulesMap = writerConfiguration.get(AssertWriterOptions.COLUMN_RULES);
    this.columnInfos = writerConfiguration.get(WriterOptions.BaseWriterOptions.COLUMNS);

    if (rowRulesMap != null) {
      rowRules = assertRuleParser.parseRowRules(rowRulesMap);
    }

    if (columnRulesMap != null) {
      columnRules = assertRuleParser.parseColumnRules(columnRulesMap, columnInfos);
    }
  }

  @Override
  public Writer<Row, String, Integer> createWriter(Writer.Context<Integer> context) throws IOException {
    List<String> columnNames = this.columnInfos.stream().map(ColumnInfo::getName).collect(Collectors.toList());
    return new AssertWriter(rowRules, columnRules, columnNames);
  }

  @Override
  public ParallelismAdvice getParallelismAdvice(BitSailConfiguration commonConf, BitSailConfiguration selfConf, ParallelismAdvice upstreamAdvice) throws Exception {
    int parallelism = upstreamAdvice.getAdviceParallelism();
    if (selfConf.fieldExists(AssertWriterOptions.ROW_RULES)) {
      parallelism = DEFAULT_PARALLELISM;
    } else if (selfConf.fieldExists(WriterOptions.BaseWriterOptions.WRITER_PARALLELISM_NUM) && !upstreamAdvice.isEnforceDownStreamChain()) {
      parallelism = selfConf.get(WriterOptions.BaseWriterOptions.WRITER_PARALLELISM_NUM);
    }
    return ParallelismAdvice.builder()
        .adviceParallelism(parallelism)
        .enforceDownStreamChain(false)
        .build();
  }
}
