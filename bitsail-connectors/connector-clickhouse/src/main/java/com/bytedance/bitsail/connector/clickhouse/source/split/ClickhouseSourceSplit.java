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

package com.bytedance.bitsail.connector.clickhouse.source.split;

import com.bytedance.bitsail.base.connector.reader.v1.SourceSplit;
import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.exception.CommonErrorCode;

import lombok.Setter;
import org.apache.commons.lang3.StringUtils;

import java.sql.PreparedStatement;
import java.sql.SQLException;

@Setter
public class ClickhouseSourceSplit implements SourceSplit {
  public static final String SOURCE_SPLIT_PREFIX = "clickhouse_source_split_";
  private static final String BETWEEN_CLAUSE = "( `%s` BETWEEN ? AND ? )";

  private final String splitId;

  /**
   * Read whole table or range [lower, upper]
   */
  private boolean readTable;
  private Long lower;
  private Long upper;

  public ClickhouseSourceSplit(int splitId) {
    this.splitId = SOURCE_SPLIT_PREFIX + splitId;
  }

  @Override
  public String uniqSplitId() {
    return splitId;
  }

  public void decorateStatement(PreparedStatement statement) {
    try {
      if (readTable) {
        lower = Long.MIN_VALUE;
        upper = Long.MAX_VALUE;
      }
      statement.setObject(1, lower);
      statement.setObject(2, upper);
    } catch (SQLException e) {
      throw BitSailException.asBitSailException(CommonErrorCode.RUNTIME_ERROR, "Failed to decorate statement with split " + this, e.getCause());
    }
  }

  public static String getRangeClause(String splitField) {
    return StringUtils.isEmpty(splitField) ? null : String.format(BETWEEN_CLAUSE, splitField);
  }

  @Override
  public String toString() {
    return String.format(
        "{\"split_id\":\"%s\", \"lower\":%s, \"upper\":%s, \"readTable\":%s}",
        splitId, lower, upper, readTable);
  }
}
