/*
 * Copyright [2022] [ByteDance]
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

package com.bytedance.bitsail.connector.legacy.jdbc.split;

import com.bytedance.bitsail.common.util.Pair;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

public class TableRangeInfo<K> implements Serializable {
  @Setter
  @Getter
  Pair<K, K> splitKeyRange;
  @Setter
  @Getter
  String quoteTableWithSchema;

  public TableRangeInfo(String quoteTableWithSchema, Pair<K, K> splitKeyRange) {
    this.quoteTableWithSchema = quoteTableWithSchema;
    this.splitKeyRange = splitKeyRange;
  }

  public static <K> TableRangeInfo<K> getEmptyRange() {
    return new TableRangeInfo<>(null, new Pair<>());
  }

  public String toString() {
    return "TableRangeInfo{" +
        "quoteTableWithSchema=" + quoteTableWithSchema +
        ", splitKeyRange='" + splitKeyRange + '\'' +
        '}';
  }
}
