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

package com.bytedance.bitsail.connector.larksheet.source.split.strategy;

import com.bytedance.bitsail.connector.larksheet.meta.SheetInfo;
import com.bytedance.bitsail.connector.larksheet.meta.SheetMeta;
import com.bytedance.bitsail.connector.larksheet.source.split.LarkSheetSplit;

import java.util.ArrayList;
import java.util.List;

public class SimpleDivideSplitConstructor {

  /**
   * Number of rows queried in batch.
   */
  private final int batchSize;

  /**
   * Number of lines to skip for each sheet.
   */
  private final List<Integer> skipNums;

  /**
   * the list of sheet information
   */
  private final List<SheetInfo> sheetInfoList;

  public SimpleDivideSplitConstructor(List<SheetInfo> sheetInfoList, int batchSize, List<Integer> skipNums) {
    this.sheetInfoList = sheetInfoList;
    this.batchSize = batchSize;
    this.skipNums = skipNums;
  }

  public List<LarkSheetSplit> construct() {
    int splitCount = 0;
    if (skipNums.isEmpty()) {
      splitCount = sheetInfoList.stream()
          .mapToInt(sheetInfo -> (int) Math.ceil((double) (sheetInfo.getSheetMeta().getRowCount() - 1) / (double) batchSize))
          .sum();
    } else {
      for (int i = 0; i < sheetInfoList.size(); i++) {
        SheetInfo sheetInfo = sheetInfoList.get(i);
        int skipNum = 0;
        if (skipNums.size() > i) {
          skipNum = Math.max(skipNums.get(i), 0);
        }
        splitCount += (int) Math.ceil((double) Math.max(sheetInfo.getSheetMeta().getRowCount() - 1 - skipNum, 0) / (double) batchSize);
      }
    }
    List<LarkSheetSplit> splitList = new ArrayList<>();

    splitCount = 0;
    // generate inputSplits array
    for (int i = 0; i < sheetInfoList.size(); i++) {
      SheetInfo sheetInfo = sheetInfoList.get(i);

      // Get the relevant information of the current sheet Meta and the number of shards
      SheetMeta sheetMeta = sheetInfo.getSheetMeta();
      String sheetToken = sheetInfo.getSheetToken();
      int curCount;
      int skipNum = 0;
      if (skipNums.isEmpty()) {
        curCount = (int) Math.ceil((double) (sheetMeta.getRowCount() - 1) / (double) batchSize);
      } else {
        if (skipNums.size() > i) {
          skipNum = Math.max(skipNums.get(i), 0);
        }
        curCount = (int) Math.ceil((double) Math.max(sheetInfo.getSheetMeta().getRowCount() - 1 - skipNum, 0) / (double) batchSize);
      }

      // Generate the shard of the current sheet Meta
      for (int j = 0; j < curCount; j++) {
        int startRowNumber = 2 + j * batchSize + skipNum;
        int endRowNumber = startRowNumber + batchSize - 1;

        // Initialize shard information
        LarkSheetSplit larkSheetSplit = new LarkSheetSplit(String.valueOf(splitCount));
        larkSheetSplit.setSheetId(sheetMeta.getSheetId());
        larkSheetSplit.setSheetMeta(sheetMeta);
        larkSheetSplit.setSheetToken(sheetToken);
        larkSheetSplit.setStartRowNumber(startRowNumber);
        larkSheetSplit.setEndRowNumber(endRowNumber);
        splitList.add(larkSheetSplit);
        splitCount += 1;
      }
    }
    return splitList;
  }
}
