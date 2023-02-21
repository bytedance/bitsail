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

package com.bytedance.bitsail.connector.larksheet.source.split;

import com.bytedance.bitsail.base.connector.reader.v1.SourceSplit;
import com.bytedance.bitsail.connector.larksheet.meta.SheetMeta;

import lombok.Getter;
import lombok.Setter;

import static com.bytedance.bitsail.connector.larksheet.constant.LarkSheetConstant.SOURCE_SPLIT_PREFIX;

@Setter
@Getter
public class LarkSheetSplit implements SourceSplit {

  private String splitId;

  private SheetMeta sheetMeta;

  private String sheetToken;

  private String sheetId;

  private int startRowNumber;

  private int endRowNumber;

  public LarkSheetSplit(String splitId) {
    this.splitId = splitId;
  }

  @Override
  public String uniqSplitId() {
    return SOURCE_SPLIT_PREFIX + splitId;
  }

  @Override
  public String toString() {
    return "LarkSheetSplit{" +
        "splitId='" + splitId + '\'' +
        ", sheetMeta=" + sheetMeta +
        ", sheetToken='" + sheetToken + '\'' +
        ", sheetId='" + sheetId + '\'' +
        ", startRowNumber=" + startRowNumber +
        ", endRowNumber=" + endRowNumber +
        '}';
  }
}
