package com.bytedance.bitsail.connector.legacy.larksheet.source;

import com.bytedance.bitsail.connector.legacy.larksheet.meta.SheetMeta;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;
import org.apache.flink.core.io.InputSplit;

/**
 * @author yangyun
 */
@Data
@ToString
@AllArgsConstructor
public class LarkSheetInputSplit implements InputSplit {
  SheetMeta sheetMeta;
  String sheetToken;
  String sheetId;
  int startRowNumber;
  int endRowNumber;

  @Override
  public int getSplitNumber() {
    return 0;
  }
}
