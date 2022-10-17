package com.bytedance.bitsail.connector.legacy.larksheet.meta;


import com.bytedance.bitsail.connector.legacy.larksheet.util.LarkSheetUtil;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Meta information of a sheet.
 *
 * @author yangyun
 **/
@Data
@AllArgsConstructor
@NoArgsConstructor
public class SheetMeta implements Serializable {
  private static final long serialVersionUID = 1800860169827483423L;
  /**
   * Index of sheet.
   */
  private int index;

  /**
   * Number of effective columns in the sheet.
   */
  private int columnCount;

  /**
   * Number of rows in the sheet. The open api has the problem of line expansion and needs to filter empty lines
   */
  private int rowCount;

  /**
   * Id of sheet.
   */
  private String sheetId;

  /**
   * Title of sheet.
   */
  private String title;

  /**
   * Cell merge information of the sheet.
   */
  private Object merges;


  /**
   * Get the maximum range of the first row.
   *
   * @return Top left cell index -> top right cell index.
   */
  public String getMaxHeaderRange() {
    return String.format("A1:%s1", LarkSheetUtil.numberToSequence(columnCount));
  }
}