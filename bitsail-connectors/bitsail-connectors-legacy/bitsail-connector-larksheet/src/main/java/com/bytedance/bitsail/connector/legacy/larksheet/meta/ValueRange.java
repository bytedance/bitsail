package com.bytedance.bitsail.connector.legacy.larksheet.meta;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * @author yangyun
 **/
@Data
@AllArgsConstructor
@NoArgsConstructor
public class ValueRange {

  private String majorDimension;

  /**
   * Value range.<br/>
   * <i>e.g.</i> sheetId!A20:F20.
   */
  private String range;

  /**
   * Revision.
   */
  private int revision;

  /**
   * Two-dimensional array.
   * From left to right and top to bottom, each element represents a data in a cell.
   */
  private List<List<Object>> values;
}
