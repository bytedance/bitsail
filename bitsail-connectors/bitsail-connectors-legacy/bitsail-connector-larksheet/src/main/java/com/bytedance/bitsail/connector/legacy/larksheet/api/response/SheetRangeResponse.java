package com.bytedance.bitsail.connector.legacy.larksheet.api.response;

import com.bytedance.bitsail.connector.legacy.larksheet.meta.ValueRange;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.List;

/**
 * @author yangyun
 **/
@Data
@NoArgsConstructor
@AllArgsConstructor
@ToString(callSuper = true)
public class SheetRangeResponse extends OpenApiBaseResponse {
  private Data data;

  @lombok.Data
  @AllArgsConstructor
  @NoArgsConstructor
  public static class Data {
    /**
     * Revision
     */
    private Integer revision;

    /**
     * Unique identifier of the sheet
     */
    private String spreadsheetToken;

    /**
     * Range value
     */
    private ValueRange valueRange;
  }

  /**
   * @return Data in the range.
   */
  public List<List<Object>> getValues() {
    return data.getValueRange().getValues();
  }
}
