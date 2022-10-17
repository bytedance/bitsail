package com.bytedance.bitsail.connector.legacy.larksheet.api.response;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import static com.bytedance.bitsail.connector.legacy.larksheet.api.SheetConfig.FLOW_CONTROL_CODES;
import static com.bytedance.bitsail.connector.legacy.larksheet.api.SheetConfig.INVALID_ACCESS_TOKEN_CODES;
import static com.bytedance.bitsail.connector.legacy.larksheet.api.SheetConfig.REQUEST_FORBIDDEN;
import static com.bytedance.bitsail.connector.legacy.larksheet.api.SheetConfig.REQUEST_SUCCESS;

/**
 * Ref: <a href="https://open.feishu.cn/document/ukTMukTMukTM/ugjM14COyUjL4ITN">Server Error Codes</a>
 * @author yangyun.
 **/
@Data
@AllArgsConstructor
@NoArgsConstructor
public abstract class OpenApiBaseResponse {

  /**
   * Lark open api error code.
   */
  protected int code;

  /**
   * Error message.
   */
  protected String msg;

  /**
   * Judge if trigger flow control by code.
   */
  public boolean isFlowLimited() {
    return FLOW_CONTROL_CODES.contains(code);
  }

  /**
   * Judge if token is expired by code.
   */
  public boolean isTokenExpired() {
    return INVALID_ACCESS_TOKEN_CODES.contains(code);
  }

  /**
   * Check if the pair '(app_id,app_secret)' has permission to api.
   */
  public boolean isForbidden() {
    return REQUEST_FORBIDDEN == code;
  }

  /**
   * Non-zero code means error.
   */
  public boolean isFailed() {
    return REQUEST_SUCCESS != code;
  }

  /**
   * Zero code means success.
   */
  public boolean isSuccessful() {
    return REQUEST_SUCCESS == code;
  }

}

