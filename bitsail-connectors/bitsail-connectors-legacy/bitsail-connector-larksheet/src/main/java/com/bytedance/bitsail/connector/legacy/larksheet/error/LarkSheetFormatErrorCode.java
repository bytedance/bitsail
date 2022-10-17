package com.bytedance.bitsail.connector.legacy.larksheet.error;


import com.bytedance.bitsail.common.exception.ErrorCode;

/**
 * @author yangyun
 **/
public enum LarkSheetFormatErrorCode implements ErrorCode {

  REQUIRED_VALUE("LarkSheetPlugin-01",
      "You missed parameter which is required, please check your configuration."),
  INVALID_SHEET_URL("LarkSheetPlugin-02",
      "Invalid sheet url, please check your configuration to verify sheet url."),
  SHEET_NOT_FOUND("LarkSheetPlugin-03",
      "Cannot find your sheet, please verify your lark sheet or configuration."),
  INVALID_SHEET_HEADER("LarkSheetPlugin-04",
      "Invalid sheet header, please check your sheet or field mapping in dorado."),
  INVALID_ROW("LarkSheetPlugin-05",
      "Invalid sheet row, please check your sheet."),
  REQUEST_FORBIDDEN("LarkSheetPlugin-06",
      "Request is forbidden, please make sure the sheet has been authorized to DTS bot."),
  TOKEN_EXPIRED("LarkSheetPlugin-07",
      "User defined token was expired, please make sure the token is active during job running.");

  private final String code;
  private final String description;

  LarkSheetFormatErrorCode(String code, String description) {
    this.code = code;
    this.description = description;
  }

  @Override
  public String getCode() {
    return this.code;
  }

  @Override
  public String getDescription() {
    return this.description;
  }

  @Override
  public String toString() {
    return String.format("Code:[%s], Description:[%s].", this.code,
      this.description);
  }
}

