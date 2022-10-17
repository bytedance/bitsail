package com.bytedance.bitsail.connector.legacy.larksheet.meta;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;

import java.io.Serializable;

@Data
@ToString
@AllArgsConstructor
public class SheetInfo implements Serializable {
  SheetMeta sheetMeta;
  String sheetId;
  String sheetToken;
}