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

package com.bytedance.bitsail.connector.larksheet.api.response;

import com.bytedance.bitsail.connector.larksheet.meta.ValueRange;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.List;

@lombok.Data
@NoArgsConstructor
@AllArgsConstructor
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
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
