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

package com.bytedance.bitsail.connector.selectdb.typeinfo;

import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.exception.CommonErrorCode;

import java.util.Arrays;
import java.util.List;

public enum SelectdbDataType {
  NULL,
  CHAR,
  VARCHAR,
  TEXT,
  BOOLEAN,
  BINARY,
  VARBINARY,
  DECIMAL,
  DECIMALV2,
  INT,
  TINYINT,
  SMALLINT,
  INTEGER,
  INTERVAL_YEAR_MONTH,
  INTERVAL_DAY_TIME,
  BIGINT,
  LARGEINT,
  FLOAT,
  DOUBLE,
  DATE,
  DATETIME,
  TIMESTAMP_WITHOUT_TIME_ZONE,
  TIMESTAMP_WITH_LOCAL_TIME_ZONE,
  TIMESTAMP_WITH_TIME_ZONE;

  @SuppressWarnings("checkstyle:MagicNumber")
  SelectdbDataType() {
    this.precision = 27;
    this.scale = 9;
  }

  SelectdbDataType(int precision, int scale) {
    this.precision = precision;
    this.scale = scale;
  }

  private static final List<SelectdbDataType> DATA_TYPE_WITH_PRECISION = Arrays.asList(
      DECIMAL,
      DECIMALV2,
      DATETIME,
      TIMESTAMP_WITHOUT_TIME_ZONE,
      TIMESTAMP_WITH_LOCAL_TIME_ZONE,
      TIMESTAMP_WITH_TIME_ZONE);

  private int precision;
  private int scale;

  public int getPrecision() {
    if (!DATA_TYPE_WITH_PRECISION.contains(this)) {
      throw new BitSailException(CommonErrorCode.UNSUPPORTED_COLUMN_TYPE,
          "Current dataType has no precision:" + this.getClass().getName());
    }
    return precision;
  }

  public int getScale() {
    if (!DATA_TYPE_WITH_PRECISION.contains(this)) {
      throw new BitSailException(CommonErrorCode.UNSUPPORTED_COLUMN_TYPE,
          "Current dataType has no scale:" + this.getClass().getName());
    }
    return scale;
  }
}

