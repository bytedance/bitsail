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

package com.bytedance.bitsail.connector.fake.source.generate;

import com.bytedance.bitsail.common.typeinfo.TypeInfo;
import com.bytedance.bitsail.common.typeinfo.TypeInfos;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.ZoneOffset;

/**
 * auto increment column data generate
 */
public class AutoIncrementData implements ColumnDataGenerator {

  private Increment increment;

  public AutoIncrementData(TypeInfo<?> typeInfo) {
    for (IncrementInstance incrementInstance : IncrementInstance.values()) {
      if (incrementInstance.typeInfo.getTypeClass() == typeInfo.getTypeClass()) {
        increment = incrementInstance;
        break;
      }
    }
    if (increment == null) {
      throw new IllegalArgumentException("not supported the type:" + typeInfo);
    }
  }

  @Override
  public Object generate(GenerateConfig generateConfig) {
    return increment.next(generateConfig, generateConfig.getRowId().get());
  }

  private interface Increment {
    Object next(GenerateConfig config, Long rowId);

  }

  private enum IncrementInstance implements Increment {
    TO_LONG(TypeInfos.LONG_TYPE_INFO) {
      @Override
      public Object next(GenerateConfig config, Long rowId) {
        return rowId;
      }

    },
    TO_INTEGER(TypeInfos.INT_TYPE_INFO) {
      @Override
      public Object next(GenerateConfig config, Long rowId) {
        return rowId.intValue();
      }

    },
    TO_SHORT(TypeInfos.SHORT_TYPE_INFO) {
      @Override
      public Object next(GenerateConfig config, Long rowId) {
        return rowId.shortValue();
      }
    },
    TO_FLOAT(TypeInfos.FLOAT_TYPE_INFO) {
      @Override
      public Object next(GenerateConfig config, Long rowId) {
        return rowId.floatValue();
      }
    },
    TO_DOUBLE(TypeInfos.DOUBLE_TYPE_INFO) {
      @Override
      public Object next(GenerateConfig config, Long rowId) {
        return rowId.doubleValue();
      }
    },
    TO_BIG_INTEGER(TypeInfos.BIG_INTEGER_TYPE_INFO) {
      @Override
      public Object next(GenerateConfig config, Long rowId) {
        return BigInteger.valueOf(rowId);
      }
    },
    TO_BIG_DECIMAL(TypeInfos.BIG_DECIMAL_TYPE_INFO) {
      @Override
      public Object next(GenerateConfig config, Long rowId) {
        return BigDecimal.valueOf(rowId);
      }
    },
    TO_TIMESTAMP(TypeInfos.SQL_TIMESTAMP_TYPE_INFO) {
      @Override
      public Object next(GenerateConfig config, Long rowId) {
        return Timestamp.from(config.getFromTimestamp()
            .toLocalDateTime()
            .plusNanos(rowId)
            .toInstant((ZoneOffset) config.getZoneId()));
      }
    },
    TO_DATE(TypeInfos.SQL_DATE_TYPE_INFO) {
      @Override
      public Object next(GenerateConfig config, Long rowId) {
        return Date.from(config.getFromTimestamp()
            .toLocalDateTime()
            .plusDays(rowId)
            .toInstant((ZoneOffset) config.getZoneId()));
      }

    },
    TO_TIME(TypeInfos.SQL_TIME_TYPE_INFO) {
      @Override
      public Object next(GenerateConfig config, Long rowId) {
        return Time.from(config.getFromTimestamp()
            .toLocalDateTime()
            .plusSeconds(rowId)
            .toInstant((ZoneOffset) config.getZoneId()));
      }
    },
    TO_LOCAL_DATE_TIME(TypeInfos.LOCAL_DATE_TIME_TYPE_INFO) {
      @Override
      public Object next(GenerateConfig config, Long rowId) {
        return config.getFromTimestamp().toLocalDateTime().plusNanos(rowId);
      }

    },
    TO_LOCAL_DATE(TypeInfos.LOCAL_DATE_TYPE_INFO) {
      @Override
      public Object next(GenerateConfig config, Long rowId) {
        return config.getFromTimestamp().toLocalDateTime().toLocalDate().plusDays(rowId);
      }

    },
    TO_LOCAL_TIME(TypeInfos.LOCAL_TIME_TYPE_INFO) {
      @Override
      public Object next(GenerateConfig config, Long rowId) {
        return config.getFromTimestamp().toLocalDateTime().toLocalTime().plusSeconds(rowId);
      }
    },
    TO_STRING(TypeInfos.STRING_TYPE_INFO) {
      @Override
      public Object next(GenerateConfig config, Long rowId) {
        return rowId.toString();
      }
    };

    @Override
    public Object next(GenerateConfig config, Long rowId) {
      return config;
    }

    protected final TypeInfo<?> typeInfo;

    IncrementInstance(TypeInfo<?> typeInfo) {
      this.typeInfo = typeInfo;
    }
  }
}
