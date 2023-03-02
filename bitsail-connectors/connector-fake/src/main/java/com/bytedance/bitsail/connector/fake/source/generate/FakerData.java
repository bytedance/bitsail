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

import com.bytedance.bitsail.common.typeinfo.BasicArrayTypeInfo;
import com.bytedance.bitsail.common.typeinfo.TypeInfo;
import com.bytedance.bitsail.common.typeinfo.TypeInfos;

import net.datafaker.Faker;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;

/**
 * generate data by net.datafaker:datafaker
 */
public class FakerData implements ColumnDataGenerator {

  public static boolean supported(TypeInfo<?> typeInfo) {
    return Arrays.stream(FakeInstance.values())
        .anyMatch(fakeInstance -> fakeInstance.typeInfo.equals(typeInfo));
  }

  private FakeData fakeData;

  public FakerData(TypeInfo<?> typeInfo) {
    for (FakeInstance fakeInstance : FakeInstance.values()) {
      if (typeInfo.equals(fakeInstance.typeInfo)) {
        this.fakeData = fakeInstance;
      }
    }

    if (fakeData == null) {
      throw new IllegalArgumentException("not supported the type:" + typeInfo);
    }
  }

  @Override
  public Object generate(GenerateContext generateContext) {
    return fakeData.fake(generateContext);
  }

  private interface FakeData {
    Object fake(GenerateContext generateContext);
  }

  @SuppressWarnings("checkstyle:MagicNumber")
  private enum FakeInstance implements FakeData {
    LongFaker(TypeInfos.LONG_TYPE_INFO) {
      @Override
      public Object fake(GenerateContext generateContext) {
        return faker.number().randomNumber();
      }
    },
    IntFaker(TypeInfos.INT_TYPE_INFO) {
      @Override
      public Object fake(GenerateContext generateContext) {
        return Long.valueOf(faker.number().randomNumber()).intValue();
      }
    },
    ShortFaker(TypeInfos.SHORT_TYPE_INFO) {
      @Override
      public Object fake(GenerateContext generateContext) {
        return Long.valueOf(faker.number().randomNumber()).shortValue();
      }
    },
    StringFaker(TypeInfos.STRING_TYPE_INFO) {
      @Override
      public Object fake(GenerateContext generateContext) {
        return faker.name().fullName();
      }
    },
    BoolFaker(TypeInfos.BOOLEAN_TYPE_INFO) {
      @Override
      public Object fake(GenerateContext generateContext) {
        return faker.bool().bool();
      }
    },
    DoubleFaker(TypeInfos.DOUBLE_TYPE_INFO) {
      @Override
      public Object fake(GenerateContext config) {
        return faker.number().randomDouble(5, config.getLower(), config.getUpper());
      }
    },
    FloatFaker(TypeInfos.FLOAT_TYPE_INFO) {
      @Override
      public Object fake(GenerateContext config) {
        return Double.valueOf(faker.number().randomDouble(5, config.getLower(), config.getUpper())).floatValue();
      }
    },
    BigDecimalFaker(TypeInfos.BIG_DECIMAL_TYPE_INFO) {
      @Override
      public Object fake(GenerateContext config) {
        return BigDecimal.valueOf(faker.number().randomDouble(5, config.getLower(), config.getUpper()));
      }
    },
    BigIntegerFaker(TypeInfos.BIG_INTEGER_TYPE_INFO) {
      @Override
      public Object fake(GenerateContext generateContext) {
        return new BigInteger(String.valueOf(faker.number().randomNumber()));
      }
    },
    BinaryFaker(BasicArrayTypeInfo.BINARY_TYPE_INFO) {
      @Override
      public Object fake(GenerateContext generateContext) {
        return faker.name().fullName().getBytes();
      }
    },
    DateFaker(TypeInfos.SQL_DATE_TYPE_INFO) {
      @Override
      public Object fake(GenerateContext config) {
        return new Date(faker.date().between(config.getFromTimestamp(), config.getToTimestamp()).getTime());
      }
    },
    TimeFaker(TypeInfos.SQL_TIME_TYPE_INFO) {
      @Override
      public Object fake(GenerateContext config) {
        return new Time(faker.date().between(config.getFromTimestamp(), config.getToTimestamp()).getTime());
      }
    },
    TimestampFaker(TypeInfos.SQL_TIMESTAMP_TYPE_INFO) {
      @Override
      public Object fake(GenerateContext config) {
        return new Timestamp(faker.date().between(config.getFromTimestamp(), config.getToTimestamp()).getTime());
      }
    },
    LocalDateFaker(TypeInfos.LOCAL_DATE_TYPE_INFO) {
      @Override
      public Object fake(GenerateContext generateContext) {
        return faker.date().between(generateContext.getFromTimestamp(), generateContext.getToTimestamp()).toLocalDateTime().toLocalDate();
      }
    },
    LocalTimeFaker(TypeInfos.LOCAL_TIME_TYPE_INFO) {
      @Override
      public Object fake(GenerateContext generateContext) {
        return faker.date().between(generateContext.getFromTimestamp(), generateContext.getToTimestamp()).toLocalDateTime().toLocalTime();
      }
    },
    LocalDateTimeFaker(TypeInfos.LOCAL_DATE_TIME_TYPE_INFO) {
      @Override
      public Object fake(GenerateContext generateContext) {
        return faker.date().between(generateContext.getFromTimestamp(), generateContext.getToTimestamp()).toLocalDateTime();
      }
    },
    VoidFaker(TypeInfos.VOID_TYPE_INFO) {
      @Override
      public Object fake(GenerateContext generateContext) {
        return null;
      }
    };

    protected final transient Faker faker;

    protected final TypeInfo<?> typeInfo;

    FakeInstance(TypeInfo<?> typeInfo) {
      this.typeInfo = typeInfo;
      this.faker = new Faker();
    }

    @Override
    public abstract Object fake(GenerateContext generateContext);
  }

}
