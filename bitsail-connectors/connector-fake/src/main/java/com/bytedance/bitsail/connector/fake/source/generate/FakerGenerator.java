/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package com.bytedance.bitsail.connector.fake.source.generate;

import net.datafaker.Faker;

import java.math.BigInteger;
import java.sql.Time;
import java.sql.Timestamp;

/**
 * generate data by net.datafaker:datafaker
 */
public enum FakerGenerator implements ColumnDataGenerator {

  LongFaker {
    @Override
    public Object generate(ColumnConfig columnConfig) {
      return faker.number().randomNumber();
    }
  },
  IntFaker {
    @Override
    public Object generate(ColumnConfig columnConfig) {
      return Long.valueOf(faker.number().randomNumber())
          .intValue();
    }
  },
  ShortFaker {
    @Override
    public Object generate(ColumnConfig columnConfig) {
      return Long.valueOf(faker.number().randomNumber())
          .shortValue();
    }
  },
  StringFaker {
    @Override
    public Object generate(ColumnConfig columnConfig) {
      return faker.name().fullName();
    }
  },
  BoolFaker {
    @Override
    public Object generate(ColumnConfig columnConfig) {
      return faker.bool().bool();
    }
  },
  DoubleFaker {
    @Override
    public Object generate(ColumnConfig config) {
      return faker.number().randomDouble(5, config.getLower(), config.getUpper());
    }
  },
  FloatFaker {
    @Override
    public Object generate(ColumnConfig config) {
      return Double.valueOf(faker.number().randomDouble(5, config.getLower(), config.getUpper()))
          .floatValue();
    }
  },
  BigDecimalFaker {
    @Override
    public Object generate(ColumnConfig config) {
      return java.math.BigDecimal.valueOf(faker.number().randomDouble(5, config.getLower(), config.getUpper()));
    }
  },
  BigIntegerFaker {
    @Override
    public Object generate(ColumnConfig columnConfig) {
      return new BigInteger(String.valueOf(faker.number().randomNumber()));
    }
  },
  BinaryFaker {
    @Override
    public Object generate(ColumnConfig columnConfig) {
      return faker.name().fullName().getBytes();
    }
  },
  DateFaker {
    @Override
    public Object generate(ColumnConfig config) {
      return new java.sql.Date(
          faker.date()
              .between(config.getFromTimestamp(), config.getToTimestamp())
              .getTime());
    }
  },
  TimeFaker {
    @Override
    public Object generate(ColumnConfig config) {
      return new Time(
          faker.date()
              .between(config.getFromTimestamp(), config.getToTimestamp())
              .getTime());
    }
  },
  TimestampFaker {
    @Override
    public Object generate(ColumnConfig config) {
      return new Timestamp(
          faker.date()
              .between(config.getFromTimestamp(), config.getToTimestamp())
              .getTime());
    }
  },
  LocalDateFaker {
    @Override
    public Object generate(ColumnConfig columnConfig) {
      return faker.date()
          .between(columnConfig.getFromTimestamp(), columnConfig.getToTimestamp())
          .toLocalDateTime()
          .toLocalDate();
    }
  },
  LocalTimeFaker {
    @Override
    public Object generate(ColumnConfig columnConfig) {
      return faker.date()
          .between(columnConfig.getFromTimestamp(), columnConfig.getToTimestamp())
          .toLocalDateTime()
          .toLocalTime();
    }
  },
  LocalDateTimeFaker {
    @Override
    public Object generate(ColumnConfig columnConfig) {
      return faker.date()
          .between(columnConfig.getFromTimestamp(), columnConfig.getToTimestamp())
          .toLocalDateTime();
    }
  },
  VoidFaker{
    @Override
    public Object generate(ColumnConfig columnConfig) {
      return null;
    }
  }
  ;
  protected final transient Faker faker;

  FakerGenerator() {
    this.faker = new Faker();
  }


  @Override
  public abstract Object generate(ColumnConfig columnConfig);

}
