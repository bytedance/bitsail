package com.bytedance.bitsail.connector.fake.source.generate;

import com.bytedance.bitsail.common.typeinfo.BasicArrayTypeInfo;
import com.bytedance.bitsail.common.typeinfo.ListTypeInfo;
import com.bytedance.bitsail.common.typeinfo.MapTypeInfo;
import com.bytedance.bitsail.common.typeinfo.TypeInfo;
import com.bytedance.bitsail.common.typeinfo.TypeInfos;
import com.bytedance.bitsail.common.typeinfo.TypeProperty;

import org.apache.commons.collections.CollectionUtils;

public class GeneratorMatcher {

  public static ColumnDataGenerator match(TypeInfo<?> typeInfo, GenerateConfig generateConfig) {
    if (CollectionUtils.isNotEmpty(typeInfo.getTypeProperties()) && typeInfo.getTypeProperties().contains(TypeProperty.UNIQUE)) {
      if (TypeInfos.LONG_TYPE_INFO.getTypeClass() == typeInfo.getTypeClass()) {
        return new SnowflakeId(generateConfig.getTaskId());
      }
      return new AutoIncrementData(typeInfo);
    }

    if (TypeInfos.LONG_TYPE_INFO.getTypeClass() == typeInfo.getTypeClass()) {
      return FakerData.LongFaker;
    } else if (TypeInfos.INT_TYPE_INFO.getTypeClass() == typeInfo.getTypeClass()) {
      return FakerData.IntFaker;
    } else if (TypeInfos.SHORT_TYPE_INFO.getTypeClass() == typeInfo.getTypeClass()) {
      return FakerData.ShortFaker;

    } else if (TypeInfos.STRING_TYPE_INFO.getTypeClass() == typeInfo.getTypeClass()) {
      return FakerData.StringFaker;

    } else if (TypeInfos.BOOLEAN_TYPE_INFO.getTypeClass() == typeInfo.getTypeClass()) {
      return FakerData.BoolFaker;

    } else if (TypeInfos.DOUBLE_TYPE_INFO.getTypeClass() == typeInfo.getTypeClass()) {
      return FakerData.DoubleFaker;

    } else if (TypeInfos.FLOAT_TYPE_INFO.getTypeClass() == typeInfo.getTypeClass()) {
      return FakerData.FloatFaker;

    } else if (TypeInfos.BIG_DECIMAL_TYPE_INFO.getTypeClass() == typeInfo.getTypeClass()) {
      return FakerData.BigDecimalFaker;

    } else if (TypeInfos.BIG_INTEGER_TYPE_INFO.getTypeClass() == typeInfo.getTypeClass()) {
      return FakerData.BigIntegerFaker;

    } else if (BasicArrayTypeInfo.BINARY_TYPE_INFO.getTypeClass() == typeInfo.getTypeClass()) {
      return FakerData.BinaryFaker;

    } else if (TypeInfos.SQL_DATE_TYPE_INFO.getTypeClass() == typeInfo.getTypeClass()) {
      return FakerData.DateFaker;

    } else if (TypeInfos.SQL_TIME_TYPE_INFO.getTypeClass() == typeInfo.getTypeClass()) {
      return FakerData.TimeFaker;

    } else if (TypeInfos.SQL_TIMESTAMP_TYPE_INFO.getTypeClass() == typeInfo.getTypeClass()) {
      return FakerData.TimestampFaker;

    } else if (TypeInfos.LOCAL_DATE_TYPE_INFO.getTypeClass() == typeInfo.getTypeClass()) {
      return FakerData.LocalDateFaker;

    } else if (TypeInfos.LOCAL_TIME_TYPE_INFO.getTypeClass() == typeInfo.getTypeClass()) {
      return FakerData.LocalTimeFaker;

    } else if (TypeInfos.LOCAL_DATE_TIME_TYPE_INFO.getTypeClass() == typeInfo.getTypeClass()) {
      return FakerData.LocalDateTimeFaker;

    } else if (TypeInfos.VOID_TYPE_INFO.getTypeClass() == typeInfo.getTypeClass()) {
      return FakerData.VoidFaker;
    }

    if (typeInfo instanceof ListTypeInfo) {
      return new ListGenerator(match(typeInfo, generateConfig));
    }

    if (typeInfo instanceof MapTypeInfo) {
      return new MapGenerator(match(typeInfo, generateConfig), match(typeInfo, generateConfig));
    }
    throw new RuntimeException("Unsupported type " + typeInfo);
  }
}
