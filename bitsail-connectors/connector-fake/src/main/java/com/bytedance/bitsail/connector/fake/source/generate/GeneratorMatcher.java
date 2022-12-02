package com.bytedance.bitsail.connector.fake.source.generate;

import com.bytedance.bitsail.common.typeinfo.BasicArrayTypeInfo;
import com.bytedance.bitsail.common.typeinfo.ListTypeInfo;
import com.bytedance.bitsail.common.typeinfo.MapTypeInfo;
import com.bytedance.bitsail.common.typeinfo.TypeInfo;
import com.bytedance.bitsail.common.typeinfo.TypeInfos;
import com.bytedance.bitsail.common.typeinfo.TypeProperty;

import org.apache.commons.collections.CollectionUtils;

public class GeneratorMatcher {

  public static ColumnDataGenerator match(TypeInfo<?> typeInfo) {

    if (TypeInfos.LONG_TYPE_INFO.getTypeClass() == typeInfo.getTypeClass()) {
      return FakerGenerator.LongFaker;
    }
    if (TypeInfos.LONG_TYPE_INFO.getTypeClass() == typeInfo.getTypeClass()) {
      if (CollectionUtils.isNotEmpty(typeInfo.getTypeProperties()) && typeInfo.getTypeProperties().contains(TypeProperty.UNIQUE)) {
        return new SnowflakeId();
      } else {
        return FakerGenerator.IntFaker;
      }
    } else if (TypeInfos.INT_TYPE_INFO.getTypeClass() == typeInfo.getTypeClass()) {
      return FakerGenerator.IntFaker;

    } else if (TypeInfos.SHORT_TYPE_INFO.getTypeClass() == typeInfo.getTypeClass()) {
      return FakerGenerator.ShortFaker;

    } else if (TypeInfos.STRING_TYPE_INFO.getTypeClass() == typeInfo.getTypeClass()) {
      return FakerGenerator.StringFaker;

    } else if (TypeInfos.BOOLEAN_TYPE_INFO.getTypeClass() == typeInfo.getTypeClass()) {
      return FakerGenerator.BoolFaker;

    } else if (TypeInfos.DOUBLE_TYPE_INFO.getTypeClass() == typeInfo.getTypeClass()) {
      return FakerGenerator.DoubleFaker;

    } else if (TypeInfos.FLOAT_TYPE_INFO.getTypeClass() == typeInfo.getTypeClass()) {
      return FakerGenerator.FloatFaker;

    } else if (TypeInfos.BIG_DECIMAL_TYPE_INFO.getTypeClass() == typeInfo.getTypeClass()) {
      return FakerGenerator.BigDecimalFaker;

    } else if (TypeInfos.BIG_INTEGER_TYPE_INFO.getTypeClass() == typeInfo.getTypeClass()) {
      return FakerGenerator.BigIntegerFaker;

    } else if (BasicArrayTypeInfo.BINARY_TYPE_INFO.getTypeClass() == typeInfo.getTypeClass()) {
      return FakerGenerator.BinaryFaker;

    } else if (TypeInfos.SQL_DATE_TYPE_INFO.getTypeClass() == typeInfo.getTypeClass()) {
      return FakerGenerator.DateFaker;

    } else if (TypeInfos.SQL_TIME_TYPE_INFO.getTypeClass() == typeInfo.getTypeClass()) {
      return FakerGenerator.TimeFaker;

    } else if (TypeInfos.SQL_TIMESTAMP_TYPE_INFO.getTypeClass() == typeInfo.getTypeClass()) {
      return FakerGenerator.TimestampFaker;

    } else if (TypeInfos.LOCAL_DATE_TYPE_INFO.getTypeClass() == typeInfo.getTypeClass()) {
      return FakerGenerator.LocalDateFaker;

    } else if (TypeInfos.LOCAL_TIME_TYPE_INFO.getTypeClass() == typeInfo.getTypeClass()) {
      return FakerGenerator.LocalTimeFaker;

    } else if (TypeInfos.LOCAL_DATE_TIME_TYPE_INFO.getTypeClass() == typeInfo.getTypeClass()) {
      return FakerGenerator.LocalDateTimeFaker;

    } else if (TypeInfos.VOID_TYPE_INFO.getTypeClass() == typeInfo.getTypeClass()) {
      return FakerGenerator.VoidFaker;
    }

    if (typeInfo instanceof ListTypeInfo) {
      return new ListGenerator(match(typeInfo));
    }

    if (typeInfo instanceof MapTypeInfo) {
      return new MapGenerator(match(typeInfo), match(typeInfo));
    }
    throw new RuntimeException("Unsupported type " + typeInfo);
  }
}
