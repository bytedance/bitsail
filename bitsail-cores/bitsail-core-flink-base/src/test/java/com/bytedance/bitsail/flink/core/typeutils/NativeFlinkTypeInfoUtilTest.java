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

package com.bytedance.bitsail.flink.core.typeutils;

import com.bytedance.bitsail.common.model.ColumnInfo;
import com.bytedance.bitsail.common.typeinfo.BasicTypeInfo;
import com.bytedance.bitsail.common.typeinfo.ListTypeInfo;
import com.bytedance.bitsail.common.typeinfo.MapTypeInfo;
import com.bytedance.bitsail.common.typeinfo.TypeInfo;

import com.google.common.collect.ImmutableList;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class NativeFlinkTypeInfoUtilTest {

  @Test
  public void testToTypeInfo() {
    TypeInformation<?> longTypeInfo = org.apache.flink.api.common.typeinfo.BasicTypeInfo.LONG_TYPE_INFO;
    TypeInformation<?> listTypeInfo = new org.apache.flink.api.java.typeutils.ListTypeInfo<>(
        org.apache.flink.api.common.typeinfo.BasicTypeInfo.STRING_TYPE_INFO);
    TypeInformation<?> mapTypeInfo = new org.apache.flink.api.java.typeutils.MapTypeInfo<>(
        org.apache.flink.api.common.typeinfo.BasicTypeInfo.STRING_TYPE_INFO,
        org.apache.flink.api.common.typeinfo.BasicTypeInfo.DOUBLE_TYPE_INFO);
    RowTypeInfo rowTypeInfo = new RowTypeInfo(
        longTypeInfo,
        listTypeInfo,
        mapTypeInfo
    );

    com.bytedance.bitsail.common.typeinfo.RowTypeInfo frameworkRowTypeInfo = NativeFlinkTypeInfoUtil.toRowTypeInfo(rowTypeInfo);
    TypeInfo<?> bitsailLong = frameworkRowTypeInfo.getTypeInfos()[0];
    TypeInfo<?> bitsailList = frameworkRowTypeInfo.getTypeInfos()[1];
    TypeInfo<?> bitsailMap = frameworkRowTypeInfo.getTypeInfos()[2];

    Assert.assertTrue(bitsailLong instanceof BasicTypeInfo);
    Assert.assertSame(Long.class, bitsailLong.getTypeClass());

    Assert.assertTrue(bitsailList instanceof ListTypeInfo);
    Assert.assertSame(String.class, ((ListTypeInfo<?>) bitsailList).getElementTypeInfo().getTypeClass());

    Assert.assertTrue(bitsailMap instanceof MapTypeInfo);
    Assert.assertSame(String.class, ((MapTypeInfo<?, ?>) bitsailMap).getKeyTypeInfo().getTypeClass());
    Assert.assertSame(Double.class, ((MapTypeInfo<?, ?>) bitsailMap).getValueTypeInfo().getTypeClass());
  }

  @Test
  public void testGetRowTypeInformation() {
    com.bytedance.bitsail.common.typeinfo.RowTypeInfo rowTypeInfo = new com.bytedance.bitsail.common.typeinfo.RowTypeInfo(
        new String[] {"id", "labels", "properties"},
        new TypeInfo<?>[] {
            new BasicTypeInfo<>(Long.class),
            new ListTypeInfo<>(new BasicTypeInfo<>(String.class)),
            new MapTypeInfo<>(new BasicTypeInfo<>(String.class), new BasicTypeInfo<>(Double.class))
        });
    RowTypeInfo rowTypeInformation = (RowTypeInfo) NativeFlinkTypeInfoUtil.getRowTypeInformation(rowTypeInfo);

    TypeInformation<?> longTypeInfo = rowTypeInformation.getTypeAt(0);
    TypeInformation<?> listTypeInfo = rowTypeInformation.getTypeAt(1);
    TypeInformation<?> mapTypeInfo = rowTypeInformation.getTypeAt(2);

    Assert.assertEquals(org.apache.flink.api.common.typeinfo.BasicTypeInfo.LONG_TYPE_INFO, longTypeInfo);

    Assert.assertTrue(listTypeInfo instanceof org.apache.flink.api.java.typeutils.ListTypeInfo);
    Assert.assertEquals(
        org.apache.flink.api.common.typeinfo.BasicTypeInfo.STRING_TYPE_INFO,
        ((org.apache.flink.api.java.typeutils.ListTypeInfo<?>) listTypeInfo).getElementTypeInfo()
    );

    Assert.assertTrue(mapTypeInfo instanceof org.apache.flink.api.java.typeutils.MapTypeInfo);
    Assert.assertEquals(
        org.apache.flink.api.common.typeinfo.BasicTypeInfo.STRING_TYPE_INFO,
        ((org.apache.flink.api.java.typeutils.MapTypeInfo<?, ?>) mapTypeInfo).getKeyTypeInfo()
    );
    Assert.assertEquals(
        org.apache.flink.api.common.typeinfo.BasicTypeInfo.DOUBLE_TYPE_INFO,
        ((org.apache.flink.api.java.typeutils.MapTypeInfo<?, ?>) mapTypeInfo).getValueTypeInfo()
    );
  }

  @Test
  public void testGetRowTypeInfoFromColumnInfos() {
    List<ColumnInfo> columnInfos = ImmutableList.of(
        new ColumnInfo("type_long", "long"),
        new ColumnInfo("type_list", "list<string>"),
        new ColumnInfo("type_map", "map<string,double>")
    );
    RowTypeInfo rowTypeInfo = NativeFlinkTypeInfoUtil.getRowTypeInformation(columnInfos);

    TypeInformation<?> longTypeInfo = rowTypeInfo.getTypeAt(0);
    TypeInformation<?> listTypeInfo = rowTypeInfo.getTypeAt(1);
    TypeInformation<?> mapTypeInfo = rowTypeInfo.getTypeAt(2);

    Assert.assertEquals(org.apache.flink.api.common.typeinfo.BasicTypeInfo.LONG_TYPE_INFO, longTypeInfo);

    Assert.assertTrue(listTypeInfo instanceof org.apache.flink.api.java.typeutils.ListTypeInfo);
    Assert.assertEquals(
        org.apache.flink.api.common.typeinfo.BasicTypeInfo.STRING_TYPE_INFO,
        ((org.apache.flink.api.java.typeutils.ListTypeInfo<?>) listTypeInfo).getElementTypeInfo()
    );

    Assert.assertTrue(mapTypeInfo instanceof org.apache.flink.api.java.typeutils.MapTypeInfo);
    Assert.assertEquals(
        org.apache.flink.api.common.typeinfo.BasicTypeInfo.STRING_TYPE_INFO,
        ((org.apache.flink.api.java.typeutils.MapTypeInfo<?, ?>) mapTypeInfo).getKeyTypeInfo()
    );
    Assert.assertEquals(
        org.apache.flink.api.common.typeinfo.BasicTypeInfo.DOUBLE_TYPE_INFO,
        ((org.apache.flink.api.java.typeutils.MapTypeInfo<?, ?>) mapTypeInfo).getValueTypeInfo()
    );
  }
}
