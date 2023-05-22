/*
 *       Copyright 2022-2023 Bytedance Ltd. and/or its affiliates.
 *
 *       Licensed under the Apache License, Version 2.0 (the "License");
 *       you may not use this file except in compliance with the License.
 *       You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *       Unless required by applicable law or agreed to in writing, software
 *       distributed under the License is distributed on an "AS IS" BASIS,
 *       WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *       See the License for the specific language governing permissions and
 *       limitations under the License.
 */

package com.bytedance.bitsail.common.typeinfo;

import org.junit.Assert;
import org.junit.Test;

public class TypeInfosTest {

  @Test
  public void testBridgeClass() {
    TypeInfo<?> typeInfo;
    typeInfo = TypeInfoBridge.bridgeTypeClass(double.class);
    Assert.assertEquals(typeInfo, TypeInfos.DOUBLE_TYPE_INFO);

    typeInfo = TypeInfoBridge.bridgeTypeClass(Double.class);
    Assert.assertEquals(typeInfo, TypeInfos.DOUBLE_TYPE_INFO);

    typeInfo = TypeInfoBridge.bridgeTypeClass(int.class);
    Assert.assertEquals(typeInfo, TypeInfos.INT_TYPE_INFO);

    typeInfo = TypeInfoBridge.bridgeTypeClass(Integer.class);
    Assert.assertEquals(typeInfo, TypeInfos.INT_TYPE_INFO);

    typeInfo = TypeInfoBridge.bridgeTypeClass(Short.class);
    Assert.assertEquals(typeInfo, TypeInfos.SHORT_TYPE_INFO);

    typeInfo = TypeInfoBridge.bridgeTypeClass(short.class);
    Assert.assertEquals(typeInfo, TypeInfos.SHORT_TYPE_INFO);

    typeInfo = TypeInfoBridge.bridgeTypeClass(Long.class);
    Assert.assertEquals(typeInfo, TypeInfos.LONG_TYPE_INFO);

    typeInfo = TypeInfoBridge.bridgeTypeClass(long.class);
    Assert.assertEquals(typeInfo, TypeInfos.LONG_TYPE_INFO);

    typeInfo = TypeInfoBridge.bridgeTypeClass(Float.class);
    Assert.assertEquals(typeInfo, TypeInfos.FLOAT_TYPE_INFO);

    typeInfo = TypeInfoBridge.bridgeTypeClass(float.class);
    Assert.assertEquals(typeInfo, TypeInfos.FLOAT_TYPE_INFO);

    typeInfo = TypeInfoBridge.bridgeTypeClass(Boolean.class);
    Assert.assertEquals(typeInfo, TypeInfos.BOOLEAN_TYPE_INFO);

    typeInfo = TypeInfoBridge.bridgeTypeClass(boolean.class);
    Assert.assertEquals(typeInfo, TypeInfos.BOOLEAN_TYPE_INFO);

    typeInfo = TypeInfoBridge.bridgeTypeClass(void.class);
    Assert.assertEquals(typeInfo, TypeInfos.VOID_TYPE_INFO);

    typeInfo = TypeInfoBridge.bridgeTypeClass(Void.class);
    Assert.assertEquals(typeInfo, TypeInfos.VOID_TYPE_INFO);
  }

}