/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.bytedance.bitsail.common.ddl.typeinfo;

import com.bytedance.bitsail.common.util.Preconditions;

import java.util.Arrays;
import java.util.HashSet;

public class IntegerTypeInfo<T> extends NumericTypeInfo<T> {

  private static final HashSet<Class<?>> NUMERICAL_TYPES = new HashSet<>(
      Arrays.asList(
          Integer.class,
          Long.class,
          Double.class,
          Byte.class,
          Short.class,
          Float.class,
          Character.class));

  protected IntegerTypeInfo(Class<T> clazz) {
    super(clazz);

    Preconditions.checkArgument(NUMERICAL_TYPES.contains(clazz),
        "The given class %s is not a numerical type", clazz.getSimpleName());
  }
}
