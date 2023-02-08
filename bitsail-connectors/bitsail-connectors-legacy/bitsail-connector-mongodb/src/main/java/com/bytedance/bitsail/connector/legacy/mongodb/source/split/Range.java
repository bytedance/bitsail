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

package com.bytedance.bitsail.connector.legacy.mongodb.source.split;

import java.io.Serializable;

public class Range implements Serializable {

  Object lowerBound;
  Object upperBound;

  public Range(Object lowerBound, Object upperBound) {
    this.lowerBound = lowerBound;
    this.upperBound = upperBound;
  }

  public Object getLowerBound() {
    return lowerBound;
  }

  public Object getUpperBound() {
    return upperBound;
  }

  @Override
  public String toString() {
    return "[" + lowerBound + "," + upperBound + "]";
  }
}