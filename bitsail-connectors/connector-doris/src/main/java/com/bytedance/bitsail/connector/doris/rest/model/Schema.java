/*
 * Copyright 2022 Bytedance Ltd. and/or its affiliates.
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

package com.bytedance.bitsail.connector.doris.rest.model;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
@EqualsAndHashCode
public class Schema {
  private int status = 0;
  private String keysType;
  private List<Field> properties;

  public Schema(int fieldCount) {
    properties = new ArrayList<>(fieldCount);
  }

  public void put(String name, String type, String comment, int scale, int precision, String aggregationType) {
    properties.add(new Field(name, type, comment, scale, precision, aggregationType));
  }

  public void put(Field f) {
    properties.add(f);
  }

  public Field get(int index) {
    if (index >= properties.size()) {
      String errMsg = String.format("index=%s, fields size=%s", index, properties.size());
      throw new IndexOutOfBoundsException(errMsg);
    }
    return properties.get(index);
  }

  public int size() {
    return properties.size();
  }

  @Override
  public String toString() {
    return "Schema{" +
        "status=" + status +
        ", properties=" + properties +
        '}';
  }
}
