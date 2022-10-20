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

package com.bytedance.bitsail.common.row;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

@Setter
@Getter
public class Row implements Serializable {

  private final Object[] fields;
  private RowKind kind = RowKind.INSERT;

  public Row(int size) {
    fields = new Object[size];
  }

  public Row(Object[] fields) {
    this.fields = fields;
  }

  public Row(byte kind, Object[] fields) {
    this.kind = RowKind.fromByteValue(kind);
    this.fields = fields;
  }

  public void setField(int pos, Object value) {
    this.fields[pos] = value;
  }

  public Object getField(int pos) {
    return this.fields[pos];
  }

  public Object[] getFields() {
    return fields;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Row row = (Row) o;
    return kind == row.kind && Arrays.equals(fields, row.fields);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(kind);
    result = 31 * result + Arrays.hashCode(fields);
    return result;
  }
}
