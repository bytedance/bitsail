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

package com.bytedance.bitsail.common.catalog.table;

import com.bytedance.bitsail.common.typeinfo.TypeInfo;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.io.Serializable;

@Getter
@Builder
@AllArgsConstructor
@EqualsAndHashCode(of = {"name", "type"})
public class CatalogTableColumn implements Serializable {

  private final String name;

  private final TypeInfo<?> type;

  private String comment;

  public CatalogTableColumn(String name, TypeInfo<?> type) {
    this.name = name;
    this.type = type;
  }

  @Override
  public String toString() {
    return "CatalogTableColumn{" +
        "name='" + name + '\'' +
        ", type=" + type +
        ", comment='" + comment + '\'' +
        '}';
  }
}
