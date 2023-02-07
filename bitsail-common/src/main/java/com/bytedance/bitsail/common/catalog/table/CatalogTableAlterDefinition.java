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

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import org.apache.commons.collections.CollectionUtils;

import java.io.Serializable;
import java.util.List;

@Builder
@AllArgsConstructor
@Getter
public class CatalogTableAlterDefinition implements Serializable {

  private List<CatalogTableColumn> pendingAddColumns;

  private List<CatalogTableColumn> pendingUpdateColumns;

  private List<CatalogTableColumn> pendingDeleteColumns;

  public boolean isNotEmpty() {
    return CollectionUtils.isNotEmpty(pendingAddColumns) ||
        CollectionUtils.isNotEmpty(pendingUpdateColumns) ||
        CollectionUtils.isNotEmpty(pendingDeleteColumns);
  }
}
