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

package com.bytedance.bitsail.common.catalog.table;

import java.io.Serializable;

public enum TableOperation implements Serializable {
  CHANGE_TBL_PROPS,
  CHANGE_SERDE_PROPS,
  CHANGE_FILE_FORMAT,
  CHANGE_LOCATION,
  ALTER_COLUMNS_ADD,
  ALTER_COLUMNS_DELETE,
  ALTER_COLUMNS_UPDATE
}
