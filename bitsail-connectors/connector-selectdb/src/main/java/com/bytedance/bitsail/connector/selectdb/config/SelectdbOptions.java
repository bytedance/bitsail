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

package com.bytedance.bitsail.connector.selectdb.config;

import com.bytedance.bitsail.common.model.ColumnInfo;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;
import java.util.List;

/**
 * Options for the Selectdb connector.
 */
@Builder
@Data
public class SelectdbOptions implements Serializable {
  private static final long serialVersionUID = 1L;

  private String loadUrl;
  private String jdbcUrl;
  private String username;
  private String password;
  private String clusterName;
  private String tableIdentifier;
  private List<ColumnInfo> columnInfos;
  private String fieldDelimiter;
  private String lineDelimiter;
  private LOAD_CONTENT_TYPE loadDataFormat;

  public enum LOAD_CONTENT_TYPE {
    JSON,
    CSV
  }

}


