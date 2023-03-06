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

package com.bytedance.bitsail.connector.elasticsearch.error;

import com.bytedance.bitsail.common.exception.ErrorCode;

public enum ElasticsearchErrorCode implements ErrorCode {

  REQUIRED_VALUE("Elasticsearch-00", "The configuration file is lack of necessary options"),
  VALID_INDEX_FAILED("Elasticsearch-01", "Try to connect index failed."),
  NOT_SUPPORT_SPLIT_STRATEGY("Elasticsearch-02", "Split strategy not support yet."),
  FETCH_DATA_FAILED("Elasticsearch-03", "Fetch data from elasticsearch cluster failed."),
  DESERIALIZE_FAILED("Elasticsearch-04", "Deserialize data from elasticsearch cluster failed.");

  private final String code;

  private final String describe;

  ElasticsearchErrorCode(String code, String describe) {
    this.code = code;
    this.describe = describe;
  }

  @Override
  public String getCode() {
    return code;
  }

  @Override
  public String getDescription() {
    return describe;
  }

  @Override
  public String toString() {
    return String.format("Code:[%s], Describe:[%s]", this.code,
        this.describe);
  }
}
