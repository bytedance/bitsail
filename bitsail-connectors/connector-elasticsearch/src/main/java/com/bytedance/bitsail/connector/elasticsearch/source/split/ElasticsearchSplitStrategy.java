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

package com.bytedance.bitsail.connector.elasticsearch.source.split;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;

import java.util.List;

public interface ElasticsearchSplitStrategy {

  /**
   * Get source split by elasticsearch cluster metadata
   *
   * @param jobConf user conf
   * @return es-source-split collection
   */
  List<ElasticsearchSourceSplit> getElasticsearchSplits(BitSailConfiguration jobConf);

  /**
   * Estimate split num base on conf
   *
   * @param jobConf user conf
   * @return estimate split num
   */
  int estimateSplitNum(BitSailConfiguration jobConf);
}
