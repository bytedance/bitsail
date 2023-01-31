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
import com.bytedance.bitsail.connector.elasticsearch.option.ElasticsearchReaderOptions;
import com.bytedance.bitsail.connector.elasticsearch.rest.source.EsSourceRequest;
import com.bytedance.bitsail.connector.elasticsearch.util.SplitStringUtil;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.compress.utils.Lists;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;

import static com.bytedance.bitsail.connector.elasticsearch.error.ElasticsearchErrorCode.REQUIRED_VALUE;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ElasticsearchSplitByIndexStrategy implements ElasticsearchSplitStrategy {

  private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchSplitByIndexStrategy.class);
  private RestHighLevelClient restHighLevelClient;

  public List<ElasticsearchSourceSplit> getElasticsearchSplits(BitSailConfiguration jobConf) {
    List<ElasticsearchSourceSplit> splits = Lists.newArrayList();

    String indices = jobConf.getNecessaryOption(ElasticsearchReaderOptions.ES_INDEX, REQUIRED_VALUE);
    EsSourceRequest esSourceRequest = new EsSourceRequest(restHighLevelClient);

    String[] splitIndices = SplitStringUtil.splitString(indices);
    int idx = 0;
    for (String index : splitIndices) {
      if (check(index, esSourceRequest)) {
        ElasticsearchSourceSplit split = new ElasticsearchSourceSplit(idx++);
        split.setIndex(index);
        splits.add(split);
      }
    }
    return splits;
  }

  private boolean check(String index, EsSourceRequest esSourceRequest) {
    Long count = esSourceRequest.validateIndex(index);
    return Objects.nonNull(count) && count > 0;
  }

  /**
   * Used for determine parallelism num, simply based on indices num.
   *
   * @return estimate split num
   */
  @Override
  public int estimateSplitNum(BitSailConfiguration configuration) {
    String indices = configuration.getNecessaryOption(ElasticsearchReaderOptions.ES_INDEX, REQUIRED_VALUE);
    int estimatedSplitNum = SplitStringUtil.splitString(indices).length;
    LOG.info("Estimated split num is: {}", estimatedSplitNum);
    return estimatedSplitNum;
  }
}
