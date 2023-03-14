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

package com.bytedance.bitsail.connector.kudu.source.split.strategy;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.connector.kudu.option.KuduReaderOptions;
import com.bytedance.bitsail.connector.kudu.source.split.AbstractKuduSplitConstructor;
import com.bytedance.bitsail.connector.kudu.source.split.KuduSourceSplit;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.kudu.Schema;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduPredicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PredicationDivideSplitConstructor extends AbstractKuduSplitConstructor {
  private static final Logger LOG = LoggerFactory.getLogger(PredicationDivideSplitConstructor.class);

  private SplitConfiguration splitConf = null;
  private boolean available = false;

  public PredicationDivideSplitConstructor(BitSailConfiguration jobConf, KuduClient client) throws IOException {
    super(jobConf, client);

    if (!jobConf.fieldExists(KuduReaderOptions.SPLIT_CONFIGURATION)) {
      LOG.warn("{} cannot work due to lack of split configuration.", this.getClass().getSimpleName());
      return;
    }
    String splitConfStr = jobConf.get(KuduReaderOptions.SPLIT_CONFIGURATION);
    this.splitConf = new ObjectMapper().readValue(splitConfStr, SplitConfiguration.class);
    this.available = splitConf.isValid(schema);
    if (!available) {
      LOG.warn("{} cannot work because split configuration is invalid.", this.getClass().getSimpleName());
      return;
    }
    this.available = fillSplitConf(jobConf, client);
    if (!available) {
      return;
    }

    LOG.info("Successfully created ,final split configuration is: {}", splitConf);
  }

  @Override
  public boolean isAvailable() {
    return available;
  }

  @Override
  protected boolean fillSplitConf(BitSailConfiguration jobConf, KuduClient client) throws IOException {
    if (splitConf.getSplitNum() == null || splitConf.getSplitNum() <= 0) {
      splitConf.setSplitNum(jobConf.getUnNecessaryOption(KuduReaderOptions.READER_PARALLELISM_NUM, 1));
    }
    if (splitConf.getSplitNum() > splitConf.predications.size()) {
      splitConf.setSplitNum(splitConf.predications.size());
      LOG.info("Resize split num to {}.", splitConf.getSplitNum());
    }
    return true;
  }

  @Override
  public List<KuduSourceSplit> construct(KuduClient kuduClient) throws Exception {
    List<KuduSourceSplit> splits = new ArrayList<>(splitConf.getSplitNum());

    List<byte[]> predications = this.splitConf.predications;
    for (int i = 0; i < predications.size(); i++) {
      KuduSourceSplit split = new KuduSourceSplit(i);
      split.addSerializedPredicates(predications.get(i));
      splits.add(split);
    }

    LOG.info("Finally get {} splits.", splits.size());
    for (int i = 0; i < splits.size(); ++i) {
      LOG.info(">>> the {}-th split is: {}", i, splits.get(i).toFormatString(schema));
    }
    return splits;
  }

  @Override
  public int estimateSplitNum() {
    int estimatedSplitNum = 1;
    if (splitConf.getSplitNum() != null && splitConf.getSplitNum() > 0) {
      estimatedSplitNum = splitConf.getSplitNum();
    }
    LOG.info("Estimated split num is: {}", estimatedSplitNum);
    return estimatedSplitNum;
  }

  @NoArgsConstructor
  @AllArgsConstructor
  @Data
  @ToString(of = {"predications", "splitNum"})
  public static class SplitConfiguration {
    @JsonProperty("predications")
    private List<byte[]> predications;

    @JsonProperty("split_num")
    private Integer splitNum;

    @JsonIgnore
    public boolean isValid(Schema schema) {
      if (predications == null || predications.size() == 0) {
        LOG.warn("Predications configurations cannot be empty.");
        return false;
      }
      for (byte[] predication : predications) {

        try {
          KuduPredicate.deserialize(schema, predication);
        } catch (Exception e) {
          LOG.warn("Predication {} cannot be correct deserialize, error is {}.", predication, e.getMessage());
          return false;
        }
      }
      return true;
    }
  }
}