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

package com.bytedance.bitsail.connector.mongodb.source.split;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.connector.mongodb.config.MongoDBConnConfig;
import com.bytedance.bitsail.connector.mongodb.constant.MongoDBSplitMode;
import com.bytedance.bitsail.connector.mongodb.error.MongoDBErrorCode;
import com.bytedance.bitsail.connector.mongodb.option.MongoDBReaderOptions;
import com.bytedance.bitsail.connector.mongodb.source.reader.MongoDBSourceReader;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;

/**
 * Splitter factory for MongoDB.
 */
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class MongoDBSplitterFactory {

  private static final int DEFAULT_FETCH_SIZE = 100000;

  private int batchSize;
  private int parallelism;
  private String splitKey;
  private MongoDBConnConfig connConfig;

  /**
   * Get factory instance from the job config.
   *
   * @param jobConf         job config
   * @param mongoConnConfig mongodb connection config
   * @return instance of splitter factory
   */
  public static MongoDBSplitterFactory getMongoSplitterFactory(BitSailConfiguration jobConf,
                                                               MongoDBConnConfig mongoConnConfig) {
    int batchSize = jobConf.getUnNecessaryOption(MongoDBReaderOptions.READER_FETCH_SIZE, DEFAULT_FETCH_SIZE);
    int parallelismNum = MongoDBSourceReader.getParallelismNum(jobConf);
    String splitKey = jobConf.getNecessaryOption(MongoDBReaderOptions.SPLIT_PK, MongoDBErrorCode.REQUIRED_VALUE);

    return MongoDBSplitterFactory.builder()
        .batchSize(batchSize)
        .parallelism(parallelismNum)
        .connConfig(mongoConnConfig)
        .splitKey(splitKey)
        .build();
  }

  public MongoDBSplitter getSplitter(BitSailConfiguration configuration) {
    String splitMode = StringUtils.lowerCase(configuration.get(MongoDBReaderOptions.SPLIT_MODE));
    switch (splitMode) {
      case MongoDBSplitMode.PAGINATING:
        return new MongoDBSplitter4Paginating(connConfig, splitKey, batchSize, parallelism);
      case MongoDBSplitMode.PARALLELISM:
        return new MongoDBSplitter4Parallelism(connConfig, splitKey, parallelism);
      default:
        throw new UnsupportedOperationException(String.format("Unsupported split mode %s for MongoDB.", splitMode));
    }
  }

}
