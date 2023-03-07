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

import com.bytedance.bitsail.connector.mongodb.config.MongoDBConnConfig;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Paginating splitter for MongoDB
 */
public class MongoDBSplitter4Paginating extends MongoDBSplitter implements Serializable {

  private final int fetchSize;

  public MongoDBSplitter4Paginating(MongoDBConnConfig mongoConnConfig, String splitKey, int fetchSize, int parallelism) {
    super(mongoConnConfig, parallelism, splitKey);
    this.fetchSize = fetchSize;
  }

  @Override
  public List<MongoDBSourceSplit> getSplits() {
    List<MongoDBSourceSplit> splitList = new ArrayList<>();
    long count = collection.countDocuments();
    if (count == 0) {
      return splitList;
    }
    int splitPointsCount = 0;
    if (count >= this.fetchSize) {
      splitPointsCount = this.fetchSize <= 1 ? (int) (count - 1) : (int) (count / this.fetchSize - 1);
    }

    List<Object> splitPoints = getSplitPoints(splitPointsCount, this.fetchSize);
    splitList = buildSplitRangeList(splitPoints);
    return splitList;
  }
}
