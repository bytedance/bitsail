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
import com.bytedance.bitsail.connector.mongodb.util.MongoDBUtils;

import com.google.common.collect.Lists;
import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import lombok.Getter;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

/**
 * Splitter strategy for MongoDB source
 */
public abstract class MongoDBSplitter {

  public MongoDBConnConfig mongoConnConfig;

  /**
   * max parallelism num, means max split num
   */
  @Getter
  public int parallelism;

  public transient MongoClient mongoClient;
  public MongoCollection<Document> collection;
  public MongoDatabase database;
  public String splitKey;

  public MongoDBSplitter(MongoDBConnConfig mongoConnConfig, int parallelism, String splitKey) {
    this.mongoConnConfig = mongoConnConfig;
    mongoClient = MongoDBUtils.initMongoClientWithRetry(mongoConnConfig);
    database = mongoClient.getDatabase(this.mongoConnConfig.getDbName());
    collection = database.getCollection(this.mongoConnConfig.getCollectionName());

    this.splitKey = splitKey;
    this.parallelism = parallelism;
  }

  /**
   * Split the range by split key
   *
   * @return list of split range
   */
  public abstract List<MongoDBSourceSplit> getSplits();

  /**
   * Compute split points according to region number and region size.
   *
   * @param rangeNum range number
   * @param step     current step
   * @return list of split points
   */
  public List<Object> getSplitPoints(int rangeNum, int step) {
    List<Object> splitPoints = new ArrayList<>(rangeNum);

    if (rangeNum == 0) {
      Document result = collection.find().sort(new BasicDBObject(splitKey, 1)).limit(1).first();
      assert result != null;
      Object obj = result.get(splitKey);
      splitPoints.add(obj);
    } else {
      int num = 0;
      Document result;
      Object obj = null;
      while (num < rangeNum) {
        if (num == 0) {
          result = collection.find().sort(new BasicDBObject(splitKey, 1)).limit(1).skip(step).first();
        } else {
          assert obj != null;
          result = collection.find(Filters.gte(splitKey, obj)).sort(new BasicDBObject(splitKey, 1)).limit(1).skip(step).first();
        }

        num++;
        assert result != null;
        obj = result.get(splitKey);
        splitPoints.add(obj);
      }
    }

    return splitPoints;
  }

  /**
   * Build split range list according to the split points.
   *
   * @param splitPoints split points
   * @return a list of split range
   */
  public List<MongoDBSourceSplit> buildSplitRangeList(List<Object> splitPoints) {
    List<MongoDBSourceSplit> rangeList = Lists.newArrayList();
    Object lastValue = null;

    for (int i = 0; i < splitPoints.size(); i++) {
      Object splitPoint = splitPoints.get(i);
      MongoDBSourceSplit sourceSplit = new MongoDBSourceSplit(String.valueOf(i), lastValue, splitPoint);
      rangeList.add(sourceSplit);
      lastValue = splitPoint;
    }

    rangeList.add(new MongoDBSourceSplit(String.valueOf(splitPoints.size()), lastValue, null));
    return rangeList;
  }

}

