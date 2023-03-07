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

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.mongodb.MongoCommandException;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Parallelism splitter for MongoDB
 */
public class MongoDBSplitter4Parallelism extends MongoDBSplitter {

  private static final int MB_SIZE = 1024 * 1024;

  private static final Logger LOG = LoggerFactory.getLogger(MongoDBSplitter4Parallelism.class);

  private static final int ERR_CODE_UNAUTHORIZED = 13;
  private static final int ERR_CODE_ILLEGAL_OP = 20;

  public MongoDBSplitter4Parallelism(MongoDBConnConfig mongoConnConfig, String splitKey, int parallelism) {
    super(mongoConnConfig, parallelism, splitKey);
  }

  private static int getAvgObjSize(Document collStats) {
    int avgObjSize = 1;
    Object avgObjSizeObj = collStats.get("avgObjSize");
    if (avgObjSizeObj instanceof Integer) {
      avgObjSize = (Integer) avgObjSizeObj;
    } else if (avgObjSizeObj instanceof Double) {
      avgObjSize = ((Double) avgObjSizeObj).intValue();
    }
    return avgObjSize;
  }

  @Override
  public List<MongoDBSourceSplit> getSplits() {
    if (parallelism == 1) {
      LOG.info("Task parallelism equals to 1, skip the split");
      return Lists.newArrayList(new MongoDBSourceSplit());
    }

    Stopwatch started = Stopwatch.createStarted();
    try {
      Document collStats = database.runCommand(new Document("collStats", mongoConnConfig.getCollectionName()));
      Object count = collStats.get("count");
      long docCount = 0;
      if (count instanceof Double) {
        docCount = ((Double) count).longValue();
      } else if (count instanceof Integer) {
        docCount = (Integer) count;
      }

      LOG.info("Total doc count is {} in mongodb database {}", docCount, database.getName());
      if (docCount == 0) {
        return Lists.newArrayList();
      }

      int splitsCount = parallelism - 1;
      boolean splitVector = supportSplitVector();
      List<Object> splitPoints;
      if (splitVector) {
        splitPoints = splitVector(collStats, splitsCount, docCount);
        LOG.info("parallelism split by command [splitVector], split points size: {}", splitPoints.size());
      } else {
        splitPoints = splitSplitKey(splitsCount, docCount);
        LOG.info("parallelism split by [splitKey], split points size: {}", splitPoints.size());
      }

      return buildSplitRangeList(splitPoints);
    } finally {
      LOG.info("Split range time cost: {}", started.elapsed(TimeUnit.SECONDS));
    }
  }

  private boolean supportSplitVector() {
    try {
      database.runCommand(new Document("splitVector", mongoConnConfig.getDbName() + "." + mongoConnConfig.getCollectionName())
          .append("keyPattern", new Document(splitKey, 1))
          .append("force", true));
    } catch (MongoCommandException e) {
      if (e.getErrorCode() == ERR_CODE_UNAUTHORIZED || e.getErrorCode() == ERR_CODE_ILLEGAL_OP) {
        return false;
      }
    }
    return true;
  }

  private List<Object> splitVector(Document collStats, int splitsCount, long docCount) {
    List<Object> splitPoints = Lists.newArrayList();
    boolean forceMedianSplit = false;
    int avgObjSize = getAvgObjSize(collStats);
    long maxChunkSize = (docCount / splitsCount - 1) * 2 * avgObjSize / MB_SIZE;
    if (maxChunkSize < 1) {
      forceMedianSplit = true;
    }
    Document result;
    if (!forceMedianSplit) {
      result = database.runCommand(new Document("splitVector",
          mongoConnConfig.getDbName() + "." + mongoConnConfig.getCollectionName())
          .append("keyPattern", new Document(splitKey, 1))
          .append("maxChunkSize", maxChunkSize)
          .append("maxSplitPoints", parallelism - 1));
    } else {
      result = database.runCommand(new Document("splitVector",
          mongoConnConfig.getDbName() + "." + mongoConnConfig.getCollectionName())
          .append("keyPattern", new Document(splitKey, 1))
          .append("force", true));
    }

    ArrayList<Document> splitKeys = result.get("splitKeys", ArrayList.class);
    for (Document splitKeyDoc : splitKeys) {
      Object id = splitKeyDoc.get(splitKey);
      splitPoints.add(id);
    }
    return splitPoints;
  }

  private List<Object> splitSplitKey(int splitPointCount, long docCount) {
    int chunkDocCount = (int) docCount / parallelism;
    if (chunkDocCount == 0) {
      LOG.info("docCount is smaller than parallelism: " + docCount);
      chunkDocCount = (int) docCount - 1;
      splitPointCount = 1;
    }

    int skipCount = chunkDocCount;
    return getSplitPoints(splitPointCount, skipCount);
  }
}
