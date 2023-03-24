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

package com.bytedance.bitsail.connector.mongodb.source.reader;

import com.bytedance.bitsail.base.connector.reader.v1.SourcePipeline;
import com.bytedance.bitsail.base.connector.reader.v1.SourceReader;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.connector.mongodb.config.MongoDBConnConfig;
import com.bytedance.bitsail.connector.mongodb.option.MongoDBReaderOptions;
import com.bytedance.bitsail.connector.mongodb.source.split.MongoDBSourceSplit;
import com.bytedance.bitsail.connector.mongodb.util.MongoDBUtils;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedDeque;

import static com.bytedance.bitsail.connector.mongodb.constant.MongoDBConstants.DEFAULT_PARALLELISM_NUM;
import static com.bytedance.bitsail.connector.mongodb.error.MongoDBErrorCode.REQUIRED_VALUE;

public class MongoDBSourceReader implements SourceReader<Row, MongoDBSourceSplit> {

  private static final Logger LOG = LoggerFactory.getLogger(MongoDBSourceReader.class);

  private final transient MongoDBRowDeserializer rowDeserializer;
  private final MongoDBConnConfig mongoConnConfig;
  private final Deque<MongoDBSourceSplit> splits;

  private final int subTaskId;
  private final String filter;
  private final String splitKey;

  private int totalSplitNum;
  private boolean hasNoMoreSplits = false;

  private transient MongoClient mongoClient;
  private transient MongoCursor<Document> cursor;
  private MongoDBSourceSplit currSplit;
  private MongoCollection<Document> collection;

  public MongoDBSourceReader(BitSailConfiguration jobConf, SourceReader.Context readerContext) {
    this.subTaskId = readerContext.getIndexOfSubtask();

    this.mongoConnConfig = MongoDBConnConfig.initMongoConnConfig(jobConf);
    this.filter = jobConf.getUnNecessaryOption(MongoDBReaderOptions.FILTER, null);

    this.splitKey = jobConf.getNecessaryOption(MongoDBReaderOptions.SPLIT_PK, REQUIRED_VALUE);
    if (!isIndexKey(mongoConnConfig, splitKey)) {
      LOG.warn("split key [{}] was not an index key", splitKey);
    }
    LOG.info("split key [{}] ", splitKey);

    this.splits = new ConcurrentLinkedDeque<>();
    this.rowDeserializer = new MongoDBRowDeserializer(readerContext.getRowTypeInfo());
  }

  /**
   * Get parallelism num from the job configuration.
   *
   * @return parallelism num
   */
  public static int getParallelismNum(BitSailConfiguration jobConf) {
    int parallelismNum = jobConf.getUnNecessaryOption(MongoDBReaderOptions.READER_PARALLELISM_NUM, -1);
    if (parallelismNum < 0) {
      String filter = jobConf.getUnNecessaryOption(MongoDBReaderOptions.FILTER, null);
      if (StringUtils.isBlank(filter)) {
        parallelismNum = DEFAULT_PARALLELISM_NUM;
        LOG.info("not provide reader_parallelism_num, will use default num [{}]", DEFAULT_PARALLELISM_NUM);
      } else {
        parallelismNum = 1;
        LOG.info("provided reader_parallelism_num less than 0, will be set to [1] for filter query");
      }
    }

    return parallelismNum;
  }

  private static boolean isIndexKey(MongoDBConnConfig mongoConnConfig, String splitKey) {
    MongoClient mongoClient = MongoDBUtils.initMongoClientWithRetry(mongoConnConfig);
    MongoCollection<Document> collection = mongoClient.getDatabase(mongoConnConfig.getDbName()).getCollection(mongoConnConfig.getCollectionName());
    Set<String> indexKeys = MongoDBUtils.getCollectionIndexKey(collection);
    LOG.info("index keys in mongodb collection [{}] are: {}", mongoConnConfig.getCollectionName(), indexKeys);
    return indexKeys.contains(splitKey);
  }

  @Override
  public void start() {
    mongoClient = MongoDBUtils.initMongoClientWithRetry(mongoConnConfig);
    MongoDatabase database = mongoClient.getDatabase(mongoConnConfig.getDbName());
    collection = database.getCollection(mongoConnConfig.getCollectionName());

    LOG.info("mongodb source reader task {} started", subTaskId);
  }

  @Override
  public void pollNext(SourcePipeline<Row> pipeline) throws IOException {
    if (cursor == null && splits.isEmpty()) {
      return;
    }

    if (cursor == null) {
      this.currSplit = splits.poll();

      Object lowerBound = currSplit.getLowerBound();
      Object upperBound = currSplit.getUpperBound();
      Document filterDoc = getFilterDocument(lowerBound, upperBound);
      cursor = collection.find(filterDoc).iterator();

      LOG.info("mongodb reader task {} begins to read split: {}", subTaskId, currSplit);
    }

    boolean hasNext = cursor.hasNext();
    if (hasNext) {
      Document item = cursor.next();
      Row row = rowDeserializer.convert(item);
      pipeline.output(row);
    } else {
      // close will lead exception: Cursor has been closed
      cursor = null;
      LOG.info("mongodb reader task {} finished reading rows from split: {}", subTaskId, currSplit);
    }
  }

  @Override
  public void addSplits(List<MongoDBSourceSplit> splitList) {
    totalSplitNum += splitList.size();
    splits.addAll(splitList);
  }

  @Override
  public void notifyNoMoreSplits() {
    this.hasNoMoreSplits = true;
    LOG.info("No more splits will be assigned");
  }

  @Override
  public boolean hasMoreElements() {
    if (hasNoMoreSplits && splits.isEmpty() && cursor == null) {
      LOG.info("Finish reading all {} splits", totalSplitNum);
      return false;
    }
    return true;
  }

  @Override
  public List<MongoDBSourceSplit> snapshotState(long checkpointId) {
    return Collections.emptyList();
  }

  @Override
  public void close() {
    closeQuietly(cursor);
    closeQuietly(mongoClient);
  }

  private void closeQuietly(AutoCloseable closeable) {
    try {
      if (closeable != null) {
        closeable.close();
        closeable = null;
      }
    } catch (Exception e) {
      LOG.error("failed to close " + closeable.getClass().getSimpleName(), e);
    }
  }

  private Document getFilterDocument(Object lowerBound, Object upperBound) {
    Document filterDoc = new Document();
    if (lowerBound == null) {
      if (upperBound != null) {
        filterDoc.append(splitKey, new Document("$lt", upperBound));
      }
    } else {
      if (upperBound == null) {
        filterDoc.append(splitKey, new Document("$gte", lowerBound));
      } else {
        filterDoc.append(splitKey, new Document("$gte", lowerBound).append("$lt", upperBound));
      }
    }

    if (StringUtils.isNotEmpty(this.filter)) {
      Document queryFilter = Document.parse(this.filter);
      filterDoc = new Document("$and", Arrays.asList(filterDoc, queryFilter));
    }
    return filterDoc;
  }

}
