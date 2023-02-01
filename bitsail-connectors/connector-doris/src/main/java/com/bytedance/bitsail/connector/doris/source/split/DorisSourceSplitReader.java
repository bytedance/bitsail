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

package com.bytedance.bitsail.connector.doris.source.split;

import com.bytedance.bitsail.connector.doris.backend.BackendClient;
import com.bytedance.bitsail.connector.doris.backend.model.Routing;
import com.bytedance.bitsail.connector.doris.backend.model.RowBatch;
import com.bytedance.bitsail.connector.doris.config.DorisExecutionOptions;
import com.bytedance.bitsail.connector.doris.config.DorisOptions;
import com.bytedance.bitsail.connector.doris.rest.model.Field;
import com.bytedance.bitsail.connector.doris.rest.model.PartitionDefinition;
import com.bytedance.bitsail.connector.doris.rest.model.Schema;
import com.bytedance.bitsail.connector.doris.thrift.TScanBatchResult;
import com.bytedance.bitsail.connector.doris.thrift.TScanCloseParams;
import com.bytedance.bitsail.connector.doris.thrift.TScanColumnDesc;
import com.bytedance.bitsail.connector.doris.thrift.TScanNextBatchParams;
import com.bytedance.bitsail.connector.doris.thrift.TScanOpenParams;
import com.bytedance.bitsail.connector.doris.thrift.TScanOpenResult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class DorisSourceSplitReader {

  private static final Logger LOG = LoggerFactory.getLogger(DorisSourceSplitReader.class);
  private final String defaultCluster = "default_cluster";
  private final BackendClient client;
  private final PartitionDefinition partition;
  private final DorisExecutionOptions executionOptions;
  private final DorisOptions dorisOptions;
  private TScanOpenParams openParams;
  protected String contextId;
  protected Schema schema;
  protected boolean asyncThreadStarted;
  // flag indicate if support deserialize Arrow to RowBatch asynchronously
  protected Boolean deserializeArrowToRowBatchAsync = false;
  protected AtomicBoolean eos = new AtomicBoolean(false);
  protected int offset = 0;
  protected RowBatch rowBatch;
  protected BlockingQueue<RowBatch> rowBatchBlockingQueue;

  public DorisSourceSplitReader(DorisSourceSplit dorisSplit, DorisExecutionOptions executionOptions, DorisOptions dorisOptions) {
    this.partition = dorisSplit.getPartitionDefinition();
    this.executionOptions = executionOptions;
    this.dorisOptions = dorisOptions;
    this.client = backendClient();
    init();
  }

  private BackendClient backendClient() {
    try {
      return new BackendClient(new Routing(partition.getBeAddress()), executionOptions);
    } catch (IllegalArgumentException e) {
      LOG.error("init backend:{} client failed,", partition.getBeAddress(), e);
      throw new IllegalArgumentException(e);
    }
  }

  private void init() {
    this.openParams = openParams();
    TScanOpenResult openResult = this.client.openScanner(this.openParams);
    this.contextId = openResult.getContextId();
    this.schema = convertToSchema(openResult.getSelectedColumns());
    this.asyncThreadStarted = asyncThreadStarted();
    LOG.debug("Open scan result is, contextId: {}, schema: {}.", contextId, schema);
  }

  protected boolean asyncThreadStarted() {
    boolean started = false;
    if (deserializeArrowToRowBatchAsync) {
      asyncThread.start();
      started = true;
    }
    return started;
  }

  protected Thread asyncThread = new Thread(new Runnable() {
    @Override
    public void run() {
      TScanNextBatchParams nextBatchParams = new TScanNextBatchParams();
      nextBatchParams.setContextId(contextId);
      while (!eos.get()) {
        nextBatchParams.setOffset(offset);
        TScanBatchResult nextResult = client.getNext(nextBatchParams);
        eos.set(nextResult.isEos());
        if (!eos.get()) {
          RowBatch rowBatch = new RowBatch(nextResult, schema).readArrow();
          offset += rowBatch.getReadRowCount();
          rowBatch.close();
          try {
            rowBatchBlockingQueue.put(rowBatch);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        }
      }
    }
  });

  /**
   * convert Doris return schema to inner schema struct.
   *
   * @param tscanColumnDescs Doris BE return schema
   * @return inner schema struct
   */
  public static Schema convertToSchema(List<TScanColumnDesc> tscanColumnDescs) {
    Schema schema = new Schema(tscanColumnDescs.size());
    tscanColumnDescs.stream().forEach(desc -> schema.put(new Field(desc.getName(), desc.getType().name(), "", 0, 0, "")));
    return schema;
  }

  private TScanOpenParams openParams() {
    TScanOpenParams params = new TScanOpenParams();
    params.cluster = defaultCluster;
    params.database = partition.getDatabase();
    params.table = partition.getTable();

    params.tablet_ids = Arrays.asList(partition.getTabletIds().toArray(new Long[] {}));
    params.opaqued_query_plan = partition.getQueryPlan();
    // max row number of one read batch
    Integer batchSize = executionOptions.getRequestBatchSize();
    Integer queryDorisTimeout = executionOptions.getRequestQueryTimeoutS();
    Long execMemLimit = executionOptions.getExecMemLimit();
    params.setBatchSize(batchSize);
    params.setQueryTimeout(queryDorisTimeout);
    params.setMemLimit(execMemLimit);
    params.setUser(dorisOptions.getUsername());
    params.setPasswd(dorisOptions.getPassword());
    LOG.debug("Open scan params is,cluster:{},database:{},table:{},tabletId:{},batch size:{},query timeout:{},execution memory limit:{},user:{},query plan: {}",
        params.getCluster(), params.getDatabase(), params.getTable(), params.getTabletIds(), params.getBatchSize(), params.getQueryTimeout(), params.getMemLimit(),
        params.getUser(), params.getOpaquedQueryPlan());
    return params;
  }

  /**
   * read data and cached in rowBatch.
   *
   * @return true if hax next value
   */
  @SuppressWarnings("checkstyle:MagicNumber")
  public boolean hasNext() {
    boolean hasNext = false;
    if (deserializeArrowToRowBatchAsync && asyncThreadStarted) {
      // support deserialize Arrow to RowBatch asynchronously
      if (rowBatch == null || !rowBatch.hasNext()) {
        while (!eos.get() || !rowBatchBlockingQueue.isEmpty()) {
          if (!rowBatchBlockingQueue.isEmpty()) {
            try {
              rowBatch = rowBatchBlockingQueue.take();
            } catch (InterruptedException e) {
              throw new RuntimeException(e);
            }
            hasNext = true;
            break;
          } else {
            // wait for rowBatch put in queue or eos change
            try {
              Thread.sleep(5);
            } catch (InterruptedException e) {
              throw new RuntimeException(e);
            }
          }
        }
      } else {
        hasNext = true;
      }
    } else {
      // Arrow data was acquired synchronously during the iterative process
      if (!eos.get() && (rowBatch == null || !rowBatch.hasNext())) {
        if (rowBatch != null) {
          offset += rowBatch.getReadRowCount();
          rowBatch.close();
        }
        TScanNextBatchParams nextBatchParams = new TScanNextBatchParams();
        nextBatchParams.setContextId(contextId);
        nextBatchParams.setOffset(offset);
        TScanBatchResult nextResult = client.getNext(nextBatchParams);
        eos.set(nextResult.isEos());
        if (!eos.get()) {
          rowBatch = new RowBatch(nextResult, schema).readArrow();
        }
      }
      hasNext = !eos.get();
    }
    return hasNext;
  }

  /**
   * get next value.
   *
   * @return next value
   */
  public List<Object> next() {
    return rowBatch.next();
  }

  public void close() throws Exception {
    TScanCloseParams closeParams = new TScanCloseParams();
    closeParams.setContextId(contextId);
    client.closeScanner(closeParams);
  }

}
