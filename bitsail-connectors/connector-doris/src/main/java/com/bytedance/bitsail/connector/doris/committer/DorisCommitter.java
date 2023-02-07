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

package com.bytedance.bitsail.connector.doris.committer;

import com.bytedance.bitsail.base.connector.writer.v1.WriterCommitter;
import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.connector.doris.DorisConnectionHolder;
import com.bytedance.bitsail.connector.doris.config.DorisExecutionOptions;
import com.bytedance.bitsail.connector.doris.config.DorisExecutionOptions.WRITE_MODE;
import com.bytedance.bitsail.connector.doris.config.DorisOptions;
import com.bytedance.bitsail.connector.doris.error.DorisErrorCode;
import com.bytedance.bitsail.connector.doris.http.HttpPutBuilder;
import com.bytedance.bitsail.connector.doris.http.HttpUtil;
import com.bytedance.bitsail.connector.doris.http.ResponseUtil;
import com.bytedance.bitsail.connector.doris.partition.DorisPartition;
import com.bytedance.bitsail.connector.doris.partition.DorisPartitionManager;
import com.bytedance.bitsail.connector.doris.rest.RestService;
import com.bytedance.bitsail.connector.doris.sink.ddl.DorisSchemaManagerGenerator;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.NoArgsConstructor;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.bytedance.bitsail.connector.doris.sink.streamload.LoadStatus.FAIL;
import static org.apache.http.HttpStatus.SC_OK;

@NoArgsConstructor
public class DorisCommitter implements WriterCommitter<DorisCommittable> {
  private static final Logger LOG = LoggerFactory.getLogger(DorisCommitter.class);
  private static final String COMMIT_PATTERN = "http://%s/api/%s/_stream_load_2pc";
  protected DorisPartitionManager dorisPartitionManager;
  protected DorisConnectionHolder dorisConnectionHolder;
  protected DorisOptions dorisOptions;
  protected DorisExecutionOptions executionOptions;
  protected DorisExecutionOptions.WRITE_MODE writeMode;
  private int maxRetry;
  private transient CloseableHttpClient httpClient;

  public DorisCommitter(DorisOptions dorisOptions, DorisExecutionOptions executionOptions) {
    this.dorisOptions = dorisOptions;
    this.executionOptions = executionOptions;
    this.writeMode = executionOptions.getWriterMode();
    this.maxRetry = executionOptions.getMaxRetries();
  }

  @Override
  public List<DorisCommittable> commit(List<DorisCommittable> committables) throws IOException {
    if (this.writeMode.equals(WRITE_MODE.BATCH_UPSERT) && executionOptions.isEnable2PC()) {
      for (DorisCommittable committable : committables) {
        try {
          // TODO add a open method init it
          this.httpClient = new HttpUtil().getHttpClient();

          commitTransaction(committable);
        } finally {
          close();
        }
      }
    } else if (this.writeMode.equals(WRITE_MODE.BATCH_REPLACE)) {
      replacePartOrTab(committables);
    }
    return Collections.emptyList();
  }

  private void replacePartOrTab(List<DorisCommittable> committables) throws IOException {
    LOG.info("Try to commit temporary partition or table, num of committing events: {}", committables.size());
    try {
      dorisConnectionHolder = DorisSchemaManagerGenerator.getDorisConnection(new DorisConnectionHolder(), dorisOptions);
      dorisPartitionManager = DorisSchemaManagerGenerator.openDorisPartitionManager(dorisConnectionHolder, dorisOptions);
      if (!dorisOptions.isTableHasPartitions()) {
        //start to move temp table to normal table
        dorisPartitionManager.replacePartitionWithoutMutable();
        return;
      }
      List<DorisPartition> partitions = dorisOptions.getPartitions();

      if (!partitions.isEmpty()) {
        for (DorisPartition partition : partitions) {
          LOG.info("Start to commit temp partition {} to normal partition {}, start range: {}, end range: {}",
              partition.getTempName(),
              partition.getName(),
              partition.getStartRange(),
              partition.getEndRange()
          );
        }
      }
      //start to move temp partition to normal partition
      dorisPartitionManager.replacePartitionWithoutMutable();
      LOG.info("Succeed to commit temp partition");
      dorisPartitionManager.cleanTemporaryPartition();
      LOG.info("Cleaned temp partition");
      dorisConnectionHolder.closeDorisConnection();
    } catch (SQLException e) {
      throw new IOException("Failed to commit doris partition, Error: " + e.getMessage());
    } finally {
      if (Objects.nonNull(dorisConnectionHolder)) {
        dorisConnectionHolder.closeDorisConnection();
      }
    }
  }

  private void commitTransaction(DorisCommittable committable) throws IOException {
    int retry = 0;
    String hostPort = committable.getHostPort();
    CloseableHttpResponse response = null;
    while (retry++ <= maxRetry) {
      HttpPutBuilder putBuilder = new HttpPutBuilder();
      putBuilder.setUrl(String.format(COMMIT_PATTERN, hostPort, committable.getDb()))
          .baseAuth(dorisOptions.getUsername(), dorisOptions.getPassword())
          .addCommonHeader()
          .addTxnId(committable.getTxnID())
          .setEmptyEntity()
          .commit();

      try {
        response = httpClient.execute(putBuilder.build());
      } catch (IOException e) {
        LOG.error("commit transaction failed: ", e);
        hostPort = RestService.getBackend(dorisOptions, executionOptions, LOG);
        continue;
      }
      if (response.getStatusLine().getStatusCode() == SC_OK) {
        break;
      }
      LOG.warn("commit failed with {}, reason {}", hostPort, response.getStatusLine().getReasonPhrase());
      hostPort = RestService.getBackend(dorisOptions, executionOptions, LOG);
    }

    assert response != null;
    if (response.getStatusLine().getStatusCode() != SC_OK) {
      LOG.warn("stream load error, reason={}", response.getStatusLine().getReasonPhrase());
      throw new BitSailException(DorisErrorCode.LOAD_FAILED, "stream load error: " + response.getStatusLine().getReasonPhrase());
    }

    ObjectMapper mapper = new ObjectMapper();
    if (response.getEntity() != null) {
      String loadResult = EntityUtils.toString(response.getEntity());
      Map<String, String> res = mapper.readValue(loadResult, new TypeReference<HashMap<String, String>>() {
      });
      if (res.get("status").equals(FAIL) && !ResponseUtil.isCommitted(res.get("msg"))) {
        throw new BitSailException(DorisErrorCode.COMMIT_FAILED, "Commit failed " + loadResult);
      }
      LOG.info("load result {}", loadResult);
    }
  }

  public void close() throws IOException {
    if (null != httpClient) {
      try {
        httpClient.close();
      } catch (IOException e) {
        LOG.error("Closing httpClient failed.", e);
        throw new RuntimeException("Closing httpClient failed.", e);
      }
    }
  }

}

