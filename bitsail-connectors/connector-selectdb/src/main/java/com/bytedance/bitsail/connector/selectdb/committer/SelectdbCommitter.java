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

package com.bytedance.bitsail.connector.selectdb.committer;

import com.bytedance.bitsail.base.connector.writer.v1.WriterCommitter;
import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.connector.selectdb.config.SelectdbExecutionOptions;
import com.bytedance.bitsail.connector.selectdb.config.SelectdbOptions;
import com.bytedance.bitsail.connector.selectdb.error.SelectdbErrorCode;
import com.bytedance.bitsail.connector.selectdb.http.HttpPostBuilder;
import com.bytedance.bitsail.connector.selectdb.http.HttpUtil;
import com.bytedance.bitsail.connector.selectdb.http.ResponseUtil;
import com.bytedance.bitsail.connector.selectdb.http.model.BaseResponse;
import com.bytedance.bitsail.connector.selectdb.http.model.CopyIntoResp;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.NoArgsConstructor;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.bytedance.bitsail.connector.selectdb.sink.uploadload.LoadStatus.FAIL;
import static com.bytedance.bitsail.connector.selectdb.sink.uploadload.LoadStatus.SUCCESS;
import static org.apache.http.HttpStatus.SC_OK;

@NoArgsConstructor
public class SelectdbCommitter implements WriterCommitter<SelectdbCommittable> {
  private static final Logger LOG = LoggerFactory.getLogger(SelectdbCommitter.class);
  private static final String COMMIT_PATTERN = "http://%s/copy/query";
  private final ObjectMapper objectMapper = new ObjectMapper();
  private SelectdbOptions selectdbOptions;
  private int maxRetry;
  private transient CloseableHttpClient httpClient;

  public SelectdbCommitter(SelectdbOptions selectdbOptions, SelectdbExecutionOptions executionOptions) {
    this.selectdbOptions = selectdbOptions;
    this.maxRetry = executionOptions.getMaxRetries();
  }

  @Override
  public List<SelectdbCommittable> commit(List<SelectdbCommittable> committables) throws IOException {
    for (SelectdbCommittable committable : committables) {
      try {
        // TODO add a open method init it
        this.httpClient = new HttpUtil().getHttpClient();

        commitTransaction(committable);
      } finally {
        close();
      }
    }
    return Collections.emptyList();
  }

  private void commitTransaction(SelectdbCommittable committable) throws IOException {
    long start = System.currentTimeMillis();
    String hostPort = committable.getHostPort();
    String clusterName = committable.getClusterName();
    String copySQL = committable.getCopySQL();
    LOG.info("commit to cluster {} with copy sql: {}", clusterName, copySQL);

    int retry = 0;
    CloseableHttpResponse response = null;
    String loadResult = "";
    boolean success = false;
    String reasonPhrase = null;
    int statusCode = -1;
    while (retry++ <= maxRetry) {
      HttpPostBuilder postBuilder = new HttpPostBuilder();
      postBuilder.setUrl(String.format(COMMIT_PATTERN, hostPort))
          .addCommonHeader()
          .baseAuth(selectdbOptions.getUsername(), selectdbOptions.getPassword())
          .setEntity(new StringEntity(getHttpEntity(clusterName, copySQL)));

      try {
        response = httpClient.execute(postBuilder.build());
      } catch (IOException e) {
        LOG.error("commit error : ", e);
        continue;
      }
      statusCode = response.getStatusLine().getStatusCode();
      reasonPhrase = response.getStatusLine().getReasonPhrase();

      if (response.getStatusLine().getStatusCode() == SC_OK && Objects.nonNull(response.getEntity())) {
        loadResult = EntityUtils.toString(response.getEntity());
        success = handleCommitResponse(loadResult);
        if (success) {
          LOG.info("commit success cost {}ms, response is {}", System.currentTimeMillis() - start, loadResult);
          break;
        }
        LOG.warn("commit failed, retry again");
      } else {
        LOG.warn("commit failed with status={}, hostPort={}, reason={}", statusCode, hostPort, reasonPhrase);
      }
    }

    if (!success) {
      String errMsg = String.format("commit error with status=%s, reason=%s, response=%s", statusCode, reasonPhrase, loadResult);
      LOG.error(errMsg);
      throw new BitSailException(SelectdbErrorCode.COMMIT_FAILED, errMsg);
    }
  }

  public boolean handleCommitResponse(String loadResult) throws IOException {
    BaseResponse<CopyIntoResp> baseResponse = objectMapper.readValue(loadResult, new TypeReference<BaseResponse<CopyIntoResp>>() {
    });
    if (baseResponse.getCode() == SUCCESS) {
      CopyIntoResp dataResp = baseResponse.getData();
      if (FAIL.equals(dataResp.getDataCode())) {
        LOG.error("copy into execute failed, reason:{}", loadResult);
        return false;
      } else {
        Map<String, String> result = dataResp.getResult();
        if (!result.get("state").equals("FINISHED") && !ResponseUtil.isCommitted(result.get("msg"))) {
          LOG.error("copy into load failed, reason:{}", loadResult);
          return false;
        } else {
          return true;
        }
      }
    } else {
      LOG.error("commit failed, reason:{}", loadResult);
      return false;
    }
  }

  private String getHttpEntity(String clusterName, String copySQL) {
    Map<String, String> params = new HashMap<>();
    params.put("cluster", clusterName);
    params.put("sql", copySQL);
    try {
      return objectMapper.writeValueAsString(params);
    } catch (JsonProcessingException e) {
      String errMsg = "Failed to convert params as String. params=" + params + ", " + e;
      LOG.warn(errMsg);
      throw new BitSailException(SelectdbErrorCode.PARSE_FAILED, errMsg);
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

