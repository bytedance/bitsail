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

package com.bytedance.bitsail.connector.selectdb.sink.uploadload;

import com.bytedance.bitsail.common.util.Preconditions;
import com.bytedance.bitsail.connector.selectdb.config.SelectdbExecutionOptions;
import com.bytedance.bitsail.connector.selectdb.config.SelectdbOptions;
import com.bytedance.bitsail.connector.selectdb.http.HttpPutBuilder;
import com.bytedance.bitsail.connector.selectdb.http.HttpUtil;
import com.bytedance.bitsail.connector.selectdb.http.model.BaseResponse;
import com.bytedance.bitsail.connector.selectdb.sink.record.RecordStream;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.apache.commons.httpclient.HttpStatus.SC_OK;

public class SelectdbUploadLoad {
  private static final Logger LOG = LoggerFactory.getLogger(SelectdbUploadLoad.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final String UPLOAD_URL_PATTERN = "http://%s/copy/upload";
  private static final int UPLOAD_SUCCESS_CODE = 307;
  private String uploadUrl;
  private String userName;
  private String password;
  private byte[] lineDelimiter;
  private boolean loadBatchFirstRecord;
  private String hostPort;
  private RecordStream recordStream;
  protected SelectdbExecutionOptions executionOptions;
  protected SelectdbOptions selectdbOptions;
  protected transient CloseableHttpClient httpClient;
  private Future<CloseableHttpResponse> pendingLoadFuture;
  private ExecutorService executorService;
  private String fileName;
  private final List<String> fileList = new CopyOnWriteArrayList<>();

  public SelectdbUploadLoad(SelectdbExecutionOptions executionOptions, SelectdbOptions selectdbOptions) {
    this.hostPort = selectdbOptions.getLoadUrl();
    this.executionOptions = executionOptions;
    this.uploadUrl = String.format(UPLOAD_URL_PATTERN, hostPort);
    this.userName = selectdbOptions.getUsername();
    this.password = selectdbOptions.getPassword();
    this.selectdbOptions = selectdbOptions;
    this.lineDelimiter = selectdbOptions.getLineDelimiter().getBytes(StandardCharsets.UTF_8);
    this.httpClient = new HttpUtil().getHttpClient();
    this.recordStream = new RecordStream(this.executionOptions.getBufferSize(), this.executionOptions.getBufferCount());
    loadBatchFirstRecord = true;
    this.executorService = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(),
        new BasicThreadFactory.Builder().namingPattern("stream-load").daemon(true).build());
  }

  public void writeRecord(byte[] record) throws IOException {
    if (loadBatchFirstRecord) {
      loadBatchFirstRecord = false;
    } else {
      recordStream.write(lineDelimiter);
    }
    recordStream.write(record);
  }

  /**
   * start write data for new checkpoint.
   *
   * @param fileName
   * @throws IOException
   */
  public void startLoad(String fileName) throws IOException {
    this.fileName = fileName;
    loadBatchFirstRecord = true;
    recordStream.startInput();
    LOG.info("file write started for {}", fileName);
    try {
      String address = getUploadAddress(fileName);
      fileList.add(fileName);
      LOG.info("redirect to s3 address:{}", address);
      InputStreamEntity entity = new InputStreamEntity(recordStream);
      HttpPutBuilder putBuilder = new HttpPutBuilder();
      putBuilder.setUrl(address)
          .addCommonHeader()
          .setEntity(entity);
      pendingLoadFuture = executorService.submit(() -> {
        LOG.info("start execute load {}", fileName);
        return httpClient.execute(putBuilder.build());
      });
    } catch (Exception e) {
      String err = "failed to write data with fileName: " + fileName;
      LOG.warn(err, e);
      throw e;
    }
  }

  /**
   * Get the redirected s3 address
   */
  public String getUploadAddress(String fileName) throws IOException {
    HttpPutBuilder putBuilder = new HttpPutBuilder();
    putBuilder.setUrl(uploadUrl)
        .addFileName(fileName)
        .addCommonHeader()
        .setEmptyEntity()
        .baseAuth(userName, password);

    try (CloseableHttpResponse execute = httpClient.execute(putBuilder.build())) {
      int statusCode = execute.getStatusLine().getStatusCode();
      String reason = execute.getStatusLine().getReasonPhrase();
      if (statusCode == UPLOAD_SUCCESS_CODE) {
        Header location = execute.getFirstHeader("location");
        return location.getValue();
      } else {
        HttpEntity entity = execute.getEntity();
        String result = entity == null ? null : EntityUtils.toString(entity);
        LOG.error("Failed get the redirected address, status {}, reason {}, response {}", statusCode, reason, result);
        throw new RuntimeException("Could not get the redirected address.");
      }
    }
  }

  public void stopLoad() throws IOException {
    recordStream.endInput();
    LOG.info("file {} write stopped.", fileName);
    Preconditions.checkState(pendingLoadFuture != null);
    try {
      handleResponse(pendingLoadFuture.get());
      LOG.info("upload file {} finished", fileName);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public BaseResponse<HashMap<String, String>> handleResponse(CloseableHttpResponse response) throws IOException {
    try {
      final int statusCode = response.getStatusLine().getStatusCode();
      if (statusCode == SC_OK && response.getEntity() != null) {
        String loadResult = EntityUtils.toString(response.getEntity());
        if (StringUtils.isEmpty(loadResult) || StringUtils.isBlank(loadResult)) {
          return null;
        }
        LOG.info("response result {}", loadResult);
        BaseResponse<HashMap<String, String>> baseResponse = OBJECT_MAPPER.readValue(loadResult, new TypeReference<BaseResponse<HashMap<String, String>>>() {
        });
        if (baseResponse.getCode() == 0) {
          return baseResponse;
        } else {
          throw new RuntimeException("upload file error: " + baseResponse.getMsg());
        }
      }
      throw new RuntimeException("upload file error: " + response.getStatusLine().toString());
    } finally {
      if (response != null) {
        response.close();
      }
    }
  }

  public List<String> getFileList() {
    return fileList;
  }

  public void clearFileList() {
    fileList.clear();
  }

  public String getHostPort() {
    return hostPort;
  }

  public void setHostPort(String hostPort) {
    this.hostPort = hostPort;
  }

  @VisibleForTesting
  public SelectdbUploadLoad() {
  }
}

