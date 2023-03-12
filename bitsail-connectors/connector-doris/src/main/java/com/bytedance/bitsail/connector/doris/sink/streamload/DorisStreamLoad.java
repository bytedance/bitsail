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

package com.bytedance.bitsail.connector.doris.sink.streamload;

import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.model.ColumnInfo;
import com.bytedance.bitsail.common.util.Preconditions;
import com.bytedance.bitsail.connector.doris.config.DorisExecutionOptions;
import com.bytedance.bitsail.connector.doris.config.DorisOptions;
import com.bytedance.bitsail.connector.doris.error.DorisErrorCode;
import com.bytedance.bitsail.connector.doris.http.HttpPutBuilder;
import com.bytedance.bitsail.connector.doris.http.HttpUtil;
import com.bytedance.bitsail.connector.doris.http.ResponseUtil;
import com.bytedance.bitsail.connector.doris.http.model.RespContent;
import com.bytedance.bitsail.connector.doris.partition.DorisPartition;
import com.bytedance.bitsail.connector.doris.rest.RestService;
import com.bytedance.bitsail.connector.doris.sink.label.LabelGenerator;
import com.bytedance.bitsail.connector.doris.sink.record.RecordStream;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

import static com.bytedance.bitsail.connector.doris.http.ResponseUtil.LABEL_EXIST_PATTERN;
import static com.bytedance.bitsail.connector.doris.sink.streamload.LoadStatus.LABEL_ALREADY_EXIST;
import static com.bytedance.bitsail.connector.doris.sink.streamload.LoadStatus.SUCCESS;
import static org.apache.commons.httpclient.HttpStatus.SC_OK;

public class DorisStreamLoad {
  private static final Logger LOG = LoggerFactory.getLogger(DorisStreamLoad.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private static final String STREAM_LOAD_URL_FORMAT = "http://%s/api/%s/%s/_stream_load";
  private static final String ABORT_URL_PATTERN = "http://%s/api/%s/_stream_load_2pc";
  private static final int SUCCESS_STATUS_CODE = 200;
  private LabelGenerator labelGenerator;
  private final String jobExistFinished = "FINISHED";
  private String userName;
  private String password;
  private String abortUrlStr;
  private byte[] lineDelimiter;
  private boolean loadBatchFirstRecord;
  private String hostPort;
  private RecordStream recordStream;
  private boolean enable2PC;
  private String loadUrlStr;
  protected DorisExecutionOptions executionOptions;
  protected DorisOptions dorisOptions;
  protected String authEncoding;
  protected transient CloseableHttpClient httpClient;
  private Future<CloseableHttpResponse> pendingLoadFuture;
  private ExecutorService executorService;

  public DorisStreamLoad(DorisExecutionOptions executionOptions, DorisOptions dorisOptions, LabelGenerator labelGenerator, RecordStream recordStream) {
    this.hostPort = RestService.getBackend(dorisOptions, executionOptions, LOG);
    this.executionOptions = executionOptions;
    this.userName = dorisOptions.getUsername();
    this.password = dorisOptions.getPassword();
    this.dorisOptions = dorisOptions;
    this.labelGenerator = labelGenerator;
    this.authEncoding = basicAuthHeader(dorisOptions.getUsername(), dorisOptions.getPassword());
    this.lineDelimiter = dorisOptions.getLineDelimiter().getBytes(StandardCharsets.UTF_8);
    this.httpClient = new HttpUtil().getHttpClient();
    this.recordStream = recordStream;
    this.enable2PC = executionOptions.isEnable2PC();
    this.loadUrlStr = String.format(STREAM_LOAD_URL_FORMAT, hostPort, dorisOptions.getDatabaseName(), dorisOptions.getTableName());
    this.abortUrlStr = String.format(ABORT_URL_PATTERN, hostPort, dorisOptions.getDatabaseName());
    loadBatchFirstRecord = true;
    this.executorService = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(),
        new BasicThreadFactory.Builder().namingPattern("stream-load").daemon(true).build());
  }

  /**
   * try to discard pending transactions with labels beginning with labelSuffix.
   *
   * @param labelSuffix
   * @param chkID
   * @throws Exception
   */
  public void abortPreCommit(String labelSuffix, long chkID) throws Exception {
    long startChkID = chkID;
    LOG.info("abort for labelSuffix {}. start chkId {}.", labelSuffix, chkID);
    while (true) {
      try {
        String label = labelGenerator.generateLabel(startChkID);
        HttpPutBuilder builder = new HttpPutBuilder();
        builder.setUrl(loadUrlStr)
            .baseAuth(userName, password)
            .addCommonHeader()
            .enable2PC()
            .setLabel(label)
            .setEmptyEntity()
            .addProperties(executionOptions.getStreamLoadProp());
        RespContent respContent = handlePreCommitResponse(httpClient.execute(builder.build()));
        Preconditions.checkState("true".equals(respContent.getTwoPhaseCommit()));
        if (LABEL_ALREADY_EXIST.equals(respContent.getStatus())) {
          // label already exist and job finished
          if (jobExistFinished.equals(respContent.getExistingJobStatus())) {
            throw new BitSailException(DorisErrorCode.LABEL_ALREADY_EXIST,
                "Label already exist and load job finished, change you label prefix or restore from latest savepoint!");
          }
          // job not finished, abort.
          Matcher matcher = LABEL_EXIST_PATTERN.matcher(respContent.getMessage());
          if (matcher.find()) {
            Preconditions.checkState(label.equals(matcher.group(1)));
            long txnId = Long.parseLong(matcher.group(2));
            LOG.info("abort {} for exist label {}", txnId, label);
            abortTransaction(txnId);
          } else {
            LOG.error("response: {}", respContent.toString());
            throw new BitSailException(DorisErrorCode.LABEL_ALREADY_EXIST, "Label already exist, but no txnID associated with it");
          }
        } else {
          LOG.info("abort {} for check label {}.", respContent.getTxnId(), label);
          abortTransaction(respContent.getTxnId());
          break;
        }
        startChkID++;
      } catch (Exception e) {
        LOG.warn("failed to stream load data", e);
        throw e;
      }
    }
    LOG.info("abort for labelSuffix {} finished", labelSuffix);
  }

  public void abortTransaction(long txnID) throws Exception {
    HttpPutBuilder builder = new HttpPutBuilder();
    builder.setUrl(abortUrlStr)
        .baseAuth(userName, password)
        .addCommonHeader()
        .addTxnId(txnID)
        .setEmptyEntity()
        .abort();
    CloseableHttpResponse response = httpClient.execute(builder.build());

    int statusCode = response.getStatusLine().getStatusCode();
    if (statusCode != SC_OK || response.getEntity() == null) {
      LOG.warn("abort transaction response: " + response.getStatusLine().toString());
      throw new BitSailException(DorisErrorCode.ABORT_FAILED, "Fail to abort transaction " + txnID + " with url " + abortUrlStr);
    }

    ObjectMapper mapper = new ObjectMapper();
    String loadResult = EntityUtils.toString(response.getEntity());
    Map<String, String> res = mapper.readValue(loadResult, new TypeReference<HashMap<String, String>>() {
    });
    if (!SUCCESS.equals(res.get("status"))) {
      if (ResponseUtil.isCommitted(res.get("msg"))) {
        throw new BitSailException(DorisErrorCode.ABORT_FAILED, "try abort committed transaction, do you recover from old savepoint?");
      }
      LOG.warn("Fail to abort transaction. txnId: {}, error: {}", txnID, res.get("msg"));
    }
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
   * @param label
   * @throws IOException
   */
  public void startLoad(String label) throws IOException {
    loadBatchFirstRecord = true;
    HttpPutBuilder putBuilder = new HttpPutBuilder();
    recordStream.startInput();
    LOG.info("stream load started for {}", label);
    InputStreamEntity entity = new InputStreamEntity(recordStream);
    putBuilder.setUrl(loadUrlStr)
        .baseAuth(userName, password)
        .addCommonHeader()
        .setFormat(dorisOptions)
        .setLabel(label)
        .addHiddenColumns(executionOptions.getEnableDelete())
        .setEntity(entity)
        .addProperties(executionOptions.getStreamLoadProp());
    if (enable2PC) {
      putBuilder.enable2PC();
    }
    try {
      pendingLoadFuture = executorService.submit(() -> {
        LOG.info("start execute load");
        return httpClient.execute(putBuilder.build());
      });
    } catch (Exception e) {
      String err = "failed to stream load data with label: " + label;
      LOG.warn(err, e);
      throw e;
    }
  }

  public RespContent handlePreCommitResponse(CloseableHttpResponse response) throws Exception {
    final int statusCode = response.getStatusLine().getStatusCode();
    if (statusCode == SC_OK && response.getEntity() != null) {
      String loadResult = EntityUtils.toString(response.getEntity());
      LOG.info("load Result {}", loadResult);
      return OBJECT_MAPPER.readValue(loadResult, RespContent.class);
    }
    throw new BitSailException(DorisErrorCode.LOAD_FAILED, "stream load error: " + response.getStatusLine().toString());
  }

  public RespContent stopLoad() throws IOException {
    recordStream.endInput();
    LOG.info("stream load stopped.");
    Preconditions.checkState(pendingLoadFuture != null);
    try {
      return handlePreCommitResponse(pendingLoadFuture.get());
    } catch (Exception e) {
      throw BitSailException.asBitSailException(DorisErrorCode.PARSE_FAILED, "failed to stop load", e);
    }
  }

  public void load(String value, DorisOptions options, boolean isTemp) throws BitSailException {
    LoadResponse loadResponse = loadBatch(value, options, isTemp);
    LOG.info("StreamLoad Response:{}", loadResponse);
    if (loadResponse.status != SUCCESS_STATUS_CODE) {
      throw new BitSailException(DorisErrorCode.LOAD_FAILED, "stream load error: " + loadResponse.respContent);
    } else {
      try {
        RespContent respContent = OBJECT_MAPPER.readValue(loadResponse.respContent, RespContent.class);
        if (!SUCCESS.equals(respContent.getStatus())) {
          String errMsg = String.format("stream load error: %s, see more in %s, load value string: %s",
              respContent.getMessage(), respContent.getErrorURL(), value);
          throw new BitSailException(DorisErrorCode.LOAD_FAILED, errMsg);
        }
      } catch (IOException e) {
        throw new BitSailException(DorisErrorCode.LOAD_FAILED, e.getMessage());
      }
    }
  }

  private LoadResponse loadBatch(String value, DorisOptions options, boolean isTemp) {
    String label = executionOptions.getStreamLoadProp().getProperty("label");
    if (StringUtils.isBlank(label)) {
      SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd_HHmmss");
      String formatDate = sdf.format(new Date());
      label = String.format("bitsail_doris_connector_%s_%s", formatDate,
          UUID.randomUUID().toString().replaceAll("-", ""));
    }

    try {
      HttpPut put = new HttpPut(getStreamLoadUrl(isTemp));
      if (options.getLoadDataFormat().equals(DorisOptions.LOAD_CONTENT_TYPE.JSON)) {
        put.setHeader("format", "json");
        put.setHeader("strip_outer_array", "true");
      } else if (options.getLoadDataFormat().equals(DorisOptions.LOAD_CONTENT_TYPE.CSV)) {
        put.setHeader("format", "csv");
        put.setHeader("column_separator", options.getFieldDelimiter());
      }

      if (isTemp && dorisOptions.isTableHasPartitions()) {
        String tempPartitions = dorisOptions.getPartitions().stream().map(DorisPartition::getTempName).collect(Collectors.joining(","));
        put.setHeader("temporary_partitions", tempPartitions);
      }
      if (executionOptions.isEnable2PC()) {
        put.setHeader("two_phase_commit", "true");
      }

      put.setHeader(HttpHeaders.EXPECT, "100-continue");
      //set column meta info
      List<String> columnNames = new ArrayList<>();
      for (ColumnInfo columnInfo : options.getColumnInfos()) {
        columnNames.add(columnInfo.getName());
      }
      put.setHeader("columns", String.join(",", columnNames));
      put.setHeader(HttpHeaders.AUTHORIZATION, this.authEncoding);
      put.setHeader("label", label);
      for (Map.Entry<Object, Object> entry : executionOptions.getStreamLoadProp().entrySet()) {
        put.setHeader(String.valueOf(entry.getKey()), String.valueOf(entry.getValue()));
      }
      StringEntity entity = new StringEntity(value, "UTF-8");
      put.setEntity(entity);

      try (CloseableHttpResponse response = httpClient.execute(put)) {
        final int statusCode = response.getStatusLine().getStatusCode();
        final String reasonPhrase = response.getStatusLine().getReasonPhrase();
        String loadResult = "";
        if (response.getEntity() != null) {
          loadResult = EntityUtils.toString(response.getEntity());
        }
        return new LoadResponse(statusCode, reasonPhrase, loadResult);
      }
    } catch (Exception e) {
      String err = "failed to stream load data with label: " + label;
      LOG.warn(err, e);
      return new LoadResponse(-1, e.getMessage(), err);
    }
  }

  private String getStreamLoadUrl(boolean isTemp) {
    String tableName = dorisOptions.getTableName();
    if (isTemp && !dorisOptions.isTableHasPartitions()) {
      tableName = dorisOptions.getTmpTableName();
    }
    return String.format(STREAM_LOAD_URL_FORMAT, hostPort, dorisOptions.getDatabaseName(), tableName);
  }

  protected String basicAuthHeader(String username, String password) {
    final String tobeEncode = username + ":" + password;
    byte[] encoded = Base64.encodeBase64(tobeEncode.getBytes(StandardCharsets.UTF_8));
    return "Basic " + new String(encoded);
  }

  public String getHostPort() {
    return hostPort;
  }

  public void setHostPort(String hostPort) {
    this.hostPort = hostPort;
    this.loadUrlStr = String.format(STREAM_LOAD_URL_FORMAT, hostPort, dorisOptions.getDatabaseName(), dorisOptions.getTableName());
  }

  @VisibleForTesting
  public DorisStreamLoad() {
  }

  public static class LoadResponse {
    public int status;
    public String respMsg;
    public String respContent;

    public LoadResponse(int status, String respMsg, String respContent) {
      this.status = status;
      this.respMsg = respMsg;
      this.respContent = respContent;
    }

    @Override
    public String toString() {
      try {
        return OBJECT_MAPPER.writeValueAsString(this);
      } catch (JsonProcessingException e) {
        return "";
      }
    }
  }
}

