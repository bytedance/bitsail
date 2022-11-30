/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.bytedance.bitsail.connector.ftp.source.reader;

import com.bytedance.bitsail.base.connector.reader.v1.SourcePipeline;
import com.bytedance.bitsail.base.connector.reader.v1.SourceReader;
import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.connector.ftp.core.client.FtpHandlerFactory;
import com.bytedance.bitsail.connector.ftp.core.client.IFtpHandler;
import com.bytedance.bitsail.connector.ftp.core.config.FtpConfig;
import com.bytedance.bitsail.connector.ftp.error.FtpErrorCode;
import com.bytedance.bitsail.connector.ftp.source.reader.deserializer.ITextRowDeserializer;
import com.bytedance.bitsail.connector.ftp.source.reader.deserializer.TextRowDeserializerFactory;
import com.bytedance.bitsail.connector.ftp.source.split.FtpSourceSplit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;

public class FtpSourceReader implements SourceReader<Row, FtpSourceSplit> {
  private static final Logger LOG = LoggerFactory.getLogger(FtpSourceReader.class);

  private final int subTaskId;

  private final IFtpHandler ftpHandler;
  private transient BufferedReader br;
  private int totalSplitNum = 0;
  private boolean hasNoMoreSplits = false;

  private final Deque<FtpSourceSplit> splits;
  private final transient ITextRowDeserializer rowDeserializer;

  private FtpSourceSplit currentSplit;
  private long currentReadCount;
  private transient String line;

  private FtpConfig ftpConfig;

  private boolean skipFirstLine = false;
  private String successFilePath;

  public FtpSourceReader(BitSailConfiguration jobConf, int subTaskId) {
    this.subTaskId = subTaskId;
    this.ftpConfig = new FtpConfig(jobConf);
    this.ftpHandler = FtpHandlerFactory.createFtpHandler(ftpConfig);
    this.rowDeserializer = TextRowDeserializerFactory.createTextRowDeserializer(jobConf, ftpConfig);
    this.splits = new LinkedList<>();
    LOG.info("FtpReader is initialized.");

  }

  @Override
  public void start() {
    this.ftpHandler.loginFtpServer();
    checkPathsExist();
    if (this.ftpConfig.getEnableSuccessFileCheck()) {
      this.successFilePath = this.ftpConfig.getSuccessFilePath();
      checkSuccessFileExist();
    }
    if (this.ftpHandler.getFtpConfig().getSkipFirstLine()) {
      this.skipFirstLine = true;
    }
  }

  @Override
  public void pollNext(SourcePipeline<Row> pipeline) throws Exception {
    if (line == null && splits.isEmpty()) {
      return;
    }
    if (line == null) {
      this.currentSplit = splits.poll();
      LOG.info("Task {} begins to read split: {}={}", subTaskId, currentSplit.uniqSplitId(), currentSplit.getPath());
      this.currentReadCount = 0;
      ftpHandler.loginFtpServer();
      InputStream in = ftpHandler.getInputStream(this.currentSplit.getPath());
      br = new BufferedReader(new InputStreamReader(in, "utf-8"));
      if (this.skipFirstLine) {
        String skipLine = br.readLine();
        LOG.info("Skip line:{}", skipLine);
      }
      line = br.readLine();
    }
    while (line != null) {
      Row row = rowDeserializer.convert(line);
      pipeline.output(row);
      line = br.readLine();
      currentReadCount++;
    }

    br.close();
    br = null;
    ftpHandler.logoutFtpServer();
    LOG.info("Task {} finishes reading {} rows from split: {}", subTaskId, currentReadCount, currentSplit.uniqSplitId());
  }

  private void checkPathsExist() {
    for (String path : ftpConfig.getPaths()) {
      if (!ftpHandler.isPathExist(path)) {
        throw BitSailException.asBitSailException(FtpErrorCode.FILEPATH_NOT_EXIST,
                "Given filePath is not exist: " + path + " , please check paths is correct");
      }
    }
  }

  private void checkSuccessFileExist() {
    boolean fileExistFlag = false;
    int successFileRetryLeftTimes = ftpConfig.getMaxRetryTime();
    while (!fileExistFlag && successFileRetryLeftTimes-- > 0) {
      fileExistFlag = ftpHandler.isFileExist(this.successFilePath);
      if (!fileExistFlag) {
        LOG.info("SUCCESS tag file " + this.successFilePath + " is not exist, waiting for retry...");
        // wait for retry if not SUCCESS tag file
        try {
          Thread.sleep(ftpConfig.getRetryIntervalMs());
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    }
    if (!fileExistFlag) {
      throw BitSailException.asBitSailException(FtpErrorCode.SUCCESS_FILE_NOT_EXIST,
            "Success file is not ready after " + ftpConfig.getMaxRetryTime() + " retry, please wait upstream is ready");
    }
  }

  @Override
  public void addSplits(List<FtpSourceSplit> splitList) {
    totalSplitNum += splitList.size();
    splits.addAll(splitList);
  }

  @Override
  public void notifyNoMoreSplits() {
    this.hasNoMoreSplits = true;
    LOG.info("No more splits will be assigned.");
  }

  @Override
  public boolean hasMoreElements() {
    if (hasNoMoreSplits && splits.isEmpty() && line == null) {
      LOG.info("Finish reading all {} splits.", totalSplitNum);
      return false;
    }
    return true;
  }

  @Override
  public List<FtpSourceSplit> snapshotState(long checkpointId) {
    return Collections.emptyList();
  }

  @Override
  public void notifyCheckpointComplete(long checkpointId) {
    //Ignore
  }

  @Override
  public void close() throws Exception {
    if (null != this.ftpHandler) {
      this.ftpHandler.logoutFtpServer();
    }
  }
}
