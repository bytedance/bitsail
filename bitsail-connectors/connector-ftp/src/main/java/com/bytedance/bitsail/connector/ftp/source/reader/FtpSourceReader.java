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

package com.bytedance.bitsail.connector.ftp.source.reader;

import com.bytedance.bitsail.base.connector.reader.v1.SourcePipeline;
import com.bytedance.bitsail.base.connector.reader.v1.SourceReader;
import com.bytedance.bitsail.base.format.DeserializationSchema;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.connector.ftp.core.client.FtpHandlerFactory;
import com.bytedance.bitsail.connector.ftp.core.client.IFtpHandler;
import com.bytedance.bitsail.connector.ftp.core.config.FtpConfig;
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
  private final transient DeserializationSchema<byte[], Row> deserializationSchema;

  private FtpSourceSplit currentSplit;
  private long currentReadCount;
  private transient String line;

  private FtpConfig ftpConfig;

  private boolean skipFirstLine = false;
  private final transient Context context;

  public FtpSourceReader(BitSailConfiguration jobConf, Context context, int subTaskId) {
    this.subTaskId = subTaskId;
    this.ftpConfig = new FtpConfig(jobConf);
    this.ftpHandler = FtpHandlerFactory.createFtpHandler(ftpConfig);
    this.splits = new LinkedList<>();
    this.context = context;
    this.deserializationSchema = DeserializationSchemaFactory.createDeserializationSchema(jobConf, ftpConfig, context);
    LOG.info("FtpReader is initialized.");

  }

  @Override
  public void start() {
    this.ftpHandler.loginFtpServer();
    if (this.ftpHandler.getFtpConfig().getSkipFirstLine()) {
      this.skipFirstLine = true;
    }
  }

  @Override
  public void pollNext(SourcePipeline<Row> pipeline) throws Exception {
    if (line == null && br != null) {
      br.close();
      br = null;
      ftpHandler.logoutFtpServer();
      LOG.info("Task {} finishes reading {} rows from split: {}", subTaskId, currentReadCount, currentSplit.uniqSplitId());
    }
    if (line == null && splits.isEmpty()) {
      return;
    }
    if (line == null) {
      this.currentSplit = splits.poll();
      LOG.info("Task {} begins to read split: {}={},fileSize is {}", subTaskId, currentSplit.uniqSplitId(), currentSplit.getPath(), currentSplit.getFileSize());
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
    if (line != null) {
      Row row = deserializationSchema.deserialize(line.getBytes());
      pipeline.output(row);
      line = br.readLine();
      currentReadCount++;
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
    if (hasNoMoreSplits && splits.isEmpty() && br == null) {
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
