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

package com.bytedance.bitsail.connector.ftp.source.split.coordinator;

import com.bytedance.bitsail.base.connector.reader.v1.SourceSplitCoordinator;
import com.bytedance.bitsail.base.connector.writer.v1.state.EmptyState;
import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.connector.ftp.core.client.FtpHandlerFactory;
import com.bytedance.bitsail.connector.ftp.core.client.IFtpHandler;
import com.bytedance.bitsail.connector.ftp.core.config.FtpConfig;
import com.bytedance.bitsail.connector.ftp.error.FtpErrorCode;
import com.bytedance.bitsail.connector.ftp.source.split.FtpSourceSplit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class FtpSourceSplitCoordinator implements SourceSplitCoordinator<FtpSourceSplit, EmptyState> {
  private static final Logger LOG = LoggerFactory.getLogger(FtpSourceSplitCoordinator.class);

  private final SourceSplitCoordinator.Context<FtpSourceSplit, EmptyState> context;
  private final BitSailConfiguration jobConf;
  private final Map<Integer, Set<FtpSourceSplit>> splitAssignmentPlan;
  protected long fileSize;
  private FtpConfig ftpConfig;
  private final IFtpHandler ftpHandler;
  @Getter
  private List<FtpSourceSplit> splitList;

  public FtpSourceSplitCoordinator(Context<FtpSourceSplit, EmptyState> context,
                                      BitSailConfiguration jobConf) {
    this.context = context;
    this.jobConf = jobConf;
    this.splitAssignmentPlan = Maps.newConcurrentMap();
    this.ftpConfig = new FtpConfig(jobConf);
    this.ftpHandler = FtpHandlerFactory.createFtpHandler(this.ftpConfig);
  }

  @Override
  public void start() {
    try {
      splitList = constructSplit();
    } catch (IOException e) {
      throw new BitSailException(FtpErrorCode.SPLIT_ERROR, "Failed to create splits.");
    }

    int readerNum = context.totalParallelism();
    LOG.info("Found {} readers and {} splits.", readerNum, splitList.size());
    if (readerNum > splitList.size()) {
      LOG.error("Reader number {} is larger than split number {}.", readerNum, splitList.size());
    }

    for (FtpSourceSplit split : splitList) {
      int readerIndex = ReaderSelector.getReaderIndex(readerNum);
      splitAssignmentPlan.computeIfAbsent(readerIndex, k -> new HashSet<>()).add(split);
      LOG.info("Will assign split {} to the {}-th reader", split.uniqSplitId(), readerIndex);
    }
  }

  @Override
  public void addReader(int subtaskId) {
    LOG.info("Found reader {}", subtaskId);
    tryAssignSplitsToReader();
  }

  @Override
  public void addSplitsBack(List<FtpSourceSplit> splits, int subtaskId) {
    LOG.info("Source reader {} return splits {}.", subtaskId, splits);

    int readerNum = context.totalParallelism();
    for (FtpSourceSplit split : splits) {
      int readerIndex = ReaderSelector.getReaderIndex(readerNum);
      splitAssignmentPlan.computeIfAbsent(readerIndex, k -> new HashSet<>()).add(split);
      LOG.info("Re-assign split {} to the {}-th reader.", split.uniqSplitId(), readerIndex);
    }

    tryAssignSplitsToReader();
  }

  private void tryAssignSplitsToReader() {
    Map<Integer, List<FtpSourceSplit>> splitsToAssign = new HashMap<>();

    for (Integer readerIndex : splitAssignmentPlan.keySet()) {
      if (CollectionUtils.isNotEmpty(splitAssignmentPlan.get(readerIndex)) && context.registeredReaders().contains(readerIndex)) {
        splitsToAssign.put(readerIndex, Lists.newArrayList(splitAssignmentPlan.get(readerIndex)));
      }
    }

    for (Integer readerIndex : splitsToAssign.keySet()) {
      LOG.info("Try assigning splits reader {}, splits are: [{}]", readerIndex,
              splitsToAssign.get(readerIndex).stream().map(FtpSourceSplit::getSplitId).collect(Collectors.toList()));
      splitAssignmentPlan.remove(readerIndex);
      context.assignSplit(readerIndex, splitsToAssign.get(readerIndex));
      context.signalNoMoreSplits(readerIndex);
      LOG.info("Finish assigning splits reader {}", readerIndex);
    }
  }

  private List<FtpSourceSplit> constructSplit() throws IOException {
    List<FtpSourceSplit> splits = new ArrayList<>();
    this.ftpHandler.loginFtpServer();
    if (this.ftpConfig.getEnableSuccessFileCheck()) {
      checkSuccessFileExist();
    }
    String[] paths = ftpHandler.getFtpConfig().getPaths();
    int index = 0;
    //todo support regex
    for (String path : paths) {
      checkPathsExist(path);
      if (null != path && path.length() > 0) {
        path = path.replace("\n", "").replace("\r", "").trim();
        for (String f : ftpHandler.getFiles(path)) {
          FtpSourceSplit split = new FtpSourceSplit(index++);
          split.setPath(f);
          split.setFileSize(ftpHandler.getFilesSize(f));
          splits.add(split);
        }
      }
      this.fileSize += ftpHandler.getFilesSize(path);
    }

    int numSplits = splits.size();
    ftpHandler.logoutFtpServer();
    LOG.info("FtpSourceSplitCoordinator Input splits size: {}, total file size: {}", numSplits, this.fileSize);
    return splits;
  }

  private void checkPathsExist(String path) {
    if (!ftpHandler.isPathExist(path)) {
      throw BitSailException.asBitSailException(FtpErrorCode.FILEPATH_NOT_EXIST,
          "Given filePath is not exist: " + path + " , please check paths is correct");
    }
  }

  private void checkSuccessFileExist() {
    boolean fileExistFlag = false;
    int successFileRetryLeftTimes = ftpConfig.getMaxRetryTime();
    String successFilePath = ftpConfig.getSuccessFilePath();
    while (!fileExistFlag && successFileRetryLeftTimes-- > 0) {
      fileExistFlag = ftpHandler.isFileExist(successFilePath);
      if (!fileExistFlag) {
        LOG.info("SUCCESS tag file " + successFilePath + " is not exist, waiting for retry...");
        // wait for retry if not SUCCESS tag file
        try {
          Thread.sleep(ftpConfig.getSuccessFileRetryIntervalMs());
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
  public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
      // empty
  }

  @Override
  public EmptyState snapshotState(long checkpointId) {
    return new EmptyState();
  }

  @Override
  public void close() {
      // empty
  }

  @NoArgsConstructor
  static class ReaderSelector {
    private static long readerIndex = 0;
    public static int getReaderIndex(int totalReaderNum) {
      return (int) readerIndex++ % totalReaderNum;
    }
  }

  @VisibleForTesting
  public static int getReaderIndexForTest(int totalReaderNum) {
    return ReaderSelector.getReaderIndex(totalReaderNum);
  }
}
