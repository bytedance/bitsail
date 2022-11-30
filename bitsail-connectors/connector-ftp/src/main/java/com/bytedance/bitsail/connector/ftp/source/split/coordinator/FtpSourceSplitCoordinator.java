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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.NoArgsConstructor;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
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

  public FtpSourceSplitCoordinator(Context<FtpSourceSplit, EmptyState> context,
                                      BitSailConfiguration jobConf) {
    this.context = context;
    this.jobConf = jobConf;
    this.splitAssignmentPlan = Maps.newConcurrentMap();
  }

  @Override
  public void start() {
    List<FtpSourceSplit> splitList;
    try {
      IFtpHandler ftpHandler = FtpHandlerFactory.createFtpHandler(new FtpConfig(jobConf));
      splitList = construct(ftpHandler);
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

  private List<FtpSourceSplit> construct(IFtpHandler ftpHandler) throws IOException {
    List<FtpSourceSplit> splits = new ArrayList<>();
    ftpHandler.loginFtpServer();
    String[] paths = ftpHandler.getFtpConfig().getPaths();
    int index = 0;
    for (String path : paths) {
      if (null != path && path.length() > 0) {
        path = path.replace("\n", "").replace("\r", "");
        String[] pathArray = StringUtils.split(path, ",");
        for (String p : pathArray) {
          for (String f : ftpHandler.getFiles(p.trim())) {
            FtpSourceSplit split = new FtpSourceSplit(index++);
            split.setPath(f);
            splits.add(split);
          }
        }
      }
      this.fileSize += ftpHandler.getFilesSize(path);
    }

    int numSplits = splits.size();
    ftpHandler.logoutFtpServer();
    LOG.info("FtpSourceSplitCoordinator Input splits size: {}", numSplits);
    return splits;
  }

  @Override
  public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
      // empty
  }

  @Override
  public EmptyState snapshotState() {
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
}
