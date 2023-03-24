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

package com.bytedance.bitsail.connector.larksheet.source.coordinate;

import com.bytedance.bitsail.base.connector.reader.v1.SourceSplitCoordinator;
import com.bytedance.bitsail.base.connector.writer.v1.state.EmptyState;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.connector.larksheet.api.SheetConfig;
import com.bytedance.bitsail.connector.larksheet.error.LarkSheetFormatErrorCode;
import com.bytedance.bitsail.connector.larksheet.meta.SheetInfo;
import com.bytedance.bitsail.connector.larksheet.option.LarkSheetReaderOptions;
import com.bytedance.bitsail.connector.larksheet.source.split.LarkSheetSplit;
import com.bytedance.bitsail.connector.larksheet.source.split.strategy.SimpleDivideSplitConstructor;
import com.bytedance.bitsail.connector.larksheet.util.LarkSheetUtil;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.NoArgsConstructor;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.bytedance.bitsail.connector.larksheet.constant.LarkSheetConstant.SPLIT_COMMA;

public class LarkSheetSourceSplitCoordinator implements SourceSplitCoordinator<LarkSheetSplit, EmptyState> {

  private static final Logger LOG = LoggerFactory.getLogger(LarkSheetSourceSplitCoordinator.class);

  private final BitSailConfiguration jobConf;

  private final SourceSplitCoordinator.Context<LarkSheetSplit, EmptyState> context;

  /**
   * the list of sheet information
   */
  private List<SheetInfo> sheetInfoList;

  /**
   * Sheet configurations for request.
   */
  private SheetConfig larkSheetConfig;

  /**
   * Number of rows queried in batch.
   */
  private int batchSize;

  /**
   * Number of lines to skip for each sheet.
   */
  private List<Integer> skipNums;

  private final Map<Integer, Set<LarkSheetSplit>> pendingSplitAssignment;

  public LarkSheetSourceSplitCoordinator(BitSailConfiguration jobConf,
                                         Context<LarkSheetSplit, EmptyState> context) {
    this.jobConf = jobConf;
    this.context = context;

    this.pendingSplitAssignment = Maps.newConcurrentMap();

    prepareConfig();
  }

  public void prepareConfig() {
    // Initialize open api related configuration
    this.larkSheetConfig = new SheetConfig().configure(this.jobConf);

    // Parse sheet url
    String sheetUrlList = this.jobConf.getNecessaryOption(LarkSheetReaderOptions.SHEET_URL,
        LarkSheetFormatErrorCode.REQUIRED_VALUE);
    List<String> sheetUrls = Arrays.asList(sheetUrlList.split(SPLIT_COMMA));

    // Initialize sheet info
    this.sheetInfoList = LarkSheetUtil.resolveSheetUrls(sheetUrls);

    // Other initialization
    this.batchSize = this.jobConf.get(LarkSheetReaderOptions.BATCH_SIZE);
    this.skipNums = this.jobConf.getUnNecessaryOption(LarkSheetReaderOptions.SKIP_NUMS, new ArrayList<>());
  }

  @Override
  public void start() {

    SimpleDivideSplitConstructor splitConstructor = new SimpleDivideSplitConstructor(sheetInfoList, batchSize, skipNums);
    List<LarkSheetSplit> splitList = splitConstructor.construct();

    int readerNum = context.totalParallelism();
    LOG.info("Found {} readers and {} splits.", readerNum, splitList.size());
    if (readerNum > splitList.size()) {
      LOG.error("Reader number {} is larger than split number {}.", readerNum, splitList.size());
    }

    for (LarkSheetSplit split : splitList) {
      int readerIndex = ReaderSelector.getReaderIndex(readerNum);
      pendingSplitAssignment.computeIfAbsent(readerIndex, r -> new HashSet<>()).add(split);
      LOG.info("Assign Split {} to Reader {}.", split.uniqSplitId(), readerIndex);
    }
  }

  @Override
  public void addReader(int subtaskId) {
    LOG.info("Add reader {} to LarkSheet Split Coordinator.", subtaskId);
    tryAssignSplitsToReader();
  }

  @Override
  public void addSplitsBack(List<LarkSheetSplit> splits, int subtaskId) {
    LOG.info("Source reader {} return splits {}.", subtaskId, splits);

    int readerNum = context.totalParallelism();
    for (LarkSheetSplit split : splits) {
      int readerIndex = ReaderSelector.getReaderIndex(readerNum);
      pendingSplitAssignment.computeIfAbsent(readerIndex, r -> new HashSet<>()).add(split);
      LOG.info("Re-assign split {} to reader {}.", split.uniqSplitId(), readerIndex);
    }

    tryAssignSplitsToReader();
  }

  private void tryAssignSplitsToReader() {
    Map<Integer, List<LarkSheetSplit>> splitsToAssign = new HashMap<>();

    for (Integer readerIndex : pendingSplitAssignment.keySet()) {
      if (CollectionUtils.isNotEmpty(pendingSplitAssignment.get(readerIndex)) && context.registeredReaders().contains(readerIndex)) {
        Set<LarkSheetSplit> larkSheetSplits = pendingSplitAssignment.get(readerIndex);
        splitsToAssign.put(readerIndex, Lists.newArrayList(larkSheetSplits));
      }
    }

    for (Integer readerIndex : splitsToAssign.keySet()) {
      LOG.info("Try to assign splits reader {}, splits are: [{}].", readerIndex,
          splitsToAssign.get(readerIndex).stream().map(LarkSheetSplit::uniqSplitId).collect(Collectors.toList()));

      pendingSplitAssignment.remove(readerIndex);
      context.assignSplit(readerIndex, splitsToAssign.get(readerIndex));
      context.signalNoMoreSplits(readerIndex);
      LOG.info("Finish assigning splits reader {}.", readerIndex);
    }
  }

  @Override
  public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
    // empty
  }

  @Override
  public EmptyState snapshotState(long checkpointId) throws Exception {
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
