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

package com.bytedance.bitsail.connector.larksheet.source.reader;

import com.bytedance.bitsail.base.connector.reader.v1.SourcePipeline;
import com.bytedance.bitsail.base.connector.reader.v1.SourceReader;
import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.model.ColumnInfo;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.common.typeinfo.TypeInfo;
import com.bytedance.bitsail.connector.larksheet.api.SheetConfig;
import com.bytedance.bitsail.connector.larksheet.error.LarkSheetFormatErrorCode;
import com.bytedance.bitsail.connector.larksheet.meta.SheetHeader;
import com.bytedance.bitsail.connector.larksheet.meta.SheetInfo;
import com.bytedance.bitsail.connector.larksheet.meta.SheetMeta;
import com.bytedance.bitsail.connector.larksheet.option.LarkSheetReaderOptions;
import com.bytedance.bitsail.connector.larksheet.source.split.LarkSheetSplit;
import com.bytedance.bitsail.connector.larksheet.util.LarkSheetUtil;

import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedDeque;

import static com.bytedance.bitsail.connector.larksheet.constant.LarkSheetConstant.SPLIT_COMMA;
import static com.bytedance.bitsail.connector.larksheet.util.LarkSheetUtil.genSheetRange;

public class LarkSheetReader implements SourceReader<Row, LarkSheetSplit> {

  private static final Logger LOG = LoggerFactory.getLogger(LarkSheetReader.class);

  private final BitSailConfiguration jobConf;

  private final SourceReader.Context readerContext;

  private final TypeInfo<?>[] typeInfos;

  /**
   * Sheet header info.
   */
  private final SheetHeader sheetHeader;

  /**
   * Parameters used when calling single range api.
   */
  private final Map<String, String> rangeParamMap;

  private boolean hasNoMoreSplits = false;

  private final Deque<LarkSheetSplit> splits;

  private final int subTaskId;

  private int totalSplitNum = 0;

  private boolean hasNextElements;

  /**
   * A record batch get from API. Empty rows are filtered.
   */
  private final List<List<Object>> recordQueue;

  /**
   * Current record to handle.
   */
  private List<Object> curRecord;

  public LarkSheetReader(BitSailConfiguration jobConf, Context readerContext) {
    this.jobConf = jobConf;
    this.readerContext = readerContext;
    this.typeInfos = readerContext.getRowTypeInfo().getTypeInfos();

    // Sheet configurations for request
    SheetConfig larkSheetConfig = new SheetConfig().configure(this.jobConf);
    // Parse sheet url
    String sheetUrlList = this.jobConf.getNecessaryOption(LarkSheetReaderOptions.SHEET_URL,
        LarkSheetFormatErrorCode.REQUIRED_VALUE);
    List<String> sheetUrls = Arrays.asList(sheetUrlList.split(SPLIT_COMMA));
    // the list of sheet information
    List<SheetInfo> sheetInfoList = LarkSheetUtil.resolveSheetUrls(sheetUrls);

    // Get and verify sheet header
    List<ColumnInfo> readerColumns = this.jobConf.getNecessaryOption(LarkSheetReaderOptions.COLUMNS,
        LarkSheetFormatErrorCode.REQUIRED_VALUE);
    this.sheetHeader = LarkSheetUtil.getSheetHeader(sheetInfoList, readerColumns);

    // extra params for range query
    this.rangeParamMap = LarkSheetUtil.fillLarkParams(this.jobConf.get(LarkSheetReaderOptions.LARK_PROPERTIES));

    this.splits = new ConcurrentLinkedDeque<>();
    this.subTaskId = readerContext.getIndexOfSubtask();
    this.recordQueue = new LinkedList<>();
  }

  @Override
  public void start() {
    LOG.info("Task {} start.", subTaskId);
  }

  @Override
  public void pollNext(SourcePipeline<Row> pipeline) throws Exception {
    if (Objects.isNull(curRecord) && recordQueue.isEmpty() && splits.isEmpty()) {
      return;
    }

    if (recordQueue.isEmpty() && !splits.isEmpty()) {
      fetchSheetBatchData();
    }

    // Split is over if recordQueue is empty.
    hasNextElements = CollectionUtils.isNotEmpty(recordQueue);
    if (hasNextElements) {
      curRecord = recordQueue.remove(0);
    } else {
      return;
    }
    pipeline.output(buildRow());
  }

  private Row buildRow() {
    if (curRecord == null) {
      throw new BitSailException(LarkSheetFormatErrorCode.INVALID_ROW, "row is null");
    }
    Row row = new Row(this.typeInfos.length);
    for (int i = 0; i < this.typeInfos.length; i++) {
      Object fieldVal = curRecord.get(i);
      row.setField(i, Objects.isNull(fieldVal) ? null : String.valueOf(fieldVal));
    }
    return row;
  }

  private void fetchSheetBatchData() {
    LarkSheetSplit split = splits.poll();
    LOG.info("Current handle split : {}.", split);

    // According to the current shard, obtain the sheetId, sheetToken, sheetMeta,
    // and row number range corresponding to the shard.
    SheetMeta sheetMeta = split.getSheetMeta();
    String sheetId = split.getSheetId();
    String sheetToken = split.getSheetToken();

    int startRowNumber = split.getStartRowNumber();
    int endRowNumber = split.getEndRowNumber();

    List<List<Object>> rows;
    if (startRowNumber > sheetMeta.getRowCount()) {
      LOG.warn("It may indicates there is some wrong with split calculating, please check code!");
      rows = Collections.emptyList();
    } else {
      String range = genSheetRange(this.sheetHeader, startRowNumber, endRowNumber, sheetMeta);
      rows = LarkSheetUtil.getRange(sheetToken, sheetId, range, this.rangeParamMap);
    }
    LOG.info("Get {} counts.", rows.size());

    // filter empty rows.
    for (List<Object> row : rows) {
      List<Object> reorderRow = reorder(row);
      if (reorderRow.stream().anyMatch(Objects::nonNull)) {
        recordQueue.add(reorderRow);
      }
    }
  }

  @Override
  public void addSplits(List<LarkSheetSplit> larkSheetSplits) {
    totalSplitNum += larkSheetSplits.size();
    splits.addAll(larkSheetSplits);
    LOG.info("totalSplitNum: {}, splits: {}.", totalSplitNum, splits);
  }

  @Override
  public void notifyNoMoreSplits() {
    this.hasNoMoreSplits = true;
    LOG.info("No more splits will be assigned.");
  }

  @Override
  public boolean hasMoreElements() {
    if (hasNoMoreSplits && splits.isEmpty() && !hasNextElements) {
      LOG.info("Finish read all {} splits.", totalSplitNum);
      return false;
    }
    return true;
  }

  @Override
  public List<LarkSheetSplit> snapshotState(long checkpointId) {
    return Collections.emptyList();
  }

  @Override
  public void close() throws Exception {
    // empty
  }

  /**
   * Reorder rows extracted from open api.
   *
   * @param originRow Rows from open api.
   * @return A list of ordered rows.
   */
  private List<Object> reorder(List<Object> originRow) {
    Integer[] reorderColumnIndex = this.sheetHeader.getReorderColumnIndex();
    List<Object> row = new ArrayList<>(reorderColumnIndex.length);
    for (Integer columnIndex : reorderColumnIndex) {
      row.add(originRow.get(columnIndex));
    }
    return row;
  }
}
