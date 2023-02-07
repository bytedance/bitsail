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

package com.bytedance.bitsail.connector.larksheet.source;

import com.bytedance.bitsail.base.connector.reader.v1.Boundedness;
import com.bytedance.bitsail.base.connector.reader.v1.Source;
import com.bytedance.bitsail.base.connector.reader.v1.SourceReader;
import com.bytedance.bitsail.base.connector.reader.v1.SourceSplitCoordinator;
import com.bytedance.bitsail.base.connector.writer.v1.state.EmptyState;
import com.bytedance.bitsail.base.execution.ExecutionEnviron;
import com.bytedance.bitsail.base.extension.ParallelismComputable;
import com.bytedance.bitsail.base.parallelism.ParallelismAdvice;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.common.type.BitSailTypeInfoConverter;
import com.bytedance.bitsail.common.type.TypeInfoConverter;
import com.bytedance.bitsail.connector.larksheet.api.SheetConfig;
import com.bytedance.bitsail.connector.larksheet.api.TokenHolder;
import com.bytedance.bitsail.connector.larksheet.constant.LarkSheetConstant;
import com.bytedance.bitsail.connector.larksheet.error.LarkSheetFormatErrorCode;
import com.bytedance.bitsail.connector.larksheet.meta.SheetInfo;
import com.bytedance.bitsail.connector.larksheet.option.LarkSheetReaderOptions;
import com.bytedance.bitsail.connector.larksheet.source.coordinate.LarkSheetSourceSplitCoordinator;
import com.bytedance.bitsail.connector.larksheet.source.reader.LarkSheetReader;
import com.bytedance.bitsail.connector.larksheet.source.split.LarkSheetSplit;
import com.bytedance.bitsail.connector.larksheet.source.split.strategy.SimpleDivideSplitConstructor;
import com.bytedance.bitsail.connector.larksheet.util.LarkSheetUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.bytedance.bitsail.connector.larksheet.constant.LarkSheetConstant.DEFAULT_PARALLELISM;
import static com.bytedance.bitsail.connector.larksheet.constant.LarkSheetConstant.SPLIT_COMMA;

public class LarkSheetSource implements Source<Row, LarkSheetSplit, EmptyState>, ParallelismComputable {

  private static final Logger LOG = LoggerFactory.getLogger(LarkSheetSource.class);

  private BitSailConfiguration jobConf;

  @Override
  public void configure(ExecutionEnviron execution, BitSailConfiguration readerConfiguration) throws IOException {
    this.jobConf = readerConfiguration;
  }

  @Override
  public Boundedness getSourceBoundedness() {
    return Boundedness.BOUNDEDNESS;
  }

  @Override
  public SourceReader<Row, LarkSheetSplit> createReader(SourceReader.Context readerContext) {
    return new LarkSheetReader(jobConf, readerContext);
  }

  @Override
  public SourceSplitCoordinator<LarkSheetSplit, EmptyState> createSplitCoordinator(SourceSplitCoordinator.Context<LarkSheetSplit, EmptyState> coordinatorContext) {
    return new LarkSheetSourceSplitCoordinator(jobConf, coordinatorContext);
  }

  @Override
  public String getReaderName() {
    return LarkSheetConstant.LARK_SHEET_CONNECTOR_NAME;
  }

  @Override
  public TypeInfoConverter createTypeInfoConverter() {
    return new BitSailTypeInfoConverter();
  }

  @Override
  public ParallelismAdvice getParallelismAdvice(BitSailConfiguration commonConf, BitSailConfiguration selfConf, ParallelismAdvice upstreamAdvice) throws Exception {

    int batchSize = this.jobConf.get(LarkSheetReaderOptions.BATCH_SIZE);
    List<Integer> skipNums = this.jobConf.getUnNecessaryOption(LarkSheetReaderOptions.SKIP_NUMS, new ArrayList<>());

    new SheetConfig().configure(this.jobConf);
    TokenHolder.init(SheetConfig.PRE_DEFINED_SHEET_TOKEN);
    String sheetUrlList = this.jobConf.getNecessaryOption(LarkSheetReaderOptions.SHEET_URL,
        LarkSheetFormatErrorCode.REQUIRED_VALUE);
    List<String> sheetUrls = Arrays.asList(sheetUrlList.split(SPLIT_COMMA));
    List<SheetInfo> sheetInfoList = LarkSheetUtil.resolveSheetUrls(sheetUrls);

    SimpleDivideSplitConstructor splitConstructor = new SimpleDivideSplitConstructor(sheetInfoList, batchSize, skipNums);
    List<LarkSheetSplit> sheetSplits = splitConstructor.construct();
    LOG.info("Construct sheet splits number is {}.", sheetSplits.size());

    int adviceParallelism = DEFAULT_PARALLELISM;
    if (selfConf.fieldExists(LarkSheetReaderOptions.READER_PARALLELISM_NUM)) {
      adviceParallelism = selfConf.get(LarkSheetReaderOptions.READER_PARALLELISM_NUM);
      adviceParallelism = Math.min(adviceParallelism, sheetSplits.size());
    }

    if (adviceParallelism <= 0) {
      adviceParallelism = Math.max(sheetSplits.size(), DEFAULT_PARALLELISM);
    }

    LOG.info("Finally advice parallelism is {}.", adviceParallelism);
    return ParallelismAdvice.builder()
        .enforceDownStreamChain(true)
        .adviceParallelism(adviceParallelism)
        .build();
  }
}
