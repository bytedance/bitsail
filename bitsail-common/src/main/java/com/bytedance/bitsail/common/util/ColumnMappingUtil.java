/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.bytedance.bitsail.common.util;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.exception.FrameworkErrorCode;
import com.bytedance.bitsail.common.model.ColumnInfo;
import com.bytedance.bitsail.common.option.ReaderOptions;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ColumnMappingUtil {

  /**
   * Get reader mapping from configuration, and mark index for the columns.
   */
  public static Map<String, Integer> getReaderMappingFromConf(BitSailConfiguration conf) {
    List<ColumnInfo> columnInfos = conf.getNecessaryOption(ReaderOptions.BaseReaderOptions.COLUMNS, FrameworkErrorCode.REQUIRED_VALUE);

    return IntStream.range(0, columnInfos.size())
        .boxed()
        .collect(Collectors.toMap(index -> columnInfos.get(index).getName().toUpperCase(), index -> index));
  }
}
