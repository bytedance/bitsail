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

package com.bytedance.bitsail.flink.core.reader.util;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.exception.CommonErrorCode;
import com.bytedance.bitsail.common.option.ReaderOptions;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MultiSourceConfigUtils {

  /**
   * get reader conf list for multi source
   */
  public static List<BitSailConfiguration> getReaderConfList(final BitSailConfiguration jobConf) {
    List<Map<String, Object>> readerConfs = jobConf.getNecessaryOption(ReaderOptions.READER_CONFIG_LIST,
        CommonErrorCode.CONFIG_ERROR);
    return readerConfs.stream()
        .map(conf -> {
          BitSailConfiguration bitSailConf = BitSailConfiguration.newDefault();
          for (Map.Entry<String, Object> entry : conf.entrySet()) {
            bitSailConf.set(entry.getKey(), entry.getValue());
          }
          return bitSailConf;
        }).collect(Collectors.toList());
  }
}