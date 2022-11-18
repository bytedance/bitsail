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

package com.bytedance.bitsail.core.reader;

import com.bytedance.bitsail.base.connector.reader.DataReaderDAGBuilder;
import com.bytedance.bitsail.base.connector.reader.v1.Source;
import com.bytedance.bitsail.base.execution.Mode;
import com.bytedance.bitsail.base.packages.PluginExplorer;
import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.exception.CommonErrorCode;
import com.bytedance.bitsail.common.option.ReaderOptions;
import com.bytedance.bitsail.flink.core.legacy.connector.InputFormatPlugin;
import com.bytedance.bitsail.flink.core.reader.FlinkSourceDAGBuilder;
import com.bytedance.bitsail.flink.core.reader.PluginableInputFormatDAGBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Created 2022/4/21
 */
public class DataReaderBuilderFactory {
  private static final Logger LOG = LoggerFactory.getLogger(DataReaderBuilderFactory.class);

  public static <T> List<DataReaderDAGBuilder> getDataReaderDAGBuilderList(Mode mode,
                                                                           List<BitSailConfiguration> readerConfigurations,
                                                                           PluginExplorer pluginExplorer) {
    return readerConfigurations.stream()
        .map(readerConf -> {
          try {
            return getDataReaderDAGBuilder(mode, readerConf, pluginExplorer);
          } catch (Exception e) {
            LOG.error("failed to create reader DAG builder");
            throw new RuntimeException(e);
          }
        })
        .collect(Collectors.toList());
  }

  public static <T> DataReaderDAGBuilder getDataReaderDAGBuilder(Mode mode,
                                                                 BitSailConfiguration globalConfiguration,
                                                                 PluginExplorer pluginExplorer) throws Exception {
    T reader = DataReaderBuilderFactory.constructReader(globalConfiguration, pluginExplorer);
    if (reader instanceof DataReaderDAGBuilder) {
      return (DataReaderDAGBuilder) reader;
    }
    if (reader instanceof InputFormatPlugin) {
      return new PluginableInputFormatDAGBuilder<>((InputFormatPlugin<?, ?>) reader);
    }
    if (reader instanceof Source) {
      return new FlinkSourceDAGBuilder<>((Source<?, ?, ?>) reader);
    }

    throw BitSailException.asBitSailException(CommonErrorCode.CONFIG_ERROR,
        "Reader class is not supported ");
  }

  private static <T> T constructReader(BitSailConfiguration globalConfiguration,
                                       PluginExplorer pluginExplorer) {
    String readerClassName = globalConfiguration.get(ReaderOptions.READER_CLASS);
    LOG.info("Reader class name is {}", readerClassName);
    return pluginExplorer.loadPluginInstance(readerClassName);
  }
}
