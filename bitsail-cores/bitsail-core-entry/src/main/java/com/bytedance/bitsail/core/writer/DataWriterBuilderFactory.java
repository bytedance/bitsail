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

package com.bytedance.bitsail.core.writer;

import com.bytedance.bitsail.base.connector.writer.DataWriterDAGBuilder;
import com.bytedance.bitsail.base.connector.writer.v1.Sink;
import com.bytedance.bitsail.base.execution.Mode;
import com.bytedance.bitsail.base.packages.PluginExplorer;
import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.exception.CommonErrorCode;
import com.bytedance.bitsail.common.option.WriterOptions;
import com.bytedance.bitsail.flink.core.legacy.connector.OutputFormatPlugin;
import com.bytedance.bitsail.flink.core.writer.FlinkWriterBuilder;
import com.bytedance.bitsail.flink.core.writer.PluginableOutputFormatDAGBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Created 2022/4/21
 */
public class DataWriterBuilderFactory {
  private static final Logger LOG = LoggerFactory.getLogger(DataWriterBuilderFactory.class);

  public static <T> List<DataWriterDAGBuilder> getDataWriterDAGBuilderList(Mode mode,
                                                                           List<BitSailConfiguration> writerConfigurations,
                                                                           PluginExplorer pluginExplorer) {
    return writerConfigurations.stream()
        .map(writerConf -> {
          try {
            return getDataWriterDAGBuilder(mode, writerConf, pluginExplorer);
          } catch (Exception e) {
            LOG.error("failed to create writer DAG builder");
            throw new RuntimeException(e);
          }
        })
        .collect(Collectors.toList());
  }

  public static <T> DataWriterDAGBuilder getDataWriterDAGBuilder(Mode mode,
                                                                 BitSailConfiguration globalConfiguration,
                                                                 PluginExplorer pluginExplorer) throws Exception {

    String writerClassName = globalConfiguration.get(WriterOptions.WRITER_CLASS);
    T writer = DataWriterBuilderFactory.constructWriter(writerClassName, pluginExplorer);
    if (writer instanceof Sink) {
      return new FlinkWriterBuilder<>((Sink<?, ?, ?>) writer);
    }
    if (writer instanceof DataWriterDAGBuilder) {
      return (DataWriterDAGBuilder) writer;
    }
    if (writer instanceof OutputFormatPlugin) {
      return new PluginableOutputFormatDAGBuilder<>((OutputFormatPlugin<?>) writer);
    }
    throw BitSailException.asBitSailException(CommonErrorCode.CONFIG_ERROR,
        String.format("Writer %s is not support.", writerClassName));
  }

  private static <T> T constructWriter(String writerClassName,
                                       PluginExplorer pluginExplorer) {
    LOG.info("Writer class name is {}", writerClassName);
    return pluginExplorer.loadPluginInstance(writerClassName);
  }
}
