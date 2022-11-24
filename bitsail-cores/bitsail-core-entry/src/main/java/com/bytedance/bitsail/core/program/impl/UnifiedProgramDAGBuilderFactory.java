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

package com.bytedance.bitsail.core.program.impl;

import com.bytedance.bitsail.base.connector.reader.DataReaderDAGBuilder;
import com.bytedance.bitsail.base.connector.reader.v1.Source;
import com.bytedance.bitsail.base.connector.writer.DataWriterDAGBuilder;
import com.bytedance.bitsail.base.connector.writer.v1.Sink;
import com.bytedance.bitsail.base.execution.Mode;
import com.bytedance.bitsail.base.packages.PluginFinder;
import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.exception.CommonErrorCode;
import com.bytedance.bitsail.common.option.ReaderOptions;
import com.bytedance.bitsail.common.option.WriterOptions;
import com.bytedance.bitsail.core.program.ProgramDAGBuilderFactory;
import com.bytedance.bitsail.flink.core.legacy.connector.InputFormatPlugin;
import com.bytedance.bitsail.flink.core.legacy.connector.OutputFormatPlugin;
import com.bytedance.bitsail.flink.core.reader.FlinkSourceDAGBuilder;
import com.bytedance.bitsail.flink.core.reader.PluginableInputFormatDAGBuilder;
import com.bytedance.bitsail.flink.core.writer.FlinkWriterBuilder;
import com.bytedance.bitsail.flink.core.writer.PluginableOutputFormatDAGBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

public class UnifiedProgramDAGBuilderFactory implements ProgramDAGBuilderFactory {
  private static final Logger LOG = LoggerFactory.getLogger(UnifiedProgramDAGBuilderFactory.class);

  @Override
  public List<DataReaderDAGBuilder> getDataReaderDAGBuilders(Mode mode,
                                                             List<BitSailConfiguration> readerConfigurations,
                                                             PluginFinder pluginFinder) {
    return readerConfigurations.stream()
        .map(readerConf -> {
          try {
            return getDataReaderDAGBuilder(mode, readerConf, pluginFinder);
          } catch (Exception e) {
            LOG.error("failed to create reader DAG builder");
            throw new RuntimeException(e);
          }
        })
        .collect(Collectors.toList());
  }

  public <T> DataReaderDAGBuilder getDataReaderDAGBuilder(Mode mode,
                                                          BitSailConfiguration globalConfiguration,
                                                          PluginFinder pluginFinder) throws Exception {
    T reader = constructReader(globalConfiguration, pluginFinder);
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
                                       PluginFinder pluginFinder) {
    String readerClassName = globalConfiguration.get(ReaderOptions.READER_CLASS);
    LOG.info("Reader class name is {}", readerClassName);
    return pluginFinder.findPluginInstance(readerClassName);
  }

  @Override
  public List<DataWriterDAGBuilder> getDataWriterDAGBuilders(Mode mode,
                                                             List<BitSailConfiguration> writerConfigurations,
                                                             PluginFinder pluginFinder) {
    return writerConfigurations.stream()
        .map(writerConf -> {
          try {
            return getDataWriterDAGBuilder(mode, writerConf, pluginFinder);
          } catch (Exception e) {
            LOG.error("failed to create writer DAG builder");
            throw new RuntimeException(e);
          }
        })
        .collect(Collectors.toList());
  }

  public <T> DataWriterDAGBuilder getDataWriterDAGBuilder(Mode mode,
                                                          BitSailConfiguration globalConfiguration,
                                                          PluginFinder pluginFinder) throws Exception {

    String writerClassName = globalConfiguration.get(WriterOptions.WRITER_CLASS);
    T writer = constructWriter(writerClassName, pluginFinder);
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
                                       PluginFinder pluginFinder) {
    LOG.info("Writer class name is {}", writerClassName);
    return pluginFinder.findPluginInstance(writerClassName);
  }
}
