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

package com.bytedance.bitsail.core.flink116.bridge.program;

import com.bytedance.bitsail.base.component.DefaultComponentBuilderLoader;
import com.bytedance.bitsail.base.connector.reader.DataReaderDAGBuilder;
import com.bytedance.bitsail.base.connector.reader.v1.Source;
import com.bytedance.bitsail.base.connector.transform.DataTransformDAGBuilder;
import com.bytedance.bitsail.base.connector.transform.v1.MapTransformer;
import com.bytedance.bitsail.base.connector.transform.v1.PartitionTransformer;
import com.bytedance.bitsail.base.connector.transform.v1.Transformer;
import com.bytedance.bitsail.base.connector.writer.DataWriterDAGBuilder;
import com.bytedance.bitsail.base.connector.writer.v1.Sink;
import com.bytedance.bitsail.base.execution.Mode;
import com.bytedance.bitsail.base.packages.PluginFinder;
import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.exception.CommonErrorCode;
import com.bytedance.bitsail.common.option.ReaderOptions;
import com.bytedance.bitsail.common.option.TransformOptions;
import com.bytedance.bitsail.common.option.WriterOptions;
import com.bytedance.bitsail.core.api.program.factory.ProgramDAGBuilderFactory;
import com.bytedance.bitsail.core.flink116.bridge.reader.builder.FlinkSourceDAGBuilder;
import com.bytedance.bitsail.core.flink116.bridge.writer.builder.FlinkWriterBuilder;
import com.bytedance.bitsail.flink.core.legacy.connector.InputFormatPlugin;
import com.bytedance.bitsail.flink.core.legacy.connector.OutputFormatPlugin;
import com.bytedance.bitsail.flink.core.reader.PluginableInputFormatDAGBuilder;
import com.bytedance.bitsail.flink.core.transform.builder.FlinkMapTransformerBuilder;
import com.bytedance.bitsail.flink.core.transform.builder.FlinkPartitionTransformerBuilder;
import com.bytedance.bitsail.flink.core.writer.PluginableOutputFormatDAGBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class FlinkDAGBuilderFactory implements ProgramDAGBuilderFactory {
  private static final Logger LOG = LoggerFactory.getLogger(FlinkDAGBuilderFactory.class);

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
                                                          BitSailConfiguration readerConfiguration,
                                                          PluginFinder pluginFinder) throws Exception {
    String readerClassName = readerConfiguration.get(ReaderOptions.READER_CLASS);
    LOG.info("Reader class name is {}", readerClassName);
    T reader = construct(readerClassName, pluginFinder);
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
    LOG.info("Writer class name is {}", writerClassName);
    T writer = construct(writerClassName, pluginFinder);
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

  @Override
  public List<DataTransformDAGBuilder> getDataTransformDAGBuilders(Mode mode,
                                                                   List<BitSailConfiguration> configurations,
                                                                   PluginFinder pluginFinder) {
    DefaultComponentBuilderLoader<Transformer> transformerPluginFinder =
        new DefaultComponentBuilderLoader<>(Transformer.class, pluginFinder.getClassloader());

    return configurations.stream()
        .map(transformConf -> {
          try {
            return getDataTransformDAGBuilder(mode, transformConf, transformerPluginFinder);
          } catch (Exception e) {
            LOG.error("failed to create transform DAG builder");
            throw new RuntimeException(e);
          }
        })
        .filter(Objects::nonNull)
        .collect(Collectors.toList());
  }

  public <T> DataTransformDAGBuilder getDataTransformDAGBuilder(Mode mode,
                                                                BitSailConfiguration transformConfiguration,
                                                                DefaultComponentBuilderLoader<Transformer> transformerPluginFinder) {
    String name = transformConfiguration.get(TransformOptions.BaseTransformOptions
        .TRANSFORM_NAME);
    LOG.info("Transform's name {}.", name);
    Transformer transformer = transformerPluginFinder.loadComponent(name);
    if (transformer instanceof MapTransformer) {
      return new FlinkMapTransformerBuilder<>(transformer);
    } else if (transformer instanceof PartitionTransformer) {
      return new FlinkPartitionTransformerBuilder<>(transformer);
    }
    throw BitSailException.asBitSailException(CommonErrorCode.CONFIG_ERROR,
        String.format("Transformer %s is not support.", name));
  }

  private static <T> T construct(String clazz,
                                 PluginFinder pluginFinder) {
    return pluginFinder.findPluginInstance(clazz);
  }
}
