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

package com.bytedance.bitsail.core.flink.bridge.program;

import com.bytedance.bitsail.base.connector.reader.DataReaderDAGBuilder;
import com.bytedance.bitsail.base.connector.writer.DataWriterDAGBuilder;
import com.bytedance.bitsail.base.execution.Mode;
import com.bytedance.bitsail.base.packages.PluginFinder;
import com.bytedance.bitsail.base.packages.PluginFinderFactory;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.option.CommonOptions;
import com.bytedance.bitsail.common.option.ReaderOptions;
import com.bytedance.bitsail.common.option.WriterOptions;

import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class FlinkDAGBuilderFactoryTest {
  private BitSailConfiguration dagBuilderConf;
  private BitSailConfiguration legacyPluginConf;

  private PluginFinder pluginFinder;

  private FlinkDAGBuilderFactory flinkDAGBuilderFactory;

  @Before
  public void init() {
    dagBuilderConf = BitSailConfiguration.newDefault();
    dagBuilderConf.set(ReaderOptions.READER_CLASS, MockDataReaderDAGBuilder.class.getName());
    dagBuilderConf.set(WriterOptions.WRITER_CLASS, MockDataWriterDAGBuilder.class.getName());

    pluginFinder = PluginFinderFactory
        .getPluginFinder(dagBuilderConf.get(CommonOptions.PLUGIN_FINDER_NAME));
    pluginFinder.configure(dagBuilderConf);

    legacyPluginConf = BitSailConfiguration.newDefault();
    legacyPluginConf.set(ReaderOptions.READER_CLASS, MockInputFormatPlugin.class.getName());
    legacyPluginConf.set(WriterOptions.WRITER_CLASS, MockOutputFormatPlugin.class.getName());

    flinkDAGBuilderFactory = new FlinkDAGBuilderFactory();
  }

  @Test
  public void testGetDataReaderDAGBuilder() throws Exception {

    DataReaderDAGBuilder dataReaderDAGBuilder = flinkDAGBuilderFactory.getDataReaderDAGBuilder(
        Mode.BATCH, dagBuilderConf, pluginFinder);
    assertEquals(dataReaderDAGBuilder.getReaderName(), MockDataReaderDAGBuilder.class.getSimpleName());
  }

  @Test
  public void testGetInputFormatPlugin() throws Exception {
    DataReaderDAGBuilder dataReaderDAGBuilder = flinkDAGBuilderFactory.getDataReaderDAGBuilder(
        Mode.BATCH, legacyPluginConf, pluginFinder);
    assertEquals(dataReaderDAGBuilder.getReaderName(), MockInputFormatPlugin.class.getSimpleName());
  }

  @Test
  public void testGetDataReaderDAGBuilderList() {
    List<DataReaderDAGBuilder> dataReaderDAGBuilderList = flinkDAGBuilderFactory.getDataReaderDAGBuilders(
        Mode.BATCH, ImmutableList.of(dagBuilderConf, legacyPluginConf), pluginFinder);
    assertEquals(dataReaderDAGBuilderList.size(), 2);
    assertEquals(dataReaderDAGBuilderList.get(0).getReaderName(), MockDataReaderDAGBuilder.class.getSimpleName());
    assertEquals(dataReaderDAGBuilderList.get(1).getReaderName(), MockInputFormatPlugin.class.getSimpleName());
  }

  @Test
  public void testGetDataWriterDAGBuilder() throws Exception {

    DataWriterDAGBuilder dataWriterDAGBuilder = flinkDAGBuilderFactory.getDataWriterDAGBuilder(
        Mode.BATCH, dagBuilderConf, pluginFinder);
    assertEquals(dataWriterDAGBuilder.getWriterName(), MockDataWriterDAGBuilder.class.getSimpleName());
  }

  @Test
  public void testGetOutputFormatPlugin() throws Exception {
    DataWriterDAGBuilder dataWriterDAGBuilder = flinkDAGBuilderFactory.getDataWriterDAGBuilder(
        Mode.BATCH, legacyPluginConf, pluginFinder);
    assertEquals(dataWriterDAGBuilder.getWriterName(), MockOutputFormatPlugin.class.getSimpleName());
  }

  @Test
  public void testGetDataWriterDAGBuilderList() {
    List<DataWriterDAGBuilder> dataWriterDAGBuilderList = flinkDAGBuilderFactory.getDataWriterDAGBuilders(
        Mode.BATCH, ImmutableList.of(dagBuilderConf, legacyPluginConf), pluginFinder);
    assertEquals(dataWriterDAGBuilderList.size(), 2);
    assertEquals(dataWriterDAGBuilderList.get(0).getWriterName(), MockDataWriterDAGBuilder.class.getSimpleName());
    assertEquals(dataWriterDAGBuilderList.get(1).getWriterName(), MockOutputFormatPlugin.class.getSimpleName());
  }

}