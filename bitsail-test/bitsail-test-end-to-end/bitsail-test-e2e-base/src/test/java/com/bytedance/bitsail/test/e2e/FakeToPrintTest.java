/*
 * Copyright 2022 Bytedance Ltd. and/or its affiliates.
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

package com.bytedance.bitsail.test.e2e;

import com.bytedance.bitsail.base.packages.PluginFinder;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.option.ReaderOptions;
import com.bytedance.bitsail.common.option.WriterOptions;
import com.bytedance.bitsail.test.e2e.base.executor.AbstractExecutor;
import com.bytedance.bitsail.test.e2e.base.executor.flink.Flink11Executor;

import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.nio.file.Paths;

@Ignore
public class FakeToPrintTest {

  final String bitsailRevision = "0.2.0-SNAPSHOT";
  final String bitsailRootDir = "/Users/bytedance/Desktop/output";
  final String readerClass = "com.bytedance.bitsail.connector.legacy.fake.source.FakeSource";
  final String writerClass = "com.bytedance.bitsail.connector.legacy.print.sink.PrintSink";
  final String readerLib = bitsailRootDir + "/libs/connectors/bitsail-connector-fake-" + bitsailRevision + ".jar";
  final String writerLib = bitsailRootDir + "/libs/connectors/bitsail-connector-print-" + bitsailRevision + ".jar";

  PluginFinder mockPluginFinder;
  BitSailConfiguration conf;
  Flink11Executor executor;

  @Before
  public void initEnv() throws Exception {
    System.setProperty(AbstractExecutor.BITSAIL_REVISION, bitsailRevision);
    System.setProperty(AbstractExecutor.BITSAIL_ROOT_DIR, bitsailRootDir);

    conf = BitSailConfiguration.from(
        new File(Paths.get(getClass().getClassLoader().getResource("fake_to_print.json").toURI()).toString()));
    conf.set(ReaderOptions.READER_CLASS, readerClass);
    conf.set(WriterOptions.WRITER_CLASS, writerClass);

    mockPluginFinder = Mockito.mock(PluginFinder.class);
    Mockito.when(mockPluginFinder.loadPlugin(readerClass)).thenReturn(Lists.newArrayList(Paths.get(readerLib).toUri().toURL()));
    Mockito.when(mockPluginFinder.loadPlugin(writerClass)).thenReturn(Lists.newArrayList(Paths.get(writerLib).toUri().toURL()));
  }

  @Test
  public void testFakeToPrint() throws Exception {
    Flink11Executor executor = new Flink11Executor();
    executor.setPluginFinder(mockPluginFinder);
    executor.configure(conf);
    executor.init();
    executor.run("Fake_To_Print");
  }

  @After
  public void destroy() throws Exception {
    if (executor != null) {
      executor.close();
      executor = null;
    }
  }
}
