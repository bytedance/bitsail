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

package com.bytedance.bitsail.test.e2e.base.executor.flink;

import com.bytedance.bitsail.base.packages.PluginFinder;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.option.ReaderOptions;
import com.bytedance.bitsail.common.option.WriterOptions;
import com.bytedance.bitsail.test.e2e.base.TestOptions;
import com.bytedance.bitsail.test.e2e.base.executor.AbstractExecutor;
import com.bytedance.bitsail.test.e2e.base.transfer.FileMappingTransfer;
import com.bytedance.bitsail.test.e2e.base.transfer.TransferableFile;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.nio.file.Paths;
import java.util.List;
import java.util.Set;

public class FlinkExecutorTest {

  final String bitsailRevision = "0.2.0";
  final String bitsailRootDir = "/Users/bytedance/Desktop/output";
  final String readerClass = "com.bytedance.bitsail.connector.fake.source.FakeSource";
  final String writerClass = "com.bytedance.bitsail.connector.print.sink.PrintSink";
  final String readerLib = "/tmp/bitsail/libs/connectors/connector-fake-" + bitsailRevision + ".jar";
  final String writerLib = "/tmp/bitsail/libs/connectors/connector-print-" + bitsailRevision + ".jar";

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
  public void testFileMapping() {
    executor = new Flink11Executor();
    executor.setPluginFinder(mockPluginFinder);
    executor.configure(conf);
    Set<TransferableFile> actualFiles = executor.getTransferableFiles();

    List<String> expectedFiles = Lists.newArrayList(
        "bin/",
        "conf/logback.xml",
        "libs/bitsail-core.jar",
        "libs/clients/",
        "libs/engines/bitsail-engine-flink-" + bitsailRevision + ".jar",
        "libs/engines/mapping/",
        "libs/connectors/connector-fake-" + bitsailRevision + ".jar",
        "libs/connectors/connector-print-" + bitsailRevision + ".jar",
        "libs/connectors/mapping/"
    );

    FileMappingTransfer transfer = new FileMappingTransfer(bitsailRootDir, conf.get(TestOptions.PROJECT_ROOT_DIR));
    for (String expectFile : expectedFiles) {
      TransferableFile file = transfer.transfer(expectFile);
      Assert.assertTrue(actualFiles.contains(file));
    }
  }
}
