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

package com.bytedance.bitsail.test.e2e.executor.flink;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.option.CommonOptions;
import com.bytedance.bitsail.common.option.ReaderOptions;
import com.bytedance.bitsail.common.option.WriterOptions;
import com.bytedance.bitsail.test.e2e.base.transfer.FileMappingTransfer;
import com.bytedance.bitsail.test.e2e.base.transfer.TransferableFile;
import com.bytedance.bitsail.test.e2e.executor.AbstractExecutor;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;
import java.util.Set;

@Ignore
public class FlinkExecutorTest {

  final String bitsailRevision = "0.2.0";
  final String bitsailRootDir = "/local/bitsail";
  final String readerClass = "com.bytedance.bitsail.connector.fake.source.FakeSource";
  final String writerClass = "com.bytedance.bitsail.connector.print.sink.PrintSink";
  final String readerLib = "/tmp/bitsail/libs/connectors/connector-fake-" + bitsailRevision + ".jar";
  final String writerLib = "/tmp/bitsail/libs/connectors/connector-print-" + bitsailRevision + ".jar";

  BitSailConfiguration conf;
  Flink11Executor executor;

  @Before
  public void initEnv() throws Exception {
    System.setProperty(AbstractExecutor.BITSAIL_REVISION, bitsailRevision);
    System.setProperty(AbstractExecutor.BITSAIL_ROOT_DIR, bitsailRootDir);

    conf = BitSailConfiguration.newDefault();
    conf.set(ReaderOptions.READER_CLASS, readerClass);
    conf.set(WriterOptions.WRITER_CLASS, writerClass);
  }

  @Test
  public void testFileMapping() {
    executor = new Flink11Executor();
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

    FileMappingTransfer transfer = new FileMappingTransfer(bitsailRootDir,
        conf.get(CommonOptions.E2EOptions.E2E_EXECUTOR_ROOT_DIR));
    for (String expectFile : expectedFiles) {
      TransferableFile file = transfer.transfer(expectFile);
      Assert.assertTrue(actualFiles.contains(file));
    }
  }
}
