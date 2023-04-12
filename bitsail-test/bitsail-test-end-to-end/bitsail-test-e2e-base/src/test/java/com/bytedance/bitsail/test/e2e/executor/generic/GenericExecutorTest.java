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

package com.bytedance.bitsail.test.e2e.executor.generic;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.option.ReaderOptions;
import com.bytedance.bitsail.common.option.WriterOptions;
import com.bytedance.bitsail.connector.fake.source.FakeSource;
import com.bytedance.bitsail.connector.print.sink.PrintSink;
import com.bytedance.bitsail.test.e2e.base.transfer.TransferableFile;
import com.bytedance.bitsail.test.e2e.executor.AbstractExecutor;

import org.junit.Assert;
import org.junit.Test;

import java.nio.file.Paths;
import java.util.Set;

public class GenericExecutorTest {

  @Test
  public void testCreateGenericExecutor() throws Exception {
    String settingFilePath = Paths.get(GenericExecutorSettingTest.class.getClassLoader()
        .getResource("executor/generic/TestGenericExecutorSetting.json")
        .toURI()).toString();

    GenericExecutor executor = new GenericExecutor(GenericExecutorSetting.initFromFile(settingFilePath));
    Assert.assertEquals("test-executor", executor.getContainerName());

    BitSailConfiguration jobConf = BitSailConfiguration.newDefault();
    jobConf.set(ReaderOptions.READER_CLASS, FakeSource.class.getName());
    jobConf.set(WriterOptions.WRITER_CLASS, PrintSink.class.getName());
    executor.configure(jobConf);
    Set<TransferableFile> transferableFiles = executor.getTransferableFiles();

    String localRootPath = AbstractExecutor.getLocalRootDir();
    TransferableFile file1 = new TransferableFile(Paths.get(localRootPath, "/local/1.jar").toAbsolutePath().toString(),
        "/executor/1.jar");
    TransferableFile file2 = new TransferableFile(Paths.get(localRootPath, "/local/2.jar").toAbsolutePath().toString(),
        "/executor/2.jar");
    Assert.assertTrue(transferableFiles.contains(file1));
    Assert.assertTrue(transferableFiles.contains(file2));
  }
}
