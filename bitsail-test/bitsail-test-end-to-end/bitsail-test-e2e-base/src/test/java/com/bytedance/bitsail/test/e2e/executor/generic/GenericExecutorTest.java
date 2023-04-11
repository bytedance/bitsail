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

import java.io.File;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

public class GenericExecutorTest {
  public String transformHostFilePath(String hostFilePath) {
    File module = Paths.get(AbstractExecutor.getLocalRootDir(),
            "bitsail-test",
            "bitsail-test-end-to-end",
            "bitsail-test-e2e-base").toFile();
    File jarFolder = Paths.get(module.getAbsolutePath(),
            "src", "test", "resources", "executor", "generic", "host_file").toFile();
    return Paths.get(
            jarFolder.getAbsolutePath(),
            Paths.get(hostFilePath).getFileName().toString()
    ).toString();
  }

  @Test
  public void testCreateGenericExecutor() throws Exception {
    String settingFilePath = Paths.get(GenericExecutorSettingTest.class.getClassLoader()
        .getResource("executor/generic/TestGenericExecutorSetting.json")
        .toURI()).toString();

    GenericExecutorSetting executorSetting = GenericExecutorSetting.initFromFile(settingFilePath);
    List<TransferableFile> collect = executorSetting.getAdditionalFiles()
            .stream()
            .map(x -> new TransferableFile(transformHostFilePath(x.getHostPath()), x.getContainerPath()))
            .collect(Collectors.toList());
    executorSetting.setAdditionalFiles(collect);
    GenericExecutor executor = new GenericExecutor(executorSetting);
    Assert.assertEquals("test-executor", executor.getContainerName());

    BitSailConfiguration jobConf = BitSailConfiguration.newDefault();
    jobConf.set(ReaderOptions.READER_CLASS, FakeSource.class.getName());
    jobConf.set(WriterOptions.WRITER_CLASS, PrintSink.class.getName());
    executor.configure(jobConf);
    executor.addJobConf(jobConf);
    executor.init();

    int exitCode = executor.run("testID");
    Assert.assertEquals(exitCode, 0);

    executor.close();
  }
}
