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
import com.bytedance.bitsail.test.e2e.base.transfer.TransferableFile;

import org.junit.Assert;
import org.junit.Test;

import java.nio.file.Paths;
import java.util.List;

public class GenericExecutorSettingTest {

  @Test
  public void testReadSetting() throws Exception {
    String settingFilePath = Paths.get(GenericExecutorSettingTest.class.getClassLoader()
        .getResource("executor/generic/TestGenericExecutorSetting.json")
        .toURI()).toString();
    GenericExecutorSetting setting = GenericExecutorSetting.initFromFile(settingFilePath);

    Assert.assertEquals("test-executor", setting.getExecutorName());
    Assert.assertEquals("test-image", setting.getExecutorImage());

    List<TransferableFile> transferableFileList = setting.getAdditionalFiles();
    Assert.assertEquals(2, transferableFileList.size());
    TransferableFile file1 = new TransferableFile("/local/1.jar", "/executor/1.jar");
    TransferableFile file2 = new TransferableFile("/local/2.jar", "/executor/2.jar");
    Assert.assertTrue(transferableFileList.contains(file1));
    Assert.assertTrue(transferableFileList.contains(file2));

    Assert.assertEquals("pwd && sleep 5000",
        String.join(" ", setting.getExecCommands()));
    Assert.assertEquals("pwd && sleep 1000",
        String.join(" ", setting.getFailureHandleCommands()));

    BitSailConfiguration globalJobConf = setting.getGlobalJobConf();
    Assert.assertEquals(Integer.valueOf(1),
        globalJobConf.get(ReaderOptions.BaseReaderOptions.READER_PARALLELISM_NUM));
    Assert.assertEquals(Integer.valueOf(2),
        globalJobConf.get(WriterOptions.BaseWriterOptions.WRITER_PARALLELISM_NUM));
  }
}
