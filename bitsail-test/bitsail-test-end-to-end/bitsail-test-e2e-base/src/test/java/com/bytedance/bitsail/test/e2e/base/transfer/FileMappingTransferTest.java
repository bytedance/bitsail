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

package com.bytedance.bitsail.test.e2e.base.transfer;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.List;

public class FileMappingTransferTest {

  @Test
  public void testFileMappingTransfer() {
    String srcRoot = "/src";
    String dstRoot = "/dst";
    List<String> files = Lists.newArrayList(
        "README.md",
        "bin/bitsail",
        "conf/",
        "libs/bitsail-core.jar",
        "libs/engines/engine.jar",
        "libs/engines/mapping/conf.json"
    );

    FileMappingTransfer transfer = new FileMappingTransfer(srcRoot, dstRoot);
    List<TransferableFile> transferableFiles = transfer.transfer(files);

    Assert.assertEquals(files.size(), transferableFiles.size());
    for (int i = 0; i < files.size(); ++i) {
      String expectedStr = String.format("{\"host_path\":\"%s\", \"container_path\":\"%s\"}",
          new File(srcRoot + "/" + files.get(i)).getAbsolutePath(),
          new File(dstRoot + "/" + files.get(i)).getAbsolutePath());
      Assert.assertEquals(expectedStr, transferableFiles.get(i).toString());
    }
  }
}
