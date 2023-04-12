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

import lombok.AllArgsConstructor;

import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

@AllArgsConstructor
public class FileMappingTransfer {

  /**
   * Root dir of source.
   */
  private final String srcRoot;

  /**
   * Root dir of target.
   */
  private final String dstRoot;

  /**
   * Transfer a list of files.
   */
  public List<TransferableFile> transfer(List<String> files) {
    return files.stream().map(this::transfer).collect(Collectors.toList());
  }

  /**
   * Transfer single file.
   */
  public TransferableFile transfer(String file) {
    return new TransferableFile(
        Paths.get(srcRoot, file).toAbsolutePath().toString(),
        Paths.get(dstRoot, file).toAbsolutePath().toString()
    );
  }
}
