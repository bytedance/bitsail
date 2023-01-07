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

package com.bytedance.bitsail.connector.localfilesystem.core.config;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.connector.localfilesystem.option.FileSystemReaderOptions;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Setter
@Getter
public class FileSystemConfig implements Serializable {
  private String filePath;
  private FileSystemType fileSystemType;
  private ContentType contentType;
  private Boolean skipFirstLine;

  public enum FileSystemType {
    LOCAL
  }

  public enum ContentType {
    CSV,
    JSON
  }

  public FileSystemConfig() {}

  public FileSystemConfig(BitSailConfiguration jobConf) {
    this.filePath = jobConf.get(FileSystemReaderOptions.FILE_PATH);
    this.contentType = FileSystemConfig.ContentType.valueOf(jobConf.get(FileSystemReaderOptions.CONTENT_TYPE).toUpperCase());
    this.skipFirstLine = jobConf.get(FileSystemReaderOptions.SKIP_FIRST_LINE);
  }
}


