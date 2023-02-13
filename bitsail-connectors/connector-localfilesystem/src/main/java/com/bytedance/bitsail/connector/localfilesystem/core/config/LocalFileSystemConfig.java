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

package com.bytedance.bitsail.connector.localfilesystem.core.config;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.connector.localfilesystem.option.LocalFileSystemReaderOptions;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Setter
@Getter
public class LocalFileSystemConfig implements Serializable {
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

  public LocalFileSystemConfig() {}

  public LocalFileSystemConfig(BitSailConfiguration jobConf) {
    this.filePath = jobConf.get(LocalFileSystemReaderOptions.FILE_PATH);
    this.contentType = LocalFileSystemConfig.ContentType.valueOf(jobConf.get(LocalFileSystemReaderOptions.CONTENT_TYPE).toUpperCase());
    this.skipFirstLine = jobConf.get(LocalFileSystemReaderOptions.SKIP_FIRST_LINE);
  }
}


