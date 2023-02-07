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

package com.bytedance.bitsail.connector.ftp.source.split;

import com.bytedance.bitsail.base.connector.reader.v1.SourceSplit;

import lombok.Getter;
import lombok.Setter;

@Getter
public class FtpSourceSplit implements SourceSplit {

  public static final String FTP_SOURCE_SPLIT_PREFIX = "ftp_source_split_";

  private final String splitId;

  @Setter
  private String path;
  @Setter
  private long fileSize;

  public FtpSourceSplit(int splitId) {
    this.splitId = FTP_SOURCE_SPLIT_PREFIX + splitId;
  }

  @Override
  public String uniqSplitId() {
    return splitId;
  }

  @Override
  public boolean equals(Object obj) {
    return (obj instanceof FtpSourceSplit) && (splitId.equals(((FtpSourceSplit) obj).splitId));
  }

}
