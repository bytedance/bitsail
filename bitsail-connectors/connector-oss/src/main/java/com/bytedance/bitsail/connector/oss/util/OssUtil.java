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

package com.bytedance.bitsail.connector.oss.util;

import com.bytedance.bitsail.connector.oss.config.HadoopConf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class OssUtil {
  static FileSystem fs;
  public static Configuration getConfiguration(HadoopConf hadoopConf) {
    Configuration configuration = new Configuration();
    configuration.set(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY, hadoopConf.getHdfsNameKey());
    configuration.set(
        String.format("fs.%s.impl", hadoopConf.getSchema()), hadoopConf.getHdfsImpl());
    hadoopConf.setExtraOptionsForConfiguration(configuration);
    return configuration;
  }

  public static List<String> getFileNamesByPath(HadoopConf hadoopConf, String path) throws IOException {
    Configuration configuration = getConfiguration(hadoopConf);
    fs = FileSystem.get(configuration);
    ArrayList<String> fileNames = new ArrayList<>();
    Path listFiles = new Path(path);
    FileStatus[] stats = fs.listStatus(listFiles);
    for (FileStatus fileStatus : stats) {
      if (fileStatus.isDirectory()) {
        fileNames.addAll(getFileNamesByPath(hadoopConf, fileStatus.getPath().toString()));
        continue;
      }
      if (fileStatus.isFile()) {
        if (!fileStatus.getPath().getName().equals("_SUCCESS")) {
          String filePath = fileStatus.getPath().toString();
          fileNames.add(filePath);
        }
      }
    }
    return fileNames;
  }
}
