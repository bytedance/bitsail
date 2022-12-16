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

package com.bytedance.bitsail.connector.hadoop.source.config;

import lombok.Data;
import org.apache.hadoop.conf.Configuration;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import static com.bytedance.bitsail.connector.hadoop.constant.HadoopConstants.HDFS_IMPL;
import static com.bytedance.bitsail.connector.hadoop.constant.HadoopConstants.SCHEMA;

@Data
public class HadoopConf implements Serializable {

  private Map<String, String> extraOptions = new HashMap<>();
  private String hdfsNameKey;

  public HadoopConf(String hdfsNameKey) {
    this.hdfsNameKey = hdfsNameKey;
  }

  public String getFsHdfsImpl() {
    return HDFS_IMPL;
  }

  public String getSchema() {
    return SCHEMA;
  }

  public void setExtraOptionsForConfiguration(Configuration configuration) {
    if (!extraOptions.isEmpty()) {
      extraOptions.forEach(configuration::set);
    }
  }
}
