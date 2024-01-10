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

package com.bytedance.bitsail.connector.oss.config;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.connector.oss.option.OssReaderOptions;

import lombok.Data;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.aliyun.oss.Constants;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

@Data
public class OssConf implements Serializable {
  private static final String HDFS_IMPL = "org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem";
  private static final String SCHEMA = "oss";
  protected Map<String, String> extraOptions = new HashMap<>();
  protected String hdfsNameKey;
  protected String hdfsSitePath;
  protected String kerberosPrincipal;
  protected String kerberosKeytabPath;

  public String getHdfsImpl() {
    return HDFS_IMPL;
  }

  public String getSchema() {
    return SCHEMA;
  }

  public OssConf(String hdfsNameKey) {
    this.hdfsNameKey = hdfsNameKey;
  }

  public static OssConf buildWithConfig(BitSailConfiguration config) {
    OssConf hadoopConf = new OssConf(config.get(OssReaderOptions.BUCKET));
    HashMap<String, String> ossOptions = new HashMap<>();
    ossOptions.put(Constants.ACCESS_KEY_ID, config.get(OssReaderOptions.ACCESS_KEY));
    ossOptions.put(
        Constants.ACCESS_KEY_SECRET, config.get(OssReaderOptions.ACCESS_SECRET));
    ossOptions.put(Constants.ENDPOINT_KEY, config.get(OssReaderOptions.ENDPOINT));
    hadoopConf.setExtraOptions(ossOptions);
    return hadoopConf;
  }

  public void setExtraOptionsForConfiguration(Configuration configuration) {
    if (!extraOptions.isEmpty()) {
      extraOptions.forEach(configuration::set);
    }
    if (hdfsSitePath != null) {
      configuration.addResource(new Path(hdfsSitePath));
    }
  }
}