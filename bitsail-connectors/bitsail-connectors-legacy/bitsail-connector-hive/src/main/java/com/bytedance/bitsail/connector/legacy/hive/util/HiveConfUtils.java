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

package com.bytedance.bitsail.connector.legacy.hive.util;

import com.bytedance.bitsail.common.util.JsonSerializer;

import com.bytedance.bitsail.shaded.hive.client.HiveMetaClientUtil;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;

import java.util.Map;

public class HiveConfUtils {

  public static HiveConf fromJsonProperties(String jsonProperties) {
    Map<String, String> hiveProperties =
        JsonSerializer.parseToMap(jsonProperties);
    return HiveMetaClientUtil.getHiveConf(hiveProperties);
  }

  public HiveConf fromHiveConfPath(String location) {
    HiveConf hiveConf = new HiveConf();
    hiveConf.addResource(new Path(location));
    return hiveConf;
  }
}
