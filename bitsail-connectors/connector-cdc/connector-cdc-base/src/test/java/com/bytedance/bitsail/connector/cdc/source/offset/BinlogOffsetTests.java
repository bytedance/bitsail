/*
 * Copyright 2022-2023 Bytedance Ltd. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.bytedance.bitsail.connector.cdc.source.offset;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.connector.cdc.option.BinlogReaderOptions;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class BinlogOffsetTests {
  @Test
  public void testSpecifiedOffsetFromConfig() {
    BitSailConfiguration jobConf = BitSailConfiguration.newDefault();
    jobConf.set(BinlogReaderOptions.INITIAL_OFFSET_TYPE, "specified");
    Map<String, String> offsetProps = new HashMap<>();
    offsetProps.put("filename", "mysql.0001");
    offsetProps.put("offset", "1111");
    jobConf.set(BinlogReaderOptions.INITIAL_OFFSET_PROPS, offsetProps);
    BinlogOffset result = BinlogOffset.createFromJobConf(jobConf);
    Assert.assertEquals("mysql.0001", result.getProps().get("filename"));
    Assert.assertEquals("1111", result.getProps().get("offset"));
  }

  @Test
  public void testLatestOffsetFromConfig() {
    BitSailConfiguration jobConf = BitSailConfiguration.newDefault();
    jobConf.set(BinlogReaderOptions.INITIAL_OFFSET_TYPE, "latest");
    BinlogOffset result = BinlogOffset.createFromJobConf(jobConf);
    Assert.assertEquals(BinlogOffsetType.LATEST, result.getOffsetType());
  }
}
