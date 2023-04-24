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

package com.bytedance.bitsail.common.configuration.sys;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.configuration.ConfigParser;
import com.bytedance.bitsail.common.option.ReaderOptions;
import com.bytedance.bitsail.common.option.TransformOptions;
import com.bytedance.bitsail.common.option.WriterOptions;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.List;

public class ConfigParserTest {
  @Test
  public void testConfigList() {
    String classpath = ConfigParserTest.class.getClassLoader()
        .getResource("").getPath();
    BitSailConfiguration jobConf = BitSailConfiguration.from(new File(classpath, "multi_conf.json"));
    List<BitSailConfiguration> readerConfigs = ConfigParser.getInputConfList(jobConf);
    List<BitSailConfiguration> transformConfigs = ConfigParser.getTransformConfList(jobConf);
    List<BitSailConfiguration> writerConfigs = ConfigParser.getOutputConfList(jobConf);
    Assert.assertEquals(2, readerConfigs.size());
    Assert.assertEquals(2, transformConfigs.size());
    Assert.assertEquals(2, writerConfigs.size());
    Assert.assertEquals("com.bytedance.bitsail.connector.fake.source.FakeSource", readerConfigs.get(0).get(ReaderOptions.READER_CLASS));
    Assert.assertEquals("com.bytedance.bitsail.connector.ftp.source.FtpSource", readerConfigs.get(1).get(ReaderOptions.READER_CLASS));
    Assert.assertEquals("partition_by", transformConfigs.get(0).get(TransformOptions.BaseTransformOptions.TRANSFORM_TYPE));
    Assert.assertEquals("map", transformConfigs.get(1).get(TransformOptions.BaseTransformOptions.TRANSFORM_TYPE));
    Assert.assertEquals("com.bytedance.bitsail.connector.print.sink.PrintSink", writerConfigs.get(0).get(WriterOptions.WRITER_CLASS));
    Assert.assertEquals("com.bytedance.bitsail.connector.redis.sink.RedisSink", writerConfigs.get(1).get(WriterOptions.WRITER_CLASS));
  }
}
