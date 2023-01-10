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

package com.bytedance.bitsail.connector.hadoop.source;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.connector.hadoop.option.HadoopReaderOptions;
import com.bytedance.bitsail.test.connector.test.EmbeddedFlinkCluster;
import com.bytedance.bitsail.test.connector.test.minicluster.MiniClusterUtil;
import com.bytedance.bitsail.test.connector.test.utils.JobConfUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class HadoopParquetInputFormatITCase {
  private static final Logger LOG = LoggerFactory.getLogger(HadoopParquetInputFormatITCase.class);

  private static FileSystem fs;

  @Before
  public void setUp() throws IOException, InterruptedException {
    MiniClusterUtil.setUp();
    fs = MiniClusterUtil.fileSystem;
  }

  @After
  public void close() {
    MiniClusterUtil.shutdown();
    fs = null;
  }

  @Test
  public void testHadoopToPrintParquet() throws Exception {
    String localJsonFile = "test_namespace/source/test_parquet";
    String remoteJsonFile = "/test_namespace/source/test_parquet";
    Configuration conf = fs.getConf();
    String defaultFS = conf.get("fs.defaultFS");
    LOG.info("fs.defaultFS: {}", defaultFS);
    ClassLoader classLoader = JobConfUtils.class.getClassLoader();
    fs.copyFromLocalFile(new Path(classLoader.getResource(localJsonFile).getPath()), new Path(remoteJsonFile));
    String inputFormat = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat";
    BitSailConfiguration jobConf = JobConfUtils.fromClasspath("hadoop_to_print_hive.json");
    jobConf.set(HadoopReaderOptions.HADOOP_INPUT_FORMAT_CLASS, inputFormat);
    jobConf.set(HadoopReaderOptions.PATH_LIST, defaultFS + remoteJsonFile);
    EmbeddedFlinkCluster.submitJob(jobConf);
  }
}
