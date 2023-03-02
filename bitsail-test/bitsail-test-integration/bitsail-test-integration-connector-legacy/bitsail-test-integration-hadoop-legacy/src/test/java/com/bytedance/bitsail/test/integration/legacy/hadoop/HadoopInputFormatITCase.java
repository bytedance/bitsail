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

package com.bytedance.bitsail.test.integration.legacy.hadoop;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.connector.hadoop.option.HadoopReaderOptions;
import com.bytedance.bitsail.test.integration.AbstractIntegrationTest;
import com.bytedance.bitsail.test.integration.utils.JobConfUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class HadoopInputFormatITCase extends AbstractIntegrationTest {
  private static final Logger LOG = LoggerFactory.getLogger(HadoopInputFormatITCase.class);

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder(new File("/tmp"));

  private static FileSystem FILESYSTEM;

  @Before
  public void setUp() throws IOException, InterruptedException {
    FILESYSTEM = LocalFileSystem.getLocal(new Configuration());
  }

  @After
  public void close() {
    FILESYSTEM = null;
  }

  @Test
  public void testJsonFormatHadoopInput() throws Exception {
    Path source = Paths.get(HadoopInputFormatITCase.class.getClassLoader()
        .getResource("source/test.json")
        .toURI()
        .getPath());

    File folder = TEMP_FOLDER.newFolder();
    Path target = Paths.get(folder.getAbsolutePath(), source.getFileName().toString());
    Files.copy(source, target);
    Configuration conf = FILESYSTEM.getConf();
    String defaultFS = conf.get("fs.defaultFS");
    LOG.info("fs.defaultFS: {}", defaultFS);
    BitSailConfiguration jobConf = JobConfUtils.fromClasspath("hadoop_to_print.json");
    jobConf.set(HadoopReaderOptions.PATH_LIST, defaultFS + target);
    submitJob(jobConf);
  }
}
