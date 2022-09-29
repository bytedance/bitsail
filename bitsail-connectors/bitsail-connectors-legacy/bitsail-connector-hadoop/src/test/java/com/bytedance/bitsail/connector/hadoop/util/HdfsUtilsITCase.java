/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.bytedance.bitsail.connector.hadoop.util;

import com.github.rholder.retry.RetryException;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.Path;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

@Ignore
public class HdfsUtilsITCase {
  private static final String DIR_PATH_STRING = "hdfs://westeros/user/bitsail_test/bitsail/hdfsutils_test";
  private static final Path DIR_PATH = new Path(DIR_PATH_STRING);

  @Before
  public void before() throws IOException, ExecutionException, RetryException {
    HdfsUtils.deleteIfExist(DIR_PATH);
    HdfsUtils.mkdir(DIR_PATH);
  }

  @After
  public void after() throws ExecutionException, RetryException, IOException {
    Path dirPath = new Path(DIR_PATH_STRING);
    HdfsUtils.deleteIfExist(dirPath);
  }

  @Test
  public void checkExistsTest() throws IOException {
    Path checkExistFile = new Path(DIR_PATH_STRING + "/checkExistFile");
    HdfsUtils.touchEmptyFile(checkExistFile);
    Assert.assertTrue(HdfsUtils.checkExists(checkExistFile));
    HdfsUtils.deletePath(checkExistFile);
    Assert.assertFalse(HdfsUtils.checkExists(checkExistFile));
  }

  @Test
  public void renameTest() throws IOException {
    Path renameFileFrom = new Path(DIR_PATH_STRING + "/renameFrom");
    Path renameFileTo = new Path(DIR_PATH_STRING + "/renameTo");

    HdfsUtils.touchEmptyFile(renameFileFrom);
    HdfsUtils.rename(renameFileFrom, renameFileTo, true);
    Assert.assertFalse(HdfsUtils.checkExists(renameFileFrom));
    Assert.assertTrue(HdfsUtils.checkExists(renameFileTo));
  }

  @Test
  public void listStatusTest() throws IOException {
    Path listStatus1 = new Path(DIR_PATH_STRING + "/listStatus1");
    Path listStatus2 = new Path(DIR_PATH_STRING + "/listStatus2");
    HdfsUtils.touchEmptyFile(listStatus1);
    HdfsUtils.touchEmptyFile(listStatus2);
    FileStatus[] fileStatuses = HdfsUtils.listStatus(DIR_PATH);
    Assert.assertEquals(2, fileStatuses.length);
  }

  @Test
  public void getFileSizeTest() throws IOException {
    //path does not exist
    Path getFileSizePath1 = new Path(DIR_PATH_STRING + "/getFileSize1");
    long getFileSize1 = HdfsUtils.getFileSize(getFileSizePath1);
    Assert.assertEquals(0, getFileSize1);

    //path exist
    Path getFileSizePath2 = new Path(DIR_PATH_STRING + "/getFileSize2");
    HdfsUtils.touchEmptyFile(getFileSizePath2);
    long getFileSize2 = HdfsUtils.getFileSize(getFileSizePath2);
    Assert.assertEquals(0, getFileSize2);
  }
}
